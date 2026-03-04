#!/usr/bin/env python3
import os
import sys
import csv
import json
import time
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

API_BASE = "https://www.ebi.ac.uk/biostudies/api/v1/studies"

# ---- tuning ----
WORKERS = 10          # adjust if you want (e.g., 8~16)
SLEEP_BETWEEN = 0.02  # small politeness delay per request
TIMEOUT = 45
RETRIES = 8

# ---- heuristics ----
ABSTRACT_KEYS = {"abstract", "description", "summary", "study description", "study_summary"}
# Some studies store free text under attributes list items with name/value
ATTR_NAME_KEYS = {"abstract", "description", "summary"}

COMPRESSED_EXTS = (".gz", ".bgz", ".bz2", ".zip", ".xz", ".zst")


def norm_one_line(s: str) -> str:
    s = s.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def ext_smart(filename: str) -> Optional[str]:
    fn = filename.lower().strip()
    if not fn or fn.endswith("/"):
        return None
    base = os.path.basename(fn)

    # handle .fastq.gz, .vcf.bgz, etc.
    for cext in COMPRESSED_EXTS:
        if base.endswith(cext):
            base2 = base[: -len(cext)]
            _, e1 = os.path.splitext(base2)
            e1 = e1.lstrip(".")
            if e1:
                return f"{e1}{cext.lstrip('.') if cext != '.gz' else '.gz'}" if cext != ".gz" else f"{e1}.gz"
            # if no inner ext, return compression ext only
            return cext.lstrip(".")
    _, e = os.path.splitext(base)
    e = e.lstrip(".")
    return e or None


def find_candidate_texts(obj: Any) -> List[str]:
    """Traverse JSON and collect candidate abstract/description strings."""
    texts: List[str] = []

    def walk(x: Any):
        if isinstance(x, dict):
            # direct keys
            for k, v in x.items():
                lk = str(k).lower()
                if lk in ABSTRACT_KEYS and isinstance(v, str) and v.strip():
                    texts.append(v)
            # attribute-like objects: {"name": "...", "value": "..."}
            name = x.get("name")
            value = x.get("value")
            if isinstance(name, str) and isinstance(value, str):
                if name.strip().lower() in ATTR_NAME_KEYS and value.strip():
                    texts.append(value)

            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for it in x:
                walk(it)

    walk(obj)
    # prefer longer / more informative
    texts = [norm_one_line(t) for t in texts if norm_one_line(t)]
    texts.sort(key=len, reverse=True)
    return texts


def find_filenames(obj: Any) -> List[str]:
    """Traverse JSON and collect filename/path-like strings."""
    out: List[str] = []

    FILE_KEYS = {"filename", "file", "filepath", "path", "uri", "url", "href", "name"}

    def walk(x: Any):
        if isinstance(x, dict):
            for k, v in x.items():
                lk = str(k).lower()
                if lk in FILE_KEYS and isinstance(v, str) and v.strip():
                    # keep only things that look like filenames/paths
                    out.append(v)
                walk(v)
        elif isinstance(x, list):
            for it in x:
                walk(it)

    walk(obj)
    return out


def request_json(session: requests.Session, url: str) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = session.get(url, headers={"Accept": "application/json"}, timeout=TIMEOUT)
            r.raise_for_status()
            # If HTML returned, fail clearly
            ctype = r.headers.get("Content-Type", "")
            if "application/json" not in ctype:
                raise RuntimeError(f"Not JSON (Content-Type={ctype})")
            return r.json()
        except Exception as e:
            last_err = e
            # exponential-ish backoff
            time.sleep(min(2.0, 0.2 * attempt))
    raise RuntimeError(f"Failed after retries: {url} err={last_err}")


def enrich_one(accession: str, session: requests.Session) -> Tuple[str, str, str]:
    url = f"{API_BASE}/{accession}"
    js = request_json(session, url)

    # abstract
    cand = find_candidate_texts(js)
    abstract = cand[0] if cand else ""

    # file types
    fns = find_filenames(js)
    exts: Set[str] = set()
    for fn in fns:
        e = ext_smart(fn)
        if e:
            exts.add(e)
    file_types = ",".join(sorted(exts))

    # polite delay
    time.sleep(SLEEP_BETWEEN)
    return accession, abstract, file_types


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <input_hits_tsv> <output_tsv>", file=sys.stderr)
        sys.exit(1)

    in_path = sys.argv[1]
    out_path = sys.argv[2]
    cache_path = out_path + ".cache.jsonl"

    # Load already processed accessions (resume)
    done: Dict[str, Dict[str, str]] = {}
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    done[rec["accession"]] = rec
                except Exception:
                    continue
        print(f"[INFO] resume: loaded {len(done)} cached records", file=sys.stderr)

    # Read input hits
    rows: List[Dict[str, str]] = []
    with open(in_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = reader.fieldnames or []
        if "accession" not in fieldnames:
            raise RuntimeError("Input TSV must have 'accession' column")
        for r in reader:
            rows.append(r)

    # Prepare output headers: keep existing + add abstract + file_types
    out_fields = list(rows[0].keys()) if rows else []
    if "abstract" not in out_fields:
        out_fields.append("abstract")
    if "file_types" not in out_fields:
        out_fields.append("file_types")

    # Work list
    todo = [r["accession"] for r in rows if r.get("accession") and r["accession"] not in done]
    print(f"[INFO] total_rows={len(rows)} todo={len(todo)} workers={WORKERS}", file=sys.stderr)

    sess = requests.Session()

    # Fetch in parallel, append to cache as we go (safe resume)
    with ThreadPoolExecutor(max_workers=WORKERS) as ex, open(cache_path, "a", encoding="utf-8") as cachef:
        futs = {ex.submit(enrich_one, acc, sess): acc for acc in todo}
        for i, fut in enumerate(as_completed(futs), start=1):
            acc = futs[fut]
            try:
                a, abstract, ftypes = fut.result()
                rec = {"accession": a, "abstract": abstract, "file_types": ftypes}
                cachef.write(json.dumps(rec, ensure_ascii=False) + "\n")
                cachef.flush()
                done[a] = rec
            except Exception as e:
                # keep going; write a blank record so we don't retry forever unless you want to
                rec = {"accession": acc, "abstract": "", "file_types": ""}
                cachef.write(json.dumps(rec, ensure_ascii=False) + "\n")
                cachef.flush()
                done[acc] = rec
            if i % 200 == 0:
                print(f"[INFO] processed {i}/{len(todo)}", file=sys.stderr)

    # Write final augmented TSV
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            acc = r.get("accession", "")
            rec = done.get(acc, {})
            r2 = dict(r)
            r2["abstract"] = rec.get("abstract", "")
            r2["file_types"] = rec.get("file_types", "")
            writer.writerow(r2)

    print(f"[DONE] wrote {out_path}", file=sys.stderr)
    print(f"[CACHE] {cache_path}", file=sys.stderr)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
BioStudies harvester:
- Search BioStudies by query words/pattern
- Iterate through all pages
- Fetch full study JSON for each accession (recommended by BioStudies/ArrayExpress migration notes)
- Extract title/abstract/publication/files/links
- (Optional) infer download root from /info ftpLink
- Output TSV + JSONL + per-study JSON files

References:
- BioStudies search is paged; complete metadata requires per-study fetch. (NAR 2021 migration note) https://academic.oup.com/nar/article/49/D1/D1502/5992288
- /studies/<acc>/info provides ftpLink in some collections (example in EBI download guides). https://www.ebi.ac.uk/bioimage-archive/help-download/

USAGE examples:
  python3 biostudies_harvest.py --query 'radiation' --max-hits 200 --outdir out_radiation
  python3 biostudies_harvest.py --query '"solar radiation" aerosols China' --outdir out_solar
  python3 biostudies_harvest.py --query 'radiation AND microarray' --max-hits 500 --sleep 0.2
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import requests


BASE = "https://www.ebi.ac.uk/biostudies/api/v1"


@dataclass
class StudyRow:
    accession: str
    title: str
    collection: str
    released: str
    publication_doi: str
    publication_pmid: str
    publication_pmcid: str
    abstract: str
    ftp_link: str
    num_files: int
    file_names: str
    link_types: str
    link_urls: str


def http_get_json(url: str, timeout: int = 60) -> Dict[str, Any]:
    r = requests.get(url, timeout=timeout, headers={"Accept": "application/json"})
    r.raise_for_status()
    return r.json()


def safe_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    return str(x)


def detect_accessions_from_search_payload(payload: Dict[str, Any]) -> List[str]:
    """
    BioStudies /api/v1/search response schema can vary by deployment/version/collection.
    We attempt multiple common patterns:
      - payload["hits"] as list of objects with "accession"
      - payload["studies"] as list ...
      - payload["content"] ...
    """
    candidates: List[Any] = []
    for key in ("hits", "studies", "content", "results", "data"):
        if key in payload and isinstance(payload[key], list):
            candidates = payload[key]
            break

    accessions: List[str] = []
    for item in candidates:
        if isinstance(item, dict):
            for k in ("accession", "accno", "accNum", "id"):
                if k in item and isinstance(item[k], str) and item[k].startswith("S-"):
                    accessions.append(item[k])
                    break
        elif isinstance(item, str) and item.startswith("S-"):
            accessions.append(item)

    # De-dup while preserving order
    seen = set()
    out = []
    for a in accessions:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out


def get_publication_ids(study_json: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    Try to extract DOI/PMID/PMCID from common places.
    BioStudies records differ by collection (S-EPMC, S-ENAD, S-BIAD, etc).
    """
    doi = pmid = pmcid = ""

    # Common: section -> attributes, or links
    # We'll scan JSON string-ish for well-known patterns as fallback.
    s = json.dumps(study_json, ensure_ascii=False)

    m = re.search(r'\b10\.\d{4,9}/[^\s"<>]+', s)
    if m:
        doi = m.group(0).rstrip(".,;)")

    m = re.search(r'\bPMID[:\s"]+(\d{5,10})\b', s, flags=re.IGNORECASE)
    if m:
        pmid = m.group(1)
    else:
        # sometimes pmid appears as just a number with "pmid"
        m = re.search(r'"pmid"\s*:\s*"(\d{5,10})"', s, flags=re.IGNORECASE)
        if m:
            pmid = m.group(1)

    m = re.search(r'\bPMC\d+\b', s)
    if m:
        pmcid = m.group(0)

    return doi, pmid, pmcid


def extract_title_abstract_collection(study_json: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """
    Extract title, abstract, collection, released date.
    """
    title = abstract = collection = released = ""

    # Easy wins (some schemas)
    for k in ("title", "name"):
        if k in study_json and isinstance(study_json[k], str):
            title = study_json[k]
            break

    for k in ("collection", "domain"):
        if k in study_json and isinstance(study_json[k], str):
            collection = study_json[k]
            break

    for k in ("released", "releaseDate", "submitted", "submissionDate"):
        if k in study_json and isinstance(study_json[k], str):
            released = study_json[k]
            break

    # Common BioStudies schema: {"section": {...}}
    # Abstract often stored as a section attribute or a subsection.
    # We'll do a recursive scan for the largest "Abstract" / "abstract" field.
    def walk(obj: Any) -> List[Tuple[str, str]]:
        out: List[Tuple[str, str]] = []
        if isinstance(obj, dict):
            for kk, vv in obj.items():
                if isinstance(vv, (dict, list)):
                    out.extend(walk(vv))
                else:
                    if isinstance(vv, str) and kk.lower() in ("abstract", "description", "text"):
                        out.append((kk, vv))
        elif isinstance(obj, list):
            for it in obj:
                out.extend(walk(it))
        return out

    texts = walk(study_json)
    # Prefer something explicitly called abstract
    abs_candidates = [v for k, v in texts if k.lower() == "abstract" and len(v) > 80]
    if abs_candidates:
        abstract = max(abs_candidates, key=len)
    else:
        # fallback: long description/text
        long_candidates = [v for k, v in texts if len(v) > 200]
        if long_candidates:
            abstract = max(long_candidates, key=len)

    # If title still empty, scan for a "Study" section title-like field
    if not title:
        s = json.dumps(study_json, ensure_ascii=False)
        m = re.search(r'"title"\s*:\s*"([^"]{10,300})"', s)
        if m:
            title = m.group(1)

    return title.strip(), abstract.strip(), collection.strip(), released.strip()


def extract_files_and_links(study_json: Dict[str, Any]) -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    Files: try to find attachments (name/path)
    Links: external links like DOI/ENA/GEO/PMC
    """
    files: List[str] = []
    links: List[Tuple[str, str]] = []

    def walk(obj: Any):
        nonlocal files, links
        if isinstance(obj, dict):
            # files
            if "fileName" in obj and isinstance(obj["fileName"], str):
                files.append(obj["fileName"])
            if "name" in obj and isinstance(obj["name"], str) and ("." in obj["name"]):
                # cautious: sometimes name is a filename
                if any(obj["name"].lower().endswith(ext) for ext in (".txt", ".tsv", ".csv", ".xlsx", ".zip", ".gz", ".pdf", ".doc", ".docx")):
                    files.append(obj["name"])
            # links
            if "url" in obj and isinstance(obj["url"], str):
                t = safe_str(obj.get("type") or obj.get("label") or obj.get("name"))
                links.append((t, obj["url"]))
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for it in obj:
                walk(it)

    walk(study_json)

    # de-dup
    files = sorted(set(files))
    # keep order-ish but de-dup
    seen = set()
    uniq_links = []
    for t, u in links:
        key = (t, u)
        if key not in seen:
            seen.add(key)
            uniq_links.append((t, u))
    return files, uniq_links


def get_ftp_link(accession: str, timeout: int = 60) -> str:
    """
    /studies/{acc}/info often returns ftpLink. (varies by collection)
    """
    try:
        info = http_get_json(f"{BASE}/studies/{accession}/info", timeout=timeout)
        ftp = info.get("ftpLink") or info.get("ftp") or ""
        return safe_str(ftp)
    except Exception:
        return ""


def search_accessions(query: str, page: int, page_size: int, timeout: int = 60) -> Dict[str, Any]:
    """
    BioStudies search endpoint.
    Many deployments accept: /search?query=...&page=...&pageSize=...
    We'll try common parameter names.
    """
    # Try variants to maximize compatibility.
    variants = [
        f"{BASE}/search?query={requests.utils.quote(query)}&page={page}&pageSize={page_size}",
        f"{BASE}/search?query={requests.utils.quote(query)}&page={page}&size={page_size}",
        f"{BASE}/search?query={requests.utils.quote(query)}&from={(page-1)*page_size}&size={page_size}",
    ]
    last_err = None
    for url in variants:
        try:
            return http_get_json(url, timeout=timeout)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Search failed for all parameter variants. Last error: {last_err}")


def ensure_outdir(path: str):
    os.makedirs(path, exist_ok=True)
    os.makedirs(os.path.join(path, "studies"), exist_ok=True)


def main():
    ap = argparse.ArgumentParser(description="Harvest BioStudies by keyword/pattern and extract abstract+metadata.")
    ap.add_argument("--query", required=True, help='Search query (e.g., radiation OR "solar radiation").')
    ap.add_argument("--outdir", default="biostudies_out", help="Output directory.")
    ap.add_argument("--page-size", type=int, default=20, help="Search page size.")
    ap.add_argument("--max-hits", type=int, default=200, help="Stop after N studies (safety). Use 0 for no limit.")
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between requests (politeness).")
    ap.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds.")
    args = ap.parse_args()

    ensure_outdir(args.outdir)
    tsv_path = os.path.join(args.outdir, "results.tsv")
    jsonl_path = os.path.join(args.outdir, "results.jsonl")

    rows: List[StudyRow] = []
    seen_acc = set()

    page = 1
    total_fetched = 0

    while True:
        payload = search_accessions(args.query, page=page, page_size=args.page_size, timeout=args.timeout)
        accessions = detect_accessions_from_search_payload(payload)

        if not accessions:
            break

        for acc in accessions:
            if acc in seen_acc:
                continue
            seen_acc.add(acc)

            # Fetch full study JSON
            try:
                study = http_get_json(f"{BASE}/studies/{acc}", timeout=args.timeout)
            except Exception as e:
                print(f"[WARN] failed to fetch study {acc}: {e}", file=sys.stderr)
                continue

            # Save raw study JSON
            raw_path = os.path.join(args.outdir, "studies", f"{acc}.json")
            with open(raw_path, "w", encoding="utf-8") as f:
                json.dump(study, f, ensure_ascii=False, indent=2)

            title, abstract, collection, released = extract_title_abstract_collection(study)
            doi, pmid, pmcid = get_publication_ids(study)
            files, links = extract_files_and_links(study)
            ftp_link = get_ftp_link(acc, timeout=args.timeout)

            row = StudyRow(
                accession=acc,
                title=title,
                collection=collection,
                released=released,
                publication_doi=doi,
                publication_pmid=pmid,
                publication_pmcid=pmcid,
                abstract=abstract,
                ftp_link=ftp_link,
                num_files=len(files),
                file_names=";".join(files),
                link_types=";".join([t for t, _ in links if t]),
                link_urls=";".join([u for _, u in links if u]),
            )
            rows.append(row)

            # Write JSONL streaming
            with open(jsonl_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(asdict(row), ensure_ascii=False) + "\n")

            total_fetched += 1
            if args.sleep > 0:
                time.sleep(args.sleep)

            if args.max_hits and args.max_hits > 0 and total_fetched >= args.max_hits:
                break

        if args.max_hits and args.max_hits > 0 and total_fetched >= args.max_hits:
            break

        page += 1
        if args.sleep > 0:
            time.sleep(args.sleep)

    # Write TSV (sorted by accession by default)
    rows_sorted = sorted(rows, key=lambda r: r.accession)

    with open(tsv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(list(asdict(StudyRow(**{k: "" for k in asdict(rows_sorted[0]).keys()})).keys()) if rows_sorted else [
            "accession","title","collection","released","publication_doi","publication_pmid","publication_pmcid",
            "abstract","ftp_link","num_files","file_names","link_types","link_urls"
        ])
        for r in rows_sorted:
            w.writerow([
                r.accession, r.title, r.collection, r.released, r.publication_doi, r.publication_pmid, r.publication_pmcid,
                r.abstract, r.ftp_link, r.num_files, r.file_names, r.link_types, r.link_urls
            ])

    print(f"[OK] query='{args.query}'  studies={len(rows_sorted)}")
    print(f"[OK] TSV   : {tsv_path}")
    print(f"[OK] JSONL : {jsonl_path}")
    print(f"[OK] Raw JSON per study in: {os.path.join(args.outdir, 'studies')}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import argparse
import csv
import gzip
import json
import os
import re
import sys
import time
from typing import Dict, Optional, Tuple, List, Any

try:
    import requests
except ImportError:
    requests = None  # only needed for --llm mode


###############################################################################
# Utilities
###############################################################################

def open_text_auto(path: str, mode: str = "rt", encoding: str = "utf-8"):
    """Open plain or .gz text file."""
    if path.endswith(".gz"):
        return gzip.open(path, mode, encoding=encoding, errors="replace")
    return open(path, mode, encoding=encoding, errors="replace")


def norm_one_line(s: str) -> str:
    s = (s or "").replace("\t", " ").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def clip(s: str, n: int) -> str:
    s = norm_one_line(s)
    return s if len(s) <= n else s[: max(0, n - 1)].rstrip() + "…"


###############################################################################
# Rule-based extraction (fast baseline)
###############################################################################

RE_DOSE = re.compile(
    r"(?P<val>\b\d+(?:\.\d+)?\b)\s*(?P<unit>mgy|gy|sv|msv|gray|grays)\b",
    flags=re.IGNORECASE,
)
RE_X_BY = re.compile(
    r"\b(\d+(?:\.\d+)?)\s*[x×]\s*(\d+(?:\.\d+)?)\s*(gy|mgy|sv|msv)\b",
    flags=re.IGNORECASE,
)

MODALITY_KEYWORDS = [
    ("proton", ["proton", "pencil beam"]),
    ("carbon ion", ["carbon ion", "c-ion", "heavy ion", "heavy-ion"]),
    ("ion beam", ["ion beam", "ion-beam", "particle beam", "particle-beam"]),
    ("gamma", ["gamma", "cobalt-60", "co-60", "cs-137", "cesium-137"]),
    ("x-ray", ["x-ray", "xray", "x ray"]),
    ("synchrotron x-ray", ["synchrotron", "synchrotron radiation"]),
    ("uv", ["uv", "ultraviolet"]),
    ("neutron", ["neutron"]),
]

SPECIES_KEYWORDS = [
    ("Homo sapiens", ["human", "homo sapiens", "patient", "patients", "donor", "donors"]),
    ("Mus musculus", ["mouse", "mice", "mus musculus"]),
    ("Rattus norvegicus", ["rat", "rats", "rattus norvegicus"]),
    ("Danio rerio", ["zebrafish", "danio rerio"]),
    ("Drosophila melanogaster", ["drosophila", "fruit fly", "melanogaster"]),
    ("Arabidopsis thaliana", ["arabidopsis thaliana", "arabidopsis"]),
    ("Brassica rapa", ["brassica rapa"]),
    ("Caenorhabditis elegans", ["c. elegans", "caenorhabditis elegans"]),
    ("Saccharomyces cerevisiae", ["yeast", "saccharomyces cerevisiae"]),
    ("Escherichia coli", ["e. coli", "escherichia coli"]),
]

ASSAY_KEYWORDS = [
    ("scRNA-seq", ["single-cell", "single cell", "scrna", "10x genomics"]),
    ("RNA-seq", ["rna-seq", "rna seq", "rnaseq", "fastq", "transcriptome"]),
    ("Microarray", ["microarray", "arrayexpress", "affymetrix", "illumina beadchip"]),
    ("WGS/WES", ["wgs", "wes", "whole genome", "whole exome", "vcf"]),
    ("Proteomics", ["proteomics", "mass spec", "ms/ms", "tmt", "pride"]),
    ("Imaging", ["microscopy", "ct", "micro-ct", "micro ct", "tomography", "histology", "imaging"]),
]

SAMPLE_HINTS = [
    ("cell line", ["cell line", "hela", "a549", "hek293", "u87", "mcf7"]),
    ("tissue", ["tissue", "biopsy", "ffpe", "formalin-fixed", "paraffin"]),
    ("blood", ["pbmc", "blood", "serum", "plasma"]),
    ("organoid", ["organoid", "spheroid"]),
    ("tumor", ["tumor", "tumour", "glioblastoma", "gbm", "cancer"]),
]

TIMEPOINT_RE = re.compile(
    r"\b(\d+(?:\.\d+)?)\s*(h|hr|hrs|hour|hours|d|day|days|wk|week|weeks)\b",
    flags=re.IGNORECASE,
)


def detect_species(text: str) -> str:
    t = (text or "").lower()
    for sp, kws in SPECIES_KEYWORDS:
        for k in kws:
            if k in t:
                return sp
    return ""


def detect_modality(text: str) -> str:
    t = (text or "").lower()
    for name, kws in MODALITY_KEYWORDS:
        for k in kws:
            if k in t:
                return name
    return ""


def detect_assay(text: str, file_types: str) -> str:
    t = (text or "").lower()
    ft = (file_types or "").lower()
    combo = t + " " + ft
    for assay, kws in ASSAY_KEYWORDS:
        for k in kws:
            if k in combo:
                return assay
    if "fastq" in ft:
        return "RNA-seq"
    if "bam" in ft:
        return "Sequencing"
    if "vcf" in ft:
        return "Genomics"
    return ""


def detect_samples(text: str, title: str) -> str:
    t = (text or "").lower()
    tt = (title or "").lower()
    combo = tt + " " + t
    hits = []
    for label, kws in SAMPLE_HINTS:
        for k in kws:
            if k in combo:
                hits.append(label)
                break
    seen = set()
    uniq = []
    for h in hits:
        if h not in seen:
            seen.add(h)
            uniq.append(h)
    return ",".join(uniq[:3])


def detect_dose(text: str) -> str:
    t = text or ""
    m = RE_X_BY.search(t)
    if m:
        a, b, unit = m.group(1), m.group(2), m.group(3)
        return f"{a}x{b} {unit.upper()}"
    doses = []
    for m in RE_DOSE.finditer(t):
        val = m.group("val")
        unit = m.group("unit").lower()
        unit = "Gy" if unit in ("gy", "gray", "grays") else unit.upper()
        doses.append(f"{val} {unit}")
    doses = list(dict.fromkeys(doses))
    return ", ".join(doses[:2])


def detect_timepoints(text: str) -> str:
    t = text or ""
    found = []
    for m in TIMEPOINT_RE.finditer(t):
        found.append(m.group(0))
    out = []
    seen = set()
    for x in found:
        k = x.lower()
        if k not in seen:
            seen.add(k)
            out.append(x)
    return ", ".join(out[:3])


def rule_summary(title: str, abstract: str, file_types: str) -> Dict[str, str]:
    base = f"{title}. {abstract}"
    sp = detect_species(base)
    mod = detect_modality(base)
    dose = detect_dose(base)
    tp = detect_timepoints(base)
    assay = detect_assay(base, file_types)
    samp = detect_samples(base, title)

    parts = []
    if sp: parts.append(sp)
    if samp: parts.append(f"samples:{samp}")
    if mod: parts.append(f"rad:{mod}")
    if dose: parts.append(f"dose:{dose}")
    if tp: parts.append(f"time:{tp}")
    if assay: parts.append(f"assay:{assay}")

    summary = " | ".join(parts) if parts else ""
    snippet = clip(abstract or "", 220)
    if snippet:
        summary = (summary + " | " if summary else "") + snippet

    return {
        "species_guess": sp,
        "samples_guess": samp,
        "radiation_modality": mod,
        "dose_guess": dose,
        "timepoints_guess": tp,
        "assay_guess": assay,
        "analysis_summary": summary,
    }


###############################################################################
# LLM mode (OpenAI-compatible endpoint)
###############################################################################

def build_llm_prompt(accession: str, title: str, abstract: str, content_snippet: str, file_types: str) -> str:
    return f"""Accession: {accession}
Title: {title}
File types: {file_types}
Content snippet: {clip(content_snippet or "", 400)}
Abstract/description: {clip(abstract or "", 1400)}

Task:
Extract radiation-study key facts. Return STRICT JSON with keys:
species (string or null),
samples (string or null),
radiation_modality (string or null),
dose (string or null),
dose_unit (string or null),
fractionation (string or null),
dose_rate (string or null),
timepoints (string or null),
assay (string or null),
important_notes (string or null),
one_line_summary (string; <= 280 chars).

Rules:
- If unknown, use null (except one_line_summary must exist).
- If this is about "solar radiation / photosynthetically active radiation" and not an experimental irradiation exposure, say so in important_notes and keep dose fields null.
Return JSON only. No extra text.
"""


def parse_llm_json(text: str) -> Dict[str, str]:
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    obj = json.loads(text)
    out: Dict[str, str] = {}
    for k, v in obj.items():
        out[k] = "" if v is None else norm_one_line(str(v))
    return out


def call_openai_compatible_chat(
    session,
    base_url: str,
    api_key: str,
    model: str,
    prompt: str,
    timeout: int,
    retries: int,
    temperature: float = 0.2,
    max_tokens: int = 320,
) -> str:
    """
    Calls OpenAI-compatible Chat Completions endpoint:
    POST {base_url}/v1/chat/completions
    """
    if requests is None:
        raise RuntimeError("requests not installed. pip install requests")

    url = base_url.rstrip("/") + "/v1/chat/completions"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": "You are an expert curator. Return STRICT JSON only."},
            {"role": "user", "content": prompt},
        ],
    }

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = session.post(url, headers=headers, json=payload, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            return data["choices"][0]["message"]["content"]
        except Exception as e:
            last_err = e
            time.sleep(min(2.0, 0.3 * attempt))
    raise RuntimeError(f"LLM call failed after retries: {last_err}")


###############################################################################
# Main
###############################################################################

def main():
    p = argparse.ArgumentParser(
        description="Add radiation extraction summary columns using rule-based or LLM mode; resume with cache."
    )
    p.add_argument("--input", required=True, help="Input hits_augmented.tsv(.gz)")
    p.add_argument("--output", required=True, help="Output TSV(.gz)")
    p.add_argument("--llm", action="store_true", help="Enable LLM mode (OpenAI-compatible endpoint).")
    p.add_argument("--base-url", default=os.environ.get("OPENAI_BASE_URL", ""),
                   help="OpenAI-compatible base URL (e.g., http://127.0.0.1:8000). Env OPENAI_BASE_URL supported.")
    p.add_argument("--api-key", default=os.environ.get("OPENAI_API_KEY", ""),
                   help="API key (env OPENAI_API_KEY). Local vLLM usually blank.")
    p.add_argument("--model", default=os.environ.get("OPENAI_MODEL", ""),
                   help="Model name (env OPENAI_MODEL).")
    p.add_argument("--cache", default="", help="Cache JSONL file for resume. Default: <output>.cache.jsonl")
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--retries", type=int, default=6)
    p.add_argument("--sleep", type=float, default=0.02, help="Sleep between LLM calls (seconds).")
    p.add_argument("--log-every", type=int, default=500)

    args = p.parse_args()

    cache_path = args.cache or (args.output + ".cache.jsonl")

    # Load cache
    cache: Dict[str, Dict[str, str]] = {}
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as cf:
            for line in cf:
                try:
                    rec = json.loads(line)
                    acc = rec.get("accession")
                    if acc:
                        cache[acc] = rec
                except Exception:
                    continue
        print(f"[INFO] loaded cache: {len(cache)} records from {cache_path}", file=sys.stderr)

    fin = open_text_auto(args.input, "rt")
    fout = open_text_auto(args.output, "wt")
    reader = csv.DictReader(fin, delimiter="\t")
    in_fields = reader.fieldnames or []
    if "accession" not in in_fields:
        raise SystemExit("ERROR: input must contain 'accession' column.")

    out_fields = list(in_fields)
    for c in ["species_guess", "samples_guess", "radiation_modality", "dose_guess", "timepoints_guess", "assay_guess", "analysis_summary"]:
        if c not in out_fields:
            out_fields.append(c)

    writer = csv.DictWriter(fout, fieldnames=out_fields, delimiter="\t", extrasaction="ignore")
    writer.writeheader()

    session = None
    llm_enabled = bool(args.llm and args.base_url and args.model)
    if args.llm and not llm_enabled:
        print("[WARN] --llm specified but missing --base-url or --model; running rule-based only.", file=sys.stderr)

    if llm_enabled:
        if requests is None:
            raise SystemExit("ERROR: requests not available. pip install requests")
        session = requests.Session()
        print(f"[INFO] LLM enabled: base_url={args.base_url} model={args.model}", file=sys.stderr)

    processed = 0
    wrote_cache = 0

    cachef = open(cache_path, "a", encoding="utf-8")

    try:
        for row in reader:
            processed += 1
            acc = (row.get("accession") or "").strip()
            title = row.get("title", "")
            abstract = row.get("abstract", "")
            content_snippet = row.get("content_snippet", "")
            file_types = row.get("file_types", "")

            if acc and acc in cache:
                rec = cache[acc]
            else:
                # rule-based always
                r = rule_summary(title, abstract, file_types)
                rec = {"accession": acc, **r}

                # optional LLM refine
                if llm_enabled and acc:
                    prompt = build_llm_prompt(acc, title, abstract, content_snippet, file_types)
                    try:
                        out = call_openai_compatible_chat(
                            session=session,
                            base_url=args.base_url,
                            api_key=args.api_key,
                            model=args.model,
                            prompt=prompt,
                            timeout=args.timeout,
                            retries=args.retries,
                        )
                        j = parse_llm_json(out)

                        if j.get("one_line_summary"):
                            rec["analysis_summary"] = clip(j["one_line_summary"], 320)
                        if j.get("species"): rec["species_guess"] = j["species"]
                        if j.get("samples"): rec["samples_guess"] = j["samples"]
                        if j.get("radiation_modality"): rec["radiation_modality"] = j["radiation_modality"]
                        if j.get("dose"): rec["dose_guess"] = j["dose"]
                        if j.get("timepoints"): rec["timepoints_guess"] = j["timepoints"]
                        if j.get("assay"): rec["assay_guess"] = j["assay"]
                    except Exception as e:
                        # keep rule-based; log and continue
                        print(f"[WARN] LLM failed for {acc}: {e}", file=sys.stderr)

                    time.sleep(args.sleep)

                cachef.write(json.dumps(rec, ensure_ascii=False) + "\n")
                cachef.flush()
                cache[acc] = rec
                wrote_cache += 1

            # write row with columns
            row["species_guess"] = rec.get("species_guess", "")
            row["samples_guess"] = rec.get("samples_guess", "")
            row["radiation_modality"] = rec.get("radiation_modality", "")
            row["dose_guess"] = rec.get("dose_guess", "")
            row["timepoints_guess"] = rec.get("timepoints_guess", "")
            row["assay_guess"] = rec.get("assay_guess", "")
            row["analysis_summary"] = rec.get("analysis_summary", "")

            writer.writerow(row)

            if processed % args.log_every == 0:
                print(f"[INFO] processed={processed} cache_written={wrote_cache}", file=sys.stderr)

    except KeyboardInterrupt:
        print("\n[INTERRUPT] Ctrl+C received. Output so far is saved; rerun to resume.", file=sys.stderr)
    finally:
        cachef.close()
        fin.close()
        fout.close()

    print(f"[DONE] processed={processed} output={args.output}", file=sys.stderr)
    print(f"[CACHE] {cache_path}", file=sys.stderr)


if __name__ == "__main__":
    main()

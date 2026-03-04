#!/usr/bin/env bash
set -euo pipefail

BASE="https://www.ebi.ac.uk/biostudies/api/v1/search"
MODE="content"
TERM="radiation"
PAGESIZE=100
OUTDIR="biostudies_content_radiation"
mkdir -p "$OUTDIR"

HITS_TSV="${OUTDIR}/hits.tsv"

enc() { printf '%s' "$1" | jq -sRr @uri; }
ENC_TERM="$(enc "$TERM")"

# ---- total / maxpage (exact) ----
META="$(curl -sS -H "Accept: application/json" \
  "${BASE}?${MODE}=${ENC_TERM}&pageSize=1&page=1")"
TOTAL="$(echo "$META" | jq -r '.totalHits')"
EXACT="$(echo "$META" | jq -r '.isTotalHitsExact')"
MAXPAGE=$(( (TOTAL + PAGESIZE - 1) / PAGESIZE ))

echo "[INFO] totalHits=${TOTAL} exact=${EXACT} pageSize=${PAGESIZE} maxPage=${MAXPAGE}"

# ---- detect start page from existing hits.tsv ----
if [ -f "$HITS_TSV" ]; then
  # lines excluding header
  have=$(( $(wc -l < "$HITS_TSV") - 1 ))
  if [ "$have" -lt 0 ]; then have=0; fi
  # pages already completed
  done_pages=$(( have / PAGESIZE ))
  # if partially written page exists, rewind one page to be safe
  rem=$(( have % PAGESIZE ))
  if [ "$rem" -ne 0 ]; then
    echo "[WARN] partial page detected (have=$have rem=$rem). Rewinding 1 page."
    done_pages=$(( done_pages - 1 ))
    if [ "$done_pages" -lt 0 ]; then done_pages=0; fi
    # truncate file to full pages only (+ header)
    keep_lines=$(( done_pages * PAGESIZE + 1 ))
    tmp="${HITS_TSV}.tmp"
    head -n "$keep_lines" "$HITS_TSV" > "$tmp"
    mv "$tmp" "$HITS_TSV"
    have=$(( $(wc -l < "$HITS_TSV") - 1 ))
  fi
else
  # create file with header
  : > "$HITS_TSV"
  echo -e "accession\ttype\trelease_date\tlinks\tfiles\ttitle" >> "$HITS_TSV"
  have=0
  done_pages=0
fi

start_page=$(( done_pages + 1 ))
echo "[INFO] already_have_rows=$have => start_page=$start_page"

# ---- retry helper (DNS hiccup safe) ----
fetch_page () {
  local page="$1"
  local url="${BASE}?${MODE}=${ENC_TERM}&pageSize=${PAGESIZE}&page=${page}"

  # retry up to 10 times with backoff
  local attempt=1
  while true; do
    if json="$(curl -sS --retry 5 --retry-all-errors --retry-delay 2 \
        -H "Accept: application/json" "$url")"; then
      printf '%s' "$json"
      return 0
    fi
    attempt=$((attempt+1))
    if [ "$attempt" -gt 10 ]; then
      echo "[ERROR] failed page=$page after retries"
      return 1
    fi
    sleep 3
  done
}

# ---- main loop (resume) ----
total_downloaded=0
for page in $(seq "$start_page" "$MAXPAGE"); do
  json="$(fetch_page "$page")"
  n="$(echo "$json" | jq '.hits | length')"
  echo "[PAGE $page/$MAXPAGE] hits=$n"

  if [ "$n" -eq 0 ]; then
    echo "[STOP] empty page at $page"
    break
  fi

  echo "$json" | jq -r '.hits[] | [
      (.accession // ""),
      (.type // ""),
      (.release_date // ""),
      (.links // ""),
      (.files // ""),
      (.title // "" | gsub("\t";" ") | gsub("\n";" "))
    ] | @tsv' >> "$HITS_TSV"

  total_downloaded=$((total_downloaded + n))
  sleep 0.05
done

# rebuild accession list
cut -f1 "$HITS_TSV" | tail -n +2 > "${OUTDIR}/accessions.txt"

echo "[DONE] appended_rows=${total_downloaded}"
echo "[OUT] $HITS_TSV"
echo "[OUT] ${OUTDIR}/accessions.txt"
echo "[COUNT] $(wc -l < "${OUTDIR}/accessions.txt") accessions"

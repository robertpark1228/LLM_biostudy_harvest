#!/usr/bin/env bash
set -euo pipefail

BASE="https://www.ebi.ac.uk/biostudies/api/v1/search"
MODE="content"
TERM="radiation"
PAGESIZE=100
OUTDIR="biostudies_content_radiation"
mkdir -p "$OUTDIR"

enc() { printf '%s' "$1" | jq -sRr @uri; }
ENC_TERM="$(enc "$TERM")"

# 1) Get exact totalHits
META="$(curl -sS -H "Accept: application/json" \
  "${BASE}?${MODE}=${ENC_TERM}&pageSize=1&page=1")"

TOTAL="$(echo "$META" | jq -r '.totalHits')"
EXACT="$(echo "$META" | jq -r '.isTotalHitsExact')"

if [ "$EXACT" != "true" ]; then
  echo "[WARN] isTotalHitsExact is not true (=$EXACT). Will still paginate until empty page."
fi

MAXPAGE=$(( (TOTAL + PAGESIZE - 1) / PAGESIZE ))

echo "[INFO] mode=${MODE} term=${TERM}"
echo "[INFO] totalHits=${TOTAL} exact=${EXACT}"
echo "[INFO] pageSize=${PAGESIZE} => maxPage=${MAXPAGE}"

# 2) Output header
: > "${OUTDIR}/hits.tsv"
echo -e "accession\ttype\trelease_date\tlinks\tfiles\ttitle" >> "${OUTDIR}/hits.tsv"

# 3) Loop pages
total_downloaded=0

for page in $(seq 1 "$MAXPAGE"); do
  url="${BASE}?${MODE}=${ENC_TERM}&pageSize=${PAGESIZE}&page=${page}"
  json="$(curl -sS -H "Accept: application/json" "$url")"
  n="$(echo "$json" | jq '.hits | length')"
  echo "[PAGE $page/$MAXPAGE] hits=$n"

  if [ "$n" -eq 0 ]; then
    echo "[STOP] empty page at $page (unexpected if exact=true)"
    break
  fi

  echo "$json" | jq -r '.hits[] | [
      (.accession // ""),
      (.type // ""),
      (.release_date // ""),
      (.links // ""),
      (.files // ""),
      (.title // "" | gsub("\t";" ") | gsub("\n";" "))
    ] | @tsv' >> "${OUTDIR}/hits.tsv"

  total_downloaded=$((total_downloaded + n))
  sleep 0.05
done

# 4) accession list
cut -f1 "${OUTDIR}/hits.tsv" | tail -n +2 > "${OUTDIR}/accessions.txt"

echo "[DONE] downloaded_rows=${total_downloaded}"
echo "[OUT] ${OUTDIR}/hits.tsv"
echo "[OUT] ${OUTDIR}/accessions.txt"
echo "[COUNT] $(wc -l < "${OUTDIR}/accessions.txt") accessions"

#!/usr/bin/env bash
set -euo pipefail

BASE="https://www.ebi.ac.uk/biostudies/api/v1/search"
MODE="content"
TERM="radiation"
PAGESIZE=100
OUTDIR="biostudies_content_radiation"
OUTTSV="${OUTDIR}/hits_with_content.tsv"

mkdir -p "$OUTDIR"
enc() { printf '%s' "$1" | jq -sRr @uri; }
ENC_TERM="$(enc "$TERM")"

# Get exact totalHits/maxPage
META="$(curl -sS -H "Accept: application/json" \
  "${BASE}?${MODE}=${ENC_TERM}&pageSize=1&page=1")"
TOTAL="$(echo "$META" | jq -r '.totalHits')"
EXACT="$(echo "$META" | jq -r '.isTotalHitsExact')"
MAXPAGE=$(( (TOTAL + PAGESIZE - 1) / PAGESIZE ))

echo "[INFO] totalHits=${TOTAL} exact=${EXACT} pageSize=${PAGESIZE} maxPage=${MAXPAGE}"
echo "[INFO] writing -> ${OUTTSV}"

: > "$OUTTSV"
echo -e "accession\ttype\trelease_date\tlinks\tfiles\ttitle\tcontent_snippet" >> "$OUTTSV"

fetch_page () {
  local page="$1"
  local url="${BASE}?${MODE}=${ENC_TERM}&pageSize=${PAGESIZE}&page=${page}"
  curl -sS --retry 8 --retry-all-errors --retry-delay 2 \
    -H "Accept: application/json" "$url"
}

for page in $(seq 1 "$MAXPAGE"); do
  json="$(fetch_page "$page")"
  n="$(echo "$json" | jq '.hits | length')"
  echo "[PAGE $page/$MAXPAGE] hits=$n"

  if [ "$n" -eq 0 ]; then
    echo "[STOP] empty page at $page"
    break
  fi

  # content snippet is already a single string; we sanitize tabs/newlines anyway.
  echo "$json" | jq -r '.hits[] | [
      (.accession // ""),
      (.type // ""),
      (.release_date // ""),
      (.links // ""),
      (.files // ""),
      (.title // "" | gsub("\t";" ") | gsub("\n";" ")),
      (.content // "" | gsub("\t";" ") | gsub("\n";" ") )
    ] | @tsv' >> "$OUTTSV"

  sleep 0.05
done

echo "[DONE] $(wc -l < "$OUTTSV") lines (includes header)"

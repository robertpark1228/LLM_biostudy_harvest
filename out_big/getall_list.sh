#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# CONFIG
# -----------------------------
SEARCH_MODE="query"          # query | content | title | author | accession | ...
SEARCH_TERM="radiation"
TYPE_FILTER=""              # e.g. "study" (optional)
PAGESIZE=100                # max 100
SORTBY=""                   # numeric fields only: files, links, views (if supported)
SORTORDER="descending"      # ascending | descending
COLLECTION=""               # e.g. "arrayexpress" to use /api/v1/{collection}/search; empty => global

OUTDIR="biostudies_radiation"
mkdir -p "$OUTDIR"

# -----------------------------
# BASE URL
# -----------------------------
if [ -n "$COLLECTION" ]; then
  BASE="https://www.ebi.ac.uk/biostudies/api/v1/${COLLECTION}/search"
else
  BASE="https://www.ebi.ac.uk/biostudies/api/v1/search"
fi

# URL-encode search term safely (jq @uri)
ENC_TERM="$(printf '%s' "$SEARCH_TERM" | jq -sRr @uri)"

echo "[INFO] BASE=$BASE"
echo "[INFO] ${SEARCH_MODE}=${SEARCH_TERM} pageSize=${PAGESIZE}"

# -----------------------------
# OUTPUT HEADER
# -----------------------------
: > "${OUTDIR}/hits.tsv"
echo -e "accession\ttype\trelease_date\tlinks\tfiles\ttitle" >> "${OUTDIR}/hits.tsv"

# -----------------------------
# PAGINATION LOOP
# -----------------------------
page=1
total=0

while true; do
  url="${BASE}?${SEARCH_MODE}=${ENC_TERM}&page=${page}&pageSize=${PAGESIZE}"

  if [ -n "$TYPE_FILTER" ]; then
    url="${url}&type=$(printf '%s' "$TYPE_FILTER" | jq -sRr @uri)"
  fi

  if [ -n "$SORTBY" ]; then
    url="${url}&sortBy=$(printf '%s' "$SORTBY" | jq -sRr @uri)&sortOrder=$(printf '%s' "$SORTORDER" | jq -sRr @uri)"
  fi

  json="$(curl -sS -H "Accept: application/json" "$url")"

  # If server returned HTML, fail fast with a helpful message
  ctype="$(printf '%s' "$json" | head -c 200 | tr -d '\n')"
  if echo "$ctype" | grep -qi '<!doctype html\|<html'; then
    echo "[ERROR] Got HTML instead of JSON. URL was:"
    echo "        $url"
    exit 2
  fi

  n="$(echo "$json" | jq '.hits | length')"
  echo "[PAGE $page] hits=$n"

  if [ "$n" -eq 0 ]; then
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

  total=$((total + n))
  page=$((page + 1))

  sleep 0.1

  # If last page is partial, likely done (safe + faster).
  # If isTotalHitsExact=false and API can under/over-estimate, this is still OK because
  # a partial page indicates end-of-results for that query+sort.
  if [ "$n" -lt "$PAGESIZE" ]; then
    echo "[INFO] last page detected (hits < pageSize)"
    break
  fi
done

echo "[DONE] total_hits_downloaded=$total"
echo "[OUT] ${OUTDIR}/hits.tsv"

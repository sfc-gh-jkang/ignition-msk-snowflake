#!/usr/bin/env bash
# Downloads the v4 connector plugin jars listed in plugin-jars.txt and verifies each SHA-256.
# Usage: fetch_plugin.sh <output-dir>
# Maven Central sometimes answers HTTP 429 from corporate networks; Google's mirror serves the
# same bytes and is tried second.
set -euo pipefail
out="${1:?usage: fetch_plugin.sh <output-dir>}"
here="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$out"
if command -v sha256sum >/dev/null; then sha_check() { sha256sum -c -; }; else sha_check() { shasum -a 256 -c -; }; fi
mirrors=(https://repo1.maven.org/maven2 https://maven-central.storage-download.googleapis.com/maven2)

grep -v '^#' "$here/plugin-jars.txt" | while read -r sha path; do
  [ -n "$sha" ] || continue
  file="$out/$(basename "$path")"
  if [ ! -s "$file" ]; then
    for m in "${mirrors[@]}"; do
      curl -fsSL --retry 3 -o "$file" "$m/$path" && break
    done
  fi
  echo "$sha  $file" | sha_check >/dev/null || { echo "checksum mismatch: $file" >&2; exit 1; }
  echo "ok  $(basename "$path")"
done

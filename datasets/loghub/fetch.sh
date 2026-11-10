#!/usr/bin/env bash
# Download Loghub datasets from Zenodo record 8196385 and verify MD5.
# Raw files land in research/datasets/loghub/raw/ (git-ignored).
#
#   research/datasets/loghub/fetch.sh            # core set (BGL, HDFS_v1, SSH, Linux)
#   research/datasets/loghub/fetch.sh Thunderbird # add the large syslog set (1.9 GiB gz, ~30 GiB raw)
#
# License: Loghub datasets are free for research or academic work; any use
# must reference https://github.com/logpai/loghub and cite Zhu et al.,
# ISSRE 2023 (doi:10.1109/ISSRE59848.2023.00071). Zenodo lists CC-BY-4.0.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
raw="$here/raw"; mkdir -p "$raw"
base="https://zenodo.org/records/8196385/files"

md5_of() {  # bash 3.2 (macOS) has no associative arrays
  case "$1" in
    BGL.zip) echo 4452953c470f2d95fcb32d5f6e733f7a ;;
    HDFS_v1.zip) echo 76a24b4d9a6164d543fb275f89773260 ;;
    SSH.tar.gz) echo a44e40b4348697dabf2bba56885e0a38 ;;
    Linux.tar.gz) echo 6d1802d7778126f21c001c6aa7b6b106 ;;
    Thunderbird.tar.gz) echo 0891b048df2919dc78c99c4428686b44 ;;
  esac
}
files=(BGL.zip HDFS_v1.zip SSH.tar.gz Linux.tar.gz)
[[ "${1:-}" == "Thunderbird" ]] && files+=(Thunderbird.tar.gz)

for f in "${files[@]}"; do
  out="$raw/$f"
  if [[ -f "$out" ]] && [[ "$(md5 -q "$out")" == "$(md5_of "$f")" ]]; then
    echo "ok (cached) $f"; continue
  fi
  echo "fetching $f"
  curl -fL --retry 3 -o "$out.part" "$base/$f?download=1"
  got="$(md5 -q "$out.part")"
  [[ "$got" == "$(md5_of "$f")" ]] || { echo "MD5 mismatch for $f: $got" >&2; exit 1; }
  mv "$out.part" "$out"
  echo "ok $f"
done

# Extract each archive into raw/<name>/
for f in "${files[@]}"; do
  name="${f%%.*}"; dest="$raw/$name"
  [[ -d "$dest" ]] && continue
  mkdir -p "$dest"
  case "$f" in
    *.zip) unzip -q "$raw/$f" -d "$dest" ;;
    *.tar.gz) tar -xzf "$raw/$f" -C "$dest" ;;
  esac
done

# Record what we have: file, lines, bytes, sha256
{
  echo "file,lines,bytes,sha256"
  find "$raw" -mindepth 2 -type f -name '*.log' | sort | while read -r p; do
    printf '%s,%s,%s,%s\n' "${p#"$raw"/}" "$(wc -l < "$p" | tr -d ' ')" \
      "$(stat -f %z "$p")" "$(shasum -a 256 "$p" | cut -d' ' -f1)"
  done
} > "$here/inventory.csv"
echo "inventory written to $here/inventory.csv"

#!/usr/bin/env bash
# Build the V-norecheck ablation engine for E-RQ2-R from a vtop-engine tag (v0.10.x).
# The patch (norecheck-v0.10.patch) is gated on VTOP_ABLATION_NORECHECK: with the variable unset the binary
# behaves as the release; with it set, recovery accepts recorded VERIFIED evidence without re-checking storage.
# Usage (repo root): research/experiments/rq2-corruption-recovery/build_norecheck.sh v0.10.2
# Output: .tmp/vtop-engine-norecheck-<tag>/target/release/vtopctl ; prints its SHA-256 for the run record.
set -euo pipefail
TAG=${1:?tag, e.g. v0.10.2}
REPO=$(git rev-parse --show-toplevel)
ENGINE=${ENGINE_REPO:-$REPO/../vtop-engine}
WT=$REPO/.tmp/vtop-engine-norecheck-$TAG
git -C "$ENGINE" fetch -q --tags origin
[ -d "$WT" ] || git -C "$ENGINE" worktree add --detach "$WT" "$TAG"
cd "$WT"
git apply --check "$REPO/research/experiments/rq2-corruption-recovery/norecheck-v0.10.patch" 2>/dev/null \
  && git apply "$REPO/research/experiments/rq2-corruption-recovery/norecheck-v0.10.patch" \
  || grep -q VTOP_ABLATION_NORECHECK crates/vtop-cli/src/engine.rs   # already applied
# macOS: librdkafka links OpenSSL; Homebrew's default openssl@4 fails with undefined _ENGINE_* (LESSONS.md)
OSSL=$(brew --prefix openssl@3 2>/dev/null || true)
if [ -n "$OSSL" ]; then export OPENSSL_DIR=$OSSL OPENSSL_ROOT_DIR=$OSSL PKG_CONFIG_PATH=$OSSL/lib/pkgconfig; fi
cargo build --release --locked --bin vtopctl
echo "commit: $(git rev-parse --short HEAD) + norecheck patch"
shasum -a 256 target/release/vtopctl

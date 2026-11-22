#!/usr/bin/env python3
"""List every leaf value that differs between two analysis summary.json files (e.g. v0102 vs v0104).

  research/.venv/bin/python research/analysis/scripts/diff_summaries.py research/analysis/v0102/summary.json \
      research/analysis/v0104/summary.json [--out research/analysis/v0104/DIFF-v0102.md] [--rtol 0]

Numbers are compared exactly unless --rtol is given. Keys present on one side only are listed as added/removed.
Bookkeeping keys (generated_from_commit, result_set, vtop_commit) are listed but marked as expected.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

EXPECTED = {"generated_from_commit", "result_set", "vtop_commit"}


def leaves(x, path=()):
    if isinstance(x, dict):
        for k, v in x.items():
            yield from leaves(v, path + (str(k),))
    elif isinstance(x, list) and x and all(isinstance(v, (dict, list)) for v in x):
        for i, v in enumerate(x):
            yield from leaves(v, path + (f"[{i}]",))
    else:
        yield path, x


def same(a, b, rtol):
    if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool):
        return a == b if rtol == 0 else abs(a - b) <= rtol * max(abs(a), abs(b), 1e-300)
    if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        return all(same(x, y, rtol) for x, y in zip(a, b))
    return a == b


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("old")
    ap.add_argument("new")
    ap.add_argument("--out")
    ap.add_argument("--rtol", type=float, default=0.0)
    a = ap.parse_args()
    old = dict(leaves(json.loads(Path(a.old).read_text())))
    new = dict(leaves(json.loads(Path(a.new).read_text())))
    rows, n_same = [], 0
    for k in sorted(set(old) | set(new)):
        name = ".".join(k)
        if k not in new:
            rows.append(("removed", name, old[k], "—"))
        elif k not in old:
            rows.append(("added", name, "—", new[k]))
        elif not same(old[k], new[k], a.rtol):
            rows.append(("expected" if k[0] in EXPECTED else "changed", name, old[k], new[k]))
        else:
            n_same += 1
    out = [f"# Summary differences: `{a.old}` → `{a.new}`", "",
           f"{n_same} leaves equal; {sum(r[0] == 'changed' for r in rows)} changed, "
           f"{sum(r[0] == 'added' for r in rows)} added, {sum(r[0] == 'removed' for r in rows)} removed"
           + (f" (rtol {a.rtol})" if a.rtol else " (exact)") + ".", "",
           "| Kind | Key | Old | New |", "|---|---|---|---|"]
    for kind, name, o, n in rows:
        out.append(f"| {kind} | `{name}` | {json.dumps(o)[:120]} | {json.dumps(n)[:120]} |")
    text = "\n".join(out) + "\n"
    if a.out:
        Path(a.out).write_text(text)
    print(text if not a.out else out[2])


if __name__ == "__main__":
    main()

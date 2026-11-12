#!/usr/bin/env python3
"""Generate protocol/model figures from VTOP source rules and experiment data.

The diagram encodes only v0.7.0 behavior documented in research/state-machine.
The model-boundary chart reads the actual model-sweep results. Existing RQ
figures are copied from research/analysis after analyze.py generates them.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import matplotlib
matplotlib.use("pdf")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

ROOT = Path(__file__).resolve().parents[3]
RESEARCH = ROOT / "research"
OUT = ROOT / "VTOP-IEEE-TCC" / "figures"
TABLE_OUT = ROOT / "VTOP-IEEE-TCC" / "tables"
PREVIEW = ROOT / ".tmp" / "figure-previews"
OUT.mkdir(parents=True, exist_ok=True)
TABLE_OUT.mkdir(parents=True, exist_ok=True)
PREVIEW.mkdir(parents=True, exist_ok=True)

plt.rcParams.update({
    "font.family": "serif", "font.size": 8, "pdf.fonttype": 42,
    "figure.dpi": 300, "axes.labelsize": 8, "xtick.labelsize": 7,
    "ytick.labelsize": 7, "legend.fontsize": 7,
})
NAVY = "#17365d"
BLUE = "#2a78d6"
TEAL = "#16877a"
RED = "#c1463d"
AMBER = "#d9a441"
GRAY = "#607080"
LIGHT = "#f4f7fa"


def box(ax, xy, wh, text, color=BLUE, size=7.2):
    x, y = xy
    w, h = wh
    patch = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.025,rounding_size=0.035",
                           linewidth=0.9, edgecolor=color, facecolor="white")
    ax.add_patch(patch)
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", color=NAVY, fontsize=size)
    return patch


def arrow(ax, start, end, color=GRAY, style="-|>", lw=0.9, rad=0):
    ax.add_patch(FancyArrowPatch(start, end, arrowstyle=style, mutation_scale=9,
                                 color=color, linewidth=lw, connectionstyle=f"arc3,rad={rad}"))


def protocol_model():
    fig, ax = plt.subplots(figsize=(7.1, 3.75))
    ax.set_xlim(0, 14.2)
    ax.set_ylim(-0.1, 7.5)
    ax.axis("off")
    fig.patch.set_facecolor("white")
    ax.text(0.35, 7.12, "Verification-bound normal path", fontsize=10, fontweight="bold", color=NAVY)
    source = box(ax, (0.35, 5.65), (1.8, 0.8), "Source unit\nKafka · file · spool", BLUE, 6.0)
    seal = box(ax, (2.55, 5.65), (1.8, 0.8), "Seal range\none buffer/unit", BLUE, 6.0)
    upload = box(ax, (4.75, 5.65), (1.8, 0.8), "Upload object\n+ manifest", BLUE, 6.0)
    verify = box(ax, (6.95, 5.65), (1.8, 0.8), "Verify stored\nevidence", TEAL, 6.0)
    commit = box(ax, (9.15, 5.65), (1.8, 0.8), "Read-order\nadapter commit", TEAL, 6.0)
    ledger = box(ax, (11.35, 5.65), (1.8, 0.8), "Ledger\nevidence + cursor", NAVY, 6.0)
    replay = box(ax, (5.35, 4.0), (2.4, 0.75), "FAILED → REPLAY\nrewind source range", RED, 6.0)
    for a, b in [(source, seal), (seal, upload), (upload, verify), (verify, commit), (commit, ledger)]:
        arrow(ax, (a.get_x() + a.get_width(), a.get_y() + a.get_height()/2),
              (b.get_x(), b.get_y() + b.get_height()/2))
    arrow(ax, (7.85, 5.65), (6.55, 4.78), RED)
    ax.text(7.95, 4.25, "next read starts at the first\nunarchived position", fontsize=6.2, color=RED)

    ax.text(0.35, 3.05, "Crash recovery", fontsize=10, fontweight="bold", color=NAVY)
    seed = box(ax, (0.35, 1.65), (1.8, 0.75), "Seed generation-\naware cursors", NAVY, 6.0)
    claim = box(ax, (2.85, 1.65), (1.8, 0.75), "Claim incomplete\nbatches", NAVY, 6.0)
    recheck = box(ax, (5.35, 1.65), (1.8, 0.75), "Revalidate object\n+ manifest", TEAL, 6.0)
    hold = box(ax, (7.85, 1.65), (1.8, 0.75), "Hold back higher\nverified ranges", AMBER, 6.0)
    finish = box(ax, (10.35, 1.65), (1.8, 0.75), "Commit safe prefix\nor replay gap", TEAL, 6.0)
    for a, b in [(seed, claim), (claim, recheck), (recheck, hold), (hold, finish)]:
        arrow(ax, (a.get_x() + a.get_width(), a.get_y() + a.get_height()/2),
              (b.get_x(), b.get_y() + b.get_height()/2))
    ax.text(0.35, 0.82, "Precondition: one in-flight batch per source unit per cycle.",
            fontsize=6.5, color=NAVY)
    ax.text(0.35, 0.43, "At-least-once objects: replay may create duplicates; the ledger holds complete ranges.",
            fontsize=6.5, color=GRAY)
    ax.text(0.35, 0.04, "Recovery revalidates fresh evidence before committing.",
            fontsize=6.5, color=GRAY)
    fig.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01)
    fig.savefig(RESEARCH / "analysis/figures/alt-protocol-flow.pdf")  # superseded by diagram-lifecycle
    fig.savefig(PREVIEW / "alt-protocol-flow.png", dpi=220)
    plt.close(fig)


def model_boundary():
    rows = json.loads((RESEARCH / "experiments/rq1-source-safety/results/20261110-model-sweep/sweep.json").read_text())
    selected = []
    for r in rows:
        p = r["params"]
        if p["variant"] == "v060" and p["fails"] == 1 and p["crashes"] == 0 and p["per_cycle"] == 1:
            selected.append(("v0.6.0\nB=1 failure", r["states"], "violation"))
        if p["variant"] == "v070" and p["ranges"] == 4 and p["per_cycle"] == 1:
            selected.append(("v0.7.0\nB=1 holds", r["states"], "holds"))
        if p["variant"] == "v070" and p["per_cycle"] == 2 and p["fails"] == 1 and p["crashes"] == 0:
            selected.append(("v0.7.0\nB=2 failure", r["states"], "violation"))
    if len(selected) != 3:
        raise RuntimeError(f"Expected three model rows, found {len(selected)}")
    selected.sort(key=lambda item: item[1])
    fig, ax = plt.subplots(figsize=(3.5, 2.35))
    labels = [s[0] for s in selected]
    values = [s[1] for s in selected]
    outcomes = [s[2] for s in selected]
    colors = [TEAL if o == "holds" else RED for o in outcomes]
    bars = ax.barh(range(3), values, color=colors, height=0.55)
    ax.set_yticks(range(3), labels)
    ax.set_xscale("log")
    ax.set_xlabel("Explored states (log scale)")
    ax.grid(axis="x", color="#e4e3df", linewidth=0.5)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    for bar, value, outcome in zip(bars, values, outcomes):
        ax.text(value * 1.18, bar.get_y() + bar.get_height()/2, f"{value:,} — {outcome}",
                va="center", fontsize=7, color=NAVY)
    ax.set_xlim(20, max(values) * 4)
    fig.tight_layout(pad=0.35)
    fig.savefig(RESEARCH / "analysis/figures/alt-model-boundary.pdf")  # superseded by diagram-counterexample
    fig.savefig(PREVIEW / "alt-model-boundary.png", dpi=220)
    plt.close(fig)


def package_existing():
    # IEEE numbering = order of first callout in the manuscript. Diagrams come
    # from make_paper_diagrams.py (run it first); data figures from analyze.py.
    mapping = {
        "diagram-architecture.pdf": "suliman-fig1.pdf",    # Sec. III
        "diagram-lifecycle.pdf": "suliman-fig2.pdf",       # Sec. IV
        "diagram-counterexample.pdf": "suliman-fig3.pdf",  # Sec. V
        # rq1-duplicates.pdf is artifact-only since C15 (Table 4 carries the duplicates).
        "rq3-latency.pdf": "suliman-fig4.pdf",             # Sec. VII, RQ3 (emulated latency)
        "rq3-throughput.pdf": "suliman-figS3.pdf",   # supplemental material (local throughput)
        "rq3-stages.pdf": "suliman-figS2.pdf",       # supplemental material
        "rq3-batchsize.pdf": "suliman-figS1.pdf",       # supplemental material
    }
    for source, target in mapping.items():
        shutil.copyfile(RESEARCH / "analysis/figures" / source, OUT / target)
    for source in (RESEARCH / "analysis/tables").glob("*.tex"):
        if source.stem.endswith("_full"):  # complete versions live in the artifact only
            continue
        shutil.copyfile(source, TABLE_OUT / source.name)


if __name__ == "__main__":
    protocol_model()
    model_boundary()
    package_existing()
    print(f"Generated figures in {OUT} and tables in {TABLE_OUT}")

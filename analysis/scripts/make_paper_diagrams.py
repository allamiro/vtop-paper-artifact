#!/usr/bin/env python3
"""Diagrams for the VTOP paper, drawn from facts recorded in research/.

Every box and arrow encodes VTOP v0.7.0 (tag 6c8f386) behaviour documented in
research/state-machine/README.md, or a counterexample trace produced by
research/state-machine/model/vtop_model.py. Nothing here is measured data.

Outputs (vector PDF, IEEE sizes) in research/analysis/figures/:
  diagram-architecture.pdf    single column  (Section III / VI)
  diagram-lifecycle.pdf       double column  (Section IV; compiled from VTOP-IEEE-TCC/figures/src/lifecycle.tex)
  diagram-counterexample.pdf  single column  (Section V, replaces the bar chart)

Style matches research/analysis/scripts/make_protocol_figures.py (same palette
and serif font) so the paper's figures read as one set; hatching keeps the
failure states distinguishable in grayscale.
"""
from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("pdf")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Rectangle  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "research" / "analysis" / "figures"
PREVIEW = Path(sys.argv[1]) if len(sys.argv) > 1 else None

plt.rcParams.update({"font.family": "serif", "font.size": 7, "pdf.fonttype": 42, "figure.dpi": 300,
                     "mathtext.fontset": "dejavuserif"})
NAVY, BLUE, TEAL, RED, AMBER, GRAY = "#17365d", "#2a78d6", "#16877a", "#c1463d", "#d9a441", "#607080"
INK, MUTED, FILL = "#1d2733", "#52606d", "#f4f7fa"


def box(ax, x, y, w, h, text, edge=NAVY, fill="white", size=6.6, weight="normal", hatch=None, lw=0.9, color=INK,
        ls="solid"):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0,rounding_size=0.06", linewidth=lw,
                                edgecolor=edge, facecolor=fill, hatch=hatch, linestyle=ls))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=size, color=color,
            fontweight=weight, linespacing=1.15)


def arrow(ax, p, q, color=GRAY, lw=0.8, style="-|>", rad=0.0, ls="-"):
    ax.add_patch(FancyArrowPatch(p, q, arrowstyle=style, mutation_scale=6, color=color, linewidth=lw,
                                 connectionstyle=f"arc3,rad={rad}", linestyle=ls, shrinkA=0, shrinkB=0))


def label(ax, x, y, text, color=MUTED, size=6, ha="center", va="center", style="normal"):
    ax.text(x, y, text, ha=ha, va=va, fontsize=size, color=color, style=style, linespacing=1.15)


def canvas(w_in, h_in, w_units, h_units):
    fig, ax = plt.subplots(figsize=(w_in, h_in))
    ax.set_xlim(0, w_units)
    ax.set_ylim(0, h_units)
    ax.set_aspect("auto")
    ax.axis("off")
    fig.subplots_adjust(0, 0, 1, 1)
    return fig, ax


def save(fig, name):
    OUT.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT / f"{name}.pdf")
    if PREVIEW:
        PREVIEW.mkdir(parents=True, exist_ok=True)
        fig.savefig(PREVIEW / f"{name}.png", dpi=200)
    plt.close(fig)


# --------------------------------------------------------------------------- architecture
def architecture():
    """Source units -> adapters -> engine -> object store + ledger; commit and rewind return through the adapters.

    Checked against v0.7.0: every read, rewind and commit goes through the unit's adapter; the engine seals one
    batch per unit, compresses and digests it, puts object and manifest, reads the stored evidence back, then
    commits in read order and hands every failed range back (rewind); each state is written to the ledger.
    """
    fig, ax = canvas(3.5, 2.26, 10.0, 7.5)
    top = 6.85                                             # column headers sit just above the boxes
    # column 1: source units
    label(ax, 1.27, top + 0.25, "Source units", color=NAVY, size=6.6)
    # each source type has its own border colour; borders are solid because all three are implemented and
    # evaluated (a dotted border would read as optional or untested, and dashed marks the rewind path)
    for i, (name, prog, edge, ls) in enumerate([
            ("Kafka partition", "offset; $C(u)$ at broker", "#c8761c", "solid"),
            ("File generation", "byte; $C(u)$ in ledger", "#6f4fa8", "solid"),
            ("Syslog spool", "byte; $C(u)$ in ledger", "#4f8a1f", "solid")]):
        y = 5.75 - i * 1.3
        box(ax, 0.05, y, 2.45, 1.0, f"{name}\n{prog}", edge=edge, size=5.4, lw=1.3, ls=ls)
        arrow(ax, (2.5, y + 0.5), (2.8, y + 0.5))                     # read through the adapter
        arrow(ax, (3.22, y + 0.5), (3.5, y + 0.5))                    # records into the engine
    # adapters: a strip every read, rewind and commit passes through
    # flat light tint (no gradient: it would vary text contrast and print poorly in grayscale)
    ax.add_patch(FancyBboxPatch((2.8, 1.05), 0.42, 5.7, boxstyle="round,pad=0,rounding_size=0.06",
                                linewidth=0.9, edgecolor=NAVY, facecolor="#e3ebf5"))
    ax.text(3.01, 3.9, "adapters:  read,  rewind,  $\\mathsf{commit}(u,e)$", rotation=90, ha="center",
            va="center", fontsize=5.9, color=INK)
    # column 2: engine pipeline
    ax.add_patch(FancyBboxPatch((3.5, 0.95), 2.95, 5.85, boxstyle="round,pad=0,rounding_size=0.06",
                                linewidth=0.9, edgecolor=NAVY, facecolor=FILL))
    label(ax, 4.98, top + 0.25, "VTOP engine", color=NAVY, size=6.6)
    steps = [("seal batch (one\nbuffer per unit)", BLUE), ("compress, digest", BLUE),
             ("upload object\nand manifest", BLUE), ("verify stored\nevidence", TEAL),
             ("commit in read order;\nhand back failures", NAVY)]
    for i, (s_, edge) in enumerate(steps):
        y = 5.85 - i * 1.15
        box(ax, 3.63, y, 2.7, 0.8, s_, edge=edge, size=5.5, lw=1.2 if edge != BLUE else 0.9)
        if i:
            arrow(ax, (4.98, y + 1.15), (4.98, y + 0.8))
    commit_y = 5.85 - 4 * 1.15 + 0.4
    arrow(ax, (3.63, commit_y + 0.17), (3.22, commit_y + 0.17), color=NAVY, lw=1.0)            # commit
    arrow(ax, (3.63, commit_y - 0.17), (3.22, commit_y - 0.17), color=RED, lw=0.9, ls="--")    # rewind
    # column 3: object store and ledger, with a clear channel for "put" and "read"
    label(ax, 8.62, top + 0.25, "Archival storage", color=NAVY, size=6.6)
    box(ax, 7.3, 2.5, 2.65, 4.3, "Object store\n\nobject (zstd) and\nJSON manifest;\nnew key per attempt",
        edge=TEAL, size=5.8)
    up_y, ver_y = 5.85 - 2 * 1.15 + 0.4, 5.85 - 3 * 1.15 + 0.55
    gap_x = (6.45 + 7.3) / 2
    arrow(ax, (6.33, up_y), (7.3, up_y), color=GRAY)
    label(ax, gap_x, up_y + 0.22, "put", size=5.6)
    arrow(ax, (7.3, ver_y), (6.33, ver_y), color=TEAL)
    label(ax, gap_x, ver_y + 0.22, "read", size=5.6, color=TEAL)
    box(ax, 7.3, 0.95, 2.65, 1.35, "Recovery ledger\n(local SQLite, WAL)\nbatch states, $R_b$,\ndigests, owner lease",
        edge=NAVY, size=5.6)
    arrow(ax, (6.45, 1.62), (7.3, 1.62), color=NAVY, style="<|-|>")
    label(ax, 5.0, 0.6, "dashed: a failed range is handed back (rewind) and re-read as a new batch",
          size=5.4, color=RED, style="italic")
    label(ax, 5.0, 0.22, "restart: VERIFIED rows are re-checked against storage and ledger before commit",
          size=5.4, color=TEAL, style="italic")
    save(fig, "diagram-architecture")


# --------------------------------------------------------------------------- lifecycle
TIKZ = ROOT / "VTOP-IEEE-TCC" / "figures" / "src"


# TikZ-UML (N. Kielbasiewicz) is not in TeX Live. Fig. 2 pins release v1.0 (2016-03-29) by SHA-256 and
# caches it under .tmp/ (git-ignored); the package file has no licence header, so it is not committed.
TIKZUML_URL = "https://perso.ensta-paris.fr/~kielbasi/tikzuml/var/files/src/tikzuml-v1.0-2016-03-29.tbz"
TIKZUML_SHA256 = "0cbc4a20c8ed40162b3b9ab1400b174293ec1ad7508e640ca879e78c45a105d0"


def tikzuml_dir() -> Path:
    """Return a directory holding tikz-uml.sty, downloading and checking the pinned release if needed."""
    import hashlib
    import tarfile
    import urllib.request
    cache = ROOT / ".tmp" / "tikzuml"
    sty = cache / "tikzuml-v1.0-2016-03-29" / "tikz-uml.sty"
    if sty.exists():
        return sty.parent
    cache.mkdir(parents=True, exist_ok=True)
    archive = cache / "tikzuml.tbz"
    with urllib.request.urlopen(TIKZUML_URL, timeout=60) as resp:
        archive.write_bytes(resp.read())
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    if digest != TIKZUML_SHA256:
        archive.unlink()
        raise SystemExit(f"tikz-uml archive hash mismatch: {digest}")
    with tarfile.open(archive, "r:bz2") as tf:
        member = tf.getmember("tikzuml-v1.0-2016-03-29/tikz-uml.sty")
        tf.extract(member, cache, filter="data")
    return sty.parent


def lifecycle():
    """Compile the TikZ-UML state machine (figures/src/lifecycle.tex) to diagram-lifecycle.pdf.

    The state machine is drawn in TikZ, not matplotlib, so its labels, guards and line styles stay
    editable as LaTeX. pdflatex runs in a temporary directory so no build files land in the repo.
    """
    import os
    import shutil
    import subprocess
    import tempfile
    src = TIKZ / "lifecycle.tex"
    sty = tikzuml_dir()
    with tempfile.TemporaryDirectory() as tmp:
        shutil.copy(src, tmp)
        env = dict(os.environ, TEXINPUTS=f"{sty}:" + os.environ.get("TEXINPUTS", ""))
        r = subprocess.run(["pdflatex", "-interaction=nonstopmode", "-halt-on-error", "lifecycle.tex"],
                           cwd=tmp, capture_output=True, text=True, env=env)
        if r.returncode:
            raise SystemExit("pdflatex failed on %s:\n%s" % (src, r.stdout[-1500:]))
        OUT.mkdir(parents=True, exist_ok=True)
        shutil.copy(Path(tmp) / "lifecycle.pdf", OUT / "diagram-lifecycle.pdf")
        if PREVIEW:
            PREVIEW.mkdir(parents=True, exist_ok=True)
            subprocess.run(["pdftoppm", "-r", "200", "-png", "-singlefile", str(OUT / "diagram-lifecycle.pdf"),
                            str(PREVIEW / "diagram-lifecycle")], check=False)


# --------------------------------------------------------------------------- counterexample
def counterexample():
    """Source positions and the committed marker C in the model's shortest traces."""
    fig, ax = canvas(3.5, 1.8, 10.0, 6.0)
    rows = [
        ("Guard-only, $B=1$", "r0 fails; next cycle r1 verifies and\ncommits; failed range not handed back",
         ["fail", "ok", "none"], True),
        ("VTOP, $B=2$", "r0 and r1 sealed in one cycle; r0 fails,\nr1 commits before r0 is handed back",
         ["fail", "ok", "none"], True),
        ("VTOP, $B=1$", "r0 fails, is rewound and re-read;\n$C$ advances only over verified ranges",
         ["replay", "ok", "none"], False),
    ]
    x0, rw, rh = 3.15, 1.9, 0.62
    label(ax, x0 + 1.5 * rw, 5.75, "source positions of one unit", size=5.8, style="italic")
    for j, name in enumerate(["r0", "r1", "r2"]):
        label(ax, x0 + (j + 0.5) * rw, 5.4, name, size=6.2, color=INK)
    for i, (title, note, kinds, bad) in enumerate(rows):
        y = 4.35 - i * 1.75
        label(ax, 0.05, y + rh / 2 + 0.1, title, color=NAVY, size=6.6, ha="left")
        label(ax, 0.05, y - 0.4, note, size=5.2, ha="left", style="italic")
        for j, k in enumerate(kinds):
            x = x0 + j * rw
            if k == "fail":
                ax.add_patch(Rectangle((x, y), rw - 0.08, rh, facecolor="white", edgecolor=RED, hatch="////", lw=0.8))
                ax.text(x + rw / 2 - 0.04, y + rh / 2, "FAILED", ha="center", va="center", fontsize=5.8, color=RED,
                        fontweight="bold", bbox=dict(boxstyle="square,pad=0.12", facecolor="white", edgecolor="none"))
            elif k in ("ok", "replay"):
                ax.add_patch(Rectangle((x, y), rw - 0.08, rh, facecolor=BLUE, edgecolor=BLUE, lw=0.8))
                label(ax, x + rw / 2 - 0.04, y + rh / 2, "re-read,\nverified" if k == "replay" else "verified",
                      color="white", size=5.5)
            else:
                ax.add_patch(Rectangle((x, y), rw - 0.08, rh, facecolor="white", edgecolor="#c9d1d9", lw=0.6))
        cx = x0 + 2 * rw - 0.04
        color = RED if bad else BLUE
        ax.plot([cx, cx], [y - 0.12, y + rh + 0.12], color=color, lw=1.6)
        label(ax, cx + 0.12, y + rh + 0.2, "$C$", color=color, size=6.6, ha="left")
        label(ax, x0 + 3 * rw - 0.08, y - 0.46, "unsafe: $C$ passes r0" if bad else "no violation", color=color,
              size=5.8, ha="right")
    save(fig, "diagram-counterexample")


if __name__ == "__main__":
    architecture()
    lifecycle()
    counterexample()
    print("wrote", sorted(p.name for p in OUT.glob("diagram-*.pdf")))

#!/usr/bin/env python3
"""Fig. (draft): where the E-RQ5 engines ran and what distance cost them.

Reads the combined E-RQ5-WAN summary and the E-RQ5-COST results and draws, on a light world map, the S3 bucket
(us-east-1) and the four engine regions with their measured RTT and the verification cost of each method
(difference of medians vs size-only, SSH, v0.10.2). Lines are drawn region→bucket as straight map lines: they mark
which pairs were measured, not network paths.

Land outlines: Natural Earth 1:110m land (public domain), research/analysis/data/naturalearth/ne_110m_land.geojson,
SHA-256 9e0729ee253ca7d7a5c4ae9395fb1902264c5377c52e224d13dd85010e2835d9 (downloaded 2026-11-21 from
github.com/nvkelso/natural-earth-vector, geojson/ne_110m_land.geojson).

Usage: research/.venv/bin/python research/analysis/scripts/make_region_map.py [out.pdf]
Default output: research/analysis/figures/rq5-regions.pdf (draft; packaging into suliman-figN is a separate step).
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Polygon  # noqa: E402

REPO = Path(__file__).resolve().parents[3]
LAND = REPO / "research/analysis/data/naturalearth/ne_110m_land.geojson"
LAND_SHA256 = "9e0729ee253ca7d7a5c4ae9395fb1902264c5377c52e224d13dd85010e2835d9"
WAN = REPO / "research/experiments/rq5-cloud/wan/results/combined/summary.json"
OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else REPO / "research/analysis/figures/rq5-regions.pdf"

# Approximate location of each AWS region's metro area (for placement only, not data-centre sites).
SITES = {
    "us-east-1": ("N. Virginia", 38.9, -77.4),
    "eu-west-1": ("Ireland", 53.3, -6.3),
    "sa-east-1": ("São Paulo", -23.5, -46.6),
    "ap-northeast-1": ("Tokyo", 35.7, 139.7),
    "ap-southeast-2": ("Sydney", -33.9, 151.2),
}
SHIFT_AT = 20.0  # longitudes east of this are drawn at lon - 360, so the Pacific sits left of the Americas
XLIM, YLIM = (-235.0, 22.0), (-50.0, 72.0)

NAVY, BLUE, TEAL, RED, GOLD = "#17365d", "#2a78d6", "#16877a", "#c1463d", "#d9a441"
plt.rcParams.update({"font.family": "serif", "font.size": 7, "pdf.fonttype": 42, "ps.fonttype": 42})


def x(lon: float) -> float:
    return lon - 360.0 if lon > SHIFT_AT else lon


def land_rings():
    data = LAND.read_bytes()
    if hashlib.sha256(data).hexdigest() != LAND_SHA256:
        raise SystemExit(f"{LAND}: unexpected SHA-256")
    for f in json.loads(data)["features"]:
        g = f["geometry"]
        polys = g["coordinates"] if g["type"] == "MultiPolygon" else [g["coordinates"]]
        for poly in polys:
            yield poly[0]  # outer ring; 1:110m land has no holes worth drawing at this size


def main():
    wan = json.loads(WAN.read_text())["regions"]
    fig, ax = plt.subplots(figsize=(3.5, 2.05))
    for ring in land_rings():
        for off in (0.0, -360.0):  # draw every landmass twice and let the axes clip it
            ax.add_patch(Polygon([(lo + off, la) for lo, la in ring], closed=True,
                                 facecolor="#e9edf2", edgecolor="#c9d1db", linewidth=0.3, zorder=1))
    bname, blat, blon = SITES["us-east-1"]
    for reg, r in wan.items():
        name, lat, lon = SITES[reg]
        ax.plot([x(blon), x(lon)], [blat, lat], color=BLUE, linewidth=0.8, alpha=0.9, zorder=2,
                solid_capstyle="round")
    ax.scatter([x(blon)], [blat], marker="*", s=70, color=RED, edgecolor="white", linewidth=0.4, zorder=4)
    ax.annotate("S3 bucket\nus-east-1", (x(blon), blat), xytext=(-5, 5), textcoords="offset points",
                fontsize=6.5, color=RED, ha="right", va="bottom", fontweight="bold", zorder=5, linespacing=1.05)
    # label placement per region (points), chosen so labels clear the coasts and each other
    place = {"eu-west-1": (-5, 9, "right"), "sa-east-1": (6, -1, "left"),
             "ap-northeast-1": (3, -15, "left"), "ap-southeast-2": (5, 13, "left")}
    for reg, r in wan.items():
        name, lat, lon = SITES[reg]
        rtt = r["rtt_ms_per_run_median"]
        sha = r["native"]["sha256-store"]["vs_size_only_pct"]
        rb = r["native"]["blake3-readback"]["vs_size_only_pct"]
        ax.scatter([x(lon)], [lat], s=16, color=NAVY, edgecolor="white", linewidth=0.4, zorder=4)
        dx, dy, ha = place[reg]
        signed = lambda v: f"{v:+.0f}%".replace("-", "\u2212")  # true minus sign  # noqa: E731
        sha_txt = signed(sha[0]) + ("" if sha[2] < 0 else "*")  # * = CI includes 0
        ax.annotate(f"{name}, {rtt:.0f} ms\nSHA-256 {sha_txt}\nread-back {signed(rb[0])}",
                    (x(lon), lat), xytext=(dx, dy), textcoords="offset points", ha=ha, va="center",
                    fontsize=5.6, color=NAVY, zorder=5, linespacing=1.15)
    ax.set_xlim(*XLIM)
    ax.set_ylim(*YLIM)
    ax.set_aspect("equal")
    ax.axis("off")
    # The reading notes (RTT method, cost definition, * = CI includes 0, lines are not routes) go in the caption.
    fig.subplots_adjust(0, 0, 1, 1)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT, bbox_inches="tight", pad_inches=0.02)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()

"""ChunkSwarm benchmarks: measure the system and draw the graphs for the report.

Everything runs on this computer, over real TCP sockets on 127.0.0.1:
real PeerServers send chunks, the real downloader receives and verifies them.
(The tracker is skipped: it only does discovery, not transfer.)

Experiments
-----------
1. sources     1, 2, 3, 4 peers, each limited to UPLOAD_LIMIT  -> does parallel scale?
2. chunk_size  256 KiB / 1 MiB / 4 MiB, one unlimited peer     -> overhead per request
3. file_size   10 / 50 / 100 / 200 MB, one unlimited peer      -> is throughput stable?
4. recovery    3 limited peers, one dies halfway vs. no failure -> cost of failover

Each configuration runs RUNS times; we report the median and the min-max range.

Run:   pip install matplotlib
       python scripts/bench.py            (about 3-4 minutes)
       python scripts/bench.py --quick    (small files, 1 run: a 20-second check)
Output: bench/results.csv, bench/summary.md, bench/*.png
"""

from __future__ import annotations

import argparse
import csv
import os
import platform
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

from chunkswarm.downloader import Source, download
from chunkswarm.peer_server import PeerServer

MB = 1024 * 1024
UPLOAD_LIMIT = 5 * MB          # bytes/second per peer in the "limited" experiments
OUT = Path("bench")


class DyingServer(PeerServer):
    """A peer that serves `die_after` chunks, then disappears."""

    die_after = 10

    def _read_chunk(self, path, manifest, index):
        if self.stats["chunks_served"] >= self.die_after:
            self.stop()
            raise ConnectionResetError
        return super()._read_chunk(path, manifest, index)


def make_file(folder: Path, size: int) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"test_{size // MB}MB.bin"
    if not path.exists():
        with path.open("wb") as f:
            left = size
            while left:
                block = min(left, 8 * MB)
                f.write(os.urandom(block))
                left -= block
    return path


def start_peers(work: Path, data: Path, n: int, chunk_size: int, limit: float,
                classes: list[type] | None = None) -> tuple[list[PeerServer], object]:
    servers, manifest = [], None
    for i in range(n):
        share = work / f"peer{i}"
        share.mkdir(parents=True, exist_ok=True)
        target = share / data.name
        if not target.exists():
            shutil.copyfile(data, target)
        cls = classes[i] if classes else PeerServer
        srv = cls(share, chunk_size=chunk_size, upload_limit=limit).start()
        manifest = srv.share(target)
        servers.append(srv)
    return servers, manifest


def timed_download(manifest, servers, work: Path) -> tuple[float, float]:
    dl = work / "dl"
    if dl.exists():
        shutil.rmtree(dl)
    res = download(manifest, [Source("127.0.0.1", s.port) for s in servers], dl)
    return res.seconds, res.mb_per_s


def summarise(rows: list[dict], experiment: str, key: str) -> list[dict]:
    out = []
    for value in dict.fromkeys(r[key] for r in rows if r["experiment"] == experiment):
        runs = [r for r in rows if r["experiment"] == experiment and r[key] == value]
        speeds = [r["mb_per_s"] for r in runs]
        secs = [r["seconds"] for r in runs]
        out.append({key: value, "mb_per_s": statistics.median(speeds),
                    "mb_lo": min(speeds), "mb_hi": max(speeds),
                    "seconds": statistics.median(secs),
                    "s_lo": min(secs), "s_hi": max(secs)})
    return out


# --------------------------------------------------------------- experiments
def exp_sources(work, rows, runs, size):
    data = make_file(work / "src", size)
    for n in (1, 2, 3, 4):
        servers, m = start_peers(work / f"sources{n}", data, n, MB, UPLOAD_LIMIT)
        for r in range(runs):
            s, mbps = timed_download(m, servers, work / f"sources{n}")
            rows.append({"experiment": "sources", "sources": n, "chunk_kib": 1024,
                         "file_mb": size // MB, "run": r + 1, "seconds": s, "mb_per_s": mbps})
            print(f"  sources={n} run {r + 1}: {mbps:6.1f} MB/s  ({s:.2f} s)")
        for srv in servers:
            srv.stop()


def exp_chunk_size(work, rows, runs, size):
    data = make_file(work / "src", size)
    for kib in (256, 1024, 4096):
        servers, m = start_peers(work / f"chunk{kib}", data, 1, kib * 1024, 0)
        for r in range(runs):
            s, mbps = timed_download(m, servers, work / f"chunk{kib}")
            rows.append({"experiment": "chunk_size", "sources": 1, "chunk_kib": kib,
                         "file_mb": size // MB, "run": r + 1, "seconds": s, "mb_per_s": mbps})
            print(f"  chunk={kib} KiB run {r + 1}: {mbps:6.1f} MB/s  ({s:.2f} s)")
        servers[0].stop()


def exp_file_size(work, rows, runs, sizes):
    for size in sizes:
        data = make_file(work / "src", size)
        servers, m = start_peers(work / f"size{size // MB}", data, 1, MB, 0)
        for r in range(runs):
            s, mbps = timed_download(m, servers, work / f"size{size // MB}")
            rows.append({"experiment": "file_size", "sources": 1, "chunk_kib": 1024,
                         "file_mb": size // MB, "run": r + 1, "seconds": s, "mb_per_s": mbps})
            print(f"  file={size // MB} MB run {r + 1}: {mbps:6.1f} MB/s  ({s:.2f} s)")
        servers[0].stop()


def exp_recovery(work, rows, runs, size):
    data = make_file(work / "src", size)
    half = (size // MB) // 6          # one of 3 peers would normally serve 1/3; it dies at ~1/2 of that
    for label, classes in (("no failure", None),
                           ("one peer dies", [DyingServer, PeerServer, PeerServer])):
        for r in range(runs):
            DyingServer.die_after = max(1, half)
            folder = work / f"recovery_{label.replace(' ', '_')}_{r}"
            servers, m = start_peers(folder, data, 3, MB, UPLOAD_LIMIT, classes)
            s, mbps = timed_download(m, servers, folder)
            rows.append({"experiment": "recovery", "case": label, "sources": 3, "chunk_kib": 1024,
                         "file_mb": size // MB, "run": r + 1, "seconds": s, "mb_per_s": mbps})
            print(f"  {label:13} run {r + 1}: {mbps:6.1f} MB/s  ({s:.2f} s)")
            for srv in servers:
                srv.stop()


# --------------------------------------------------------------- graphs
INK, MUTED, GRID, SURFACE, BLUE = "#0b0b0b", "#52514e", "#e5e4df", "#fcfcfb", "#2a78d6"


def _axes(title: str, subtitle: str):
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(6.4, 3.8), dpi=160)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(MUTED)
    ax.tick_params(colors=MUTED, length=0)
    ax.yaxis.grid(True, color=GRID, linewidth=1)
    ax.set_axisbelow(True)
    fig.text(0.02, 0.95, title, fontsize=12, fontweight="bold", color=INK, va="top")
    fig.text(0.02, 0.88, subtitle, fontsize=9, color=MUTED, va="top")
    fig.subplots_adjust(top=0.78, left=0.11, right=0.97, bottom=0.15)
    return fig, ax


def _bars(ax, labels, values, lo, hi, unit):
    import numpy as np
    x = np.arange(len(labels))
    ax.bar(x, values, width=0.45, color=BLUE, zorder=2)
    err = [np.array(values) - np.array(lo), np.array(hi) - np.array(values)]
    ax.errorbar(x, values, yerr=err, fmt="none", ecolor=INK, elinewidth=1, capsize=4, zorder=3)
    for xi, v, h in zip(x, values, hi):
        ax.text(xi, h * 1.02 + max(hi) * 0.01, f"{v:.1f} {unit}", ha="center", va="bottom",
                fontsize=9, color=INK)
    ax.set_xticks(x, labels)
    ax.set_ylim(0, max(hi) * 1.18)


def draw_graphs(rows, runs):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rng = f"median of {runs} runs, black line = min to max" if runs > 1 else "single run"

    s = summarise(rows, "sources", "sources")
    if s:
        fig, ax = _axes("Parallel download speed by number of peers",
                        f"Each peer limited to {UPLOAD_LIMIT // MB} MB/s; grey line = perfect scaling "
                        f"(n x one peer); {rng}")
        _bars(ax, [str(r["sources"]) for r in s], [r["mb_per_s"] for r in s],
              [r["mb_lo"] for r in s], [r["mb_hi"] for r in s], "MB/s")
        base = s[0]["mb_per_s"]
        ax.plot(range(len(s)), [base * r["sources"] for r in s], color=MUTED, linewidth=1,
                zorder=1)
        ax.set_xlabel("Peers sending at the same time", color=MUTED)
        ax.set_ylabel("MB/s", color=MUTED)
        fig.savefig(OUT / "sources.png", facecolor=SURFACE)
        plt.close(fig)

    s = summarise(rows, "chunk_size", "chunk_kib")
    if s:
        fig, ax = _axes("Throughput by chunk size", f"One peer, no speed limit; {rng}")
        _bars(ax, [f"{r['chunk_kib'] // 1024} MiB" if r["chunk_kib"] >= 1024 else f"{r['chunk_kib']} KiB"
                   for r in s], [r["mb_per_s"] for r in s],
              [r["mb_lo"] for r in s], [r["mb_hi"] for r in s], "MB/s")
        ax.set_ylabel("MB/s", color=MUTED)
        fig.savefig(OUT / "chunk_size.png", facecolor=SURFACE)
        plt.close(fig)

    s = summarise(rows, "file_size", "file_mb")
    if s:
        fig, ax = _axes("Throughput by file size", f"One peer, no speed limit, 1 MiB chunks; {rng}")
        _bars(ax, [f"{r['file_mb']} MB" for r in s], [r["mb_per_s"] for r in s],
              [r["mb_lo"] for r in s], [r["mb_hi"] for r in s], "MB/s")
        ax.set_ylabel("MB/s", color=MUTED)
        fig.savefig(OUT / "file_size.png", facecolor=SURFACE)
        plt.close(fig)

    s = summarise(rows, "recovery", "case")
    if s:
        fig, ax = _axes("Download time when a peer dies halfway",
                        f"3 peers at {UPLOAD_LIMIT // MB} MB/s each; {rng}")
        _bars(ax, [r["case"] for r in s], [r["seconds"] for r in s],
              [r["s_lo"] for r in s], [r["s_hi"] for r in s], "s")
        ax.set_ylabel("Seconds (lower is better)", color=MUTED)
        fig.savefig(OUT / "recovery.png", facecolor=SURFACE)
        plt.close(fig)


def write_summary(rows, runs, args):
    lines = ["# ChunkSwarm benchmark results", "",
             f"- Machine: {platform.system()} {platform.release()}, {os.cpu_count()} CPU threads, "
             f"Python {platform.python_version()}",
             f"- Date: {time.strftime('%Y-%m-%d %H:%M')}",
             f"- Runs per configuration: {runs} (median shown, min-max in brackets)",
             "- All transfers over real TCP on 127.0.0.1, every chunk SHA-256 verified", ""]
    tables = [("Parallel peers (each limited to %d MB/s)" % (UPLOAD_LIMIT // MB), "sources", "sources", "Peers"),
              ("Chunk size (1 peer, no limit)", "chunk_size", "chunk_kib", "Chunk (KiB)"),
              ("File size (1 peer, no limit)", "file_size", "file_mb", "File (MB)"),
              ("Failover (3 limited peers)", "recovery", "case", "Case")]
    for title, exp, key, head in tables:
        s = summarise(rows, exp, key)
        if not s:
            continue
        lines += [f"## {title}", "", f"| {head} | MB/s | Seconds |", "| --- | ---: | ---: |"]
        for r in s:
            lines.append(f"| {r[key]} | {r['mb_per_s']:.1f} ({r['mb_lo']:.1f}-{r['mb_hi']:.1f}) "
                         f"| {r['seconds']:.2f} ({r['s_lo']:.2f}-{r['s_hi']:.2f}) |")
        lines.append("")
    s = summarise(rows, "sources", "sources")
    if len(s) > 1:
        lines.append(f"Speed-up with {s[-1]['sources']} peers vs 1: "
                     f"**{s[-1]['mb_per_s'] / s[0]['mb_per_s']:.2f}x**")
    (OUT / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="ChunkSwarm benchmarks")
    ap.add_argument("--quick", action="store_true", help="small files and 1 run (about 20 s)")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--only", choices=["sources", "chunk_size", "file_size", "recovery"])
    args = ap.parse_args()
    runs = 1 if args.quick else args.runs
    scale = 10 if args.quick else 1

    OUT.mkdir(exist_ok=True)
    rows: list[dict] = []
    work = Path(tempfile.mkdtemp(prefix="chunkswarm-bench-"))
    try:
        plan = [
            ("sources", lambda: exp_sources(work, rows, runs, 60 * MB // scale)),
            ("chunk_size", lambda: exp_chunk_size(work, rows, runs, 200 * MB // scale)),
            ("file_size", lambda: exp_file_size(work, rows, runs,
                                                [s * MB // scale for s in (10, 50, 100, 200)])),
            ("recovery", lambda: exp_recovery(work, rows, runs, 60 * MB // scale)),
        ]
        for name, run in plan:
            if args.only and name != args.only:
                continue
            print(f"\n== {name}")
            run()
    finally:
        shutil.rmtree(work, ignore_errors=True)

    with (OUT / "results.csv").open("w", newline="", encoding="utf-8") as f:
        fields = ["experiment", "case", "sources", "chunk_kib", "file_mb", "run", "seconds", "mb_per_s"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: (round(v, 3) if isinstance(v, float) else v) for k, v in r.items()})
    write_summary(rows, runs, args)
    try:
        draw_graphs(rows, runs)
        print(f"\nGraphs written to {OUT.resolve()}")
    except ImportError:
        print("\nmatplotlib is not installed: run  pip install matplotlib  to get the graphs")
    print((OUT / "summary.md").read_text(encoding="utf-8"))


if __name__ == "__main__":
    sys.exit(main())
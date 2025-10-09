#!/usr/bin/env python3

import matplotlib
matplotlib.use("Agg")
import argparse, os, pandas as pd, matplotlib.pyplot as plt

def plot_stage(load, mon, stage, title, outdir):
    os.makedirs(outdir, exist_ok=True)
    # Figure 1: Throughput (RPS) and Power (W)
    plt.figure()
    plt.plot(load["t"], load["rps"], label="RPS")
    plt.plot(mon["t"], mon["power_w"], label="Power (W)")
    plt.xlabel("Time (s)"); plt.ylabel("RPS / Watts"); plt.title(f"{title} — {stage} — RPS & Power"); plt.legend()
    plt.tight_layout(); plt.savefig(f"{outdir}/plot_rps_power.png"); plt.close()
    

    # Figure 2: Latency p95 (ms)
    plt.figure()
    plt.plot(load["t"], load["p95_ms"])
    plt.xlabel("Time (s)"); plt.ylabel("Latency p95 (ms)"); plt.title(f"{title} — {stage} — Latency p95")
    plt.tight_layout(); plt.savefig(f"{outdir}/plot_latency_p95.png"); plt.close()

    # Figure 3: CPU% and RSS
    plt.figure()
    plt.plot(mon["t"], mon["cpu_percent"], label="CPU % (proc)")
    plt.plot(mon["t"], mon["rss_mb"], label="RSS (MB)")
    plt.xlabel("Time (s)"); plt.ylabel("CPU% / MB"); plt.title(f"{title} — {stage} — CPU & Memory"); plt.legend()
    plt.tight_layout(); plt.savefig(f"{outdir}/plot_cpu_mem.png"); plt.close()

    # Figure 4: Cumulative Energy (J)
    plt.figure()
    plt.plot(mon["t"], mon["energy_j_total"])
    plt.xlabel("Time (s)"); plt.ylabel("Energy (J)"); plt.title(f"{title} — {stage} — Energy")
    plt.tight_layout(); plt.savefig(f"{outdir}/plot_energy.png"); plt.close()

def plot_compare(load, mon, stages, title, outdir):
    os.makedirs(outdir, exist_ok=True)
    colors = plt.cm.get_cmap('tab10', len(stages))

    # Figure 1: Throughput (RPS) and Power (W)
    plt.figure()
    for idx, stage in enumerate(stages):
        l = load[load["stage"] == stage]
        plt.plot(l["t"], l["rps"], label=f"{stage} RPS", color=colors(idx))
    plt.plot(mon["t"], mon["power_w"], label="Power (W)", color="black", linestyle="--")
    plt.xlabel("Time (s)"); plt.ylabel("RPS / Watts"); plt.title(f"{title} — Compare RPS & Power"); plt.legend()
    plt.tight_layout(); plt.savefig(f"{outdir}/plot_rps_power.png"); plt.close()

    # Figure 2: Latency p95 (ms)
    plt.figure()
    for idx, stage in enumerate(stages):
        l = load[load["stage"] == stage]
        plt.plot(l["t"], l["p95_ms"], label=f"{stage}", color=colors(idx))
    plt.xlabel("Time (s)"); plt.ylabel("Latency p95 (ms)"); plt.title(f"{title} — Compare Latency p95"); plt.legend()
    plt.tight_layout(); plt.savefig(f"{outdir}/plot_latency_p95.png"); plt.close()

    # Figure 3: CPU% and RSS
    plt.figure()
    plt.plot(mon["t"], mon["cpu_percent"], label="CPU % (proc)")
    plt.plot(mon["t"], mon["rss_mb"], label="RSS (MB)")
    plt.xlabel("Time (s)"); plt.ylabel("CPU% / MB"); plt.title(f"{title} — CPU & Memory"); plt.legend()
    plt.tight_layout(); plt.savefig(f"{outdir}/plot_cpu_mem.png"); plt.close()

    # Figure 4: Cumulative Energy (J)
    plt.figure()
    plt.plot(mon["t"], mon["energy_j_total"])
    plt.xlabel("Time (s)"); plt.ylabel("Energy (J)"); plt.title(f"{title} — Energy")
    plt.tight_layout(); plt.savefig(f"{outdir}/plot_energy.png"); plt.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--load", required=True, help="load_timeseries.csv from load_staged.py")
    ap.add_argument("--monitor", required=True, help="server_monitor.csv from monitor.py")
    ap.add_argument("--title", default="JITLab Run")
    args = ap.parse_args()

    load = pd.read_csv(args.load)
    mon = pd.read_csv(args.monitor)
    base_outdir = f"runs/"


    # Align times (optional)
    t0 = min(load["ts"].min(), mon["ts"].min())
    load["t"] = load["ts"] - t0
    mon["t"] = mon["ts"] - t0

    stages = load["stage"].unique()
    for stage in stages:
        l = load[load["stage"] == stage]
        stage_outdir = os.path.join(base_outdir, stage)
        os.makedirs(stage_outdir, exist_ok=True)
        plot_stage(l, mon, stage, args.title, stage_outdir)

    plot_compare(load, mon, stages, args.title, os.path.join(base_outdir, "compare"))

    print("Saved: per-stage plots in each stage folder, and comparison plots in compare/")

if __name__ == "__main__":
    main()
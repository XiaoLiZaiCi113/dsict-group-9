#!/usr/bin/env python3

import matplotlib
matplotlib.use("Agg")   # force non-GUI backend

import argparse, pandas as pd, matplotlib.pyplot as plt
import os

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--monitor", required=True, help="server_monitor.csv from monitor.py")
    ap.add_argument("--title", default="JITLab Monitor Only")
    ap.add_argument("--outdir", default=".", help="Output directory for plots")
    ap.add_argument("--suffix", default="", help="Suffix for output files (e.g. timestamp)")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    suf = f"_{args.suffix}" if args.suffix else ""

    mon = pd.read_csv(args.monitor)
    t0 = mon["ts"].min()
    mon["t"] = mon["ts"] - t0

    # Figure 1: Power (W)
    plt.figure()
    plt.plot(mon["t"], mon["power_w"], label="Power (W)")
    plt.xlabel("Time (s)"); plt.ylabel("Power (W)"); plt.title(args.title + " — Power"); plt.legend()
    plt.tight_layout(); plt.savefig(os.path.join(args.outdir, f"plot_power{suf}.png"))

    # Figure 2: CPU% and RSS
    plt.figure()
    plt.plot(mon["t"], mon["cpu_percent"], label="CPU % (proc)")
    plt.plot(mon["t"], mon["rss_mb"], label="RSS (MB)")
    plt.xlabel("Time (s)"); plt.ylabel("CPU% / MB"); plt.title(args.title + " — CPU & Memory"); plt.legend()
    plt.tight_layout(); plt.savefig(os.path.join(args.outdir, f"plot_cpu_mem{suf}.png"))

    # Figure 3: Cumulative Energy (J)
    plt.figure()
    plt.plot(mon["t"], mon["energy_j_total"])
    plt.xlabel("Time (s)"); plt.ylabel("Energy (J)"); plt.title(args.title + " — Energy")
    plt.tight_layout(); plt.savefig(os.path.join(args.outdir, f"plot_energy{suf}.png"))

    print(f"Saved: {os.path.join(args.outdir, f'plot_power{suf}.png')}, {os.path.join(args.outdir, f'plot_cpu_mem{suf}.png')}, {os.path.join(args.outdir, f'plot_energy{suf}.png')}")

if __name__ == "__main__":
    main()
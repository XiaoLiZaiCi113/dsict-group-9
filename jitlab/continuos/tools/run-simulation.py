#!/usr/bin/env python3
import argparse, json, math, random, string, time, os
import asyncio
import httpx
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple
from matplotlib.collections import LineCollection

import requests
import matplotlib.pyplot as plt
import pandas as pd


def parse_hhmm(s: str) -> timedelta:
    # "24:00" is treated as next-day midnight
    hh, mm = map(int, s.split(":"))
    if hh == 24:  # normalize to 00:00 next day
        hh = 0
        return timedelta(days=1, hours=hh, minutes=mm)
    return timedelta(hours=hh, minutes=mm)

def phase_durations(phases: List[Dict]) -> List[Tuple[Dict, float]]:
    """Return (phase, minutes) using true daily clock, handling wrap over midnight."""
    # Anchor day at t0=00:00
    t0 = timedelta()
    out = []
    # Convert each start/end to absolute minutes since 00:00 (may wrap day)
    spans = []
    for p in phases:
        start = parse_hhmm(p["start"])
        end = parse_hhmm(p["end"])
        if end <= start:
            end = end + timedelta(days=1)  # wrap
        spans.append((p, start, end))
    # Sort by start time
    spans.sort(key=lambda x: x[1])
    for p, st, en in spans:
        minutes = (en - st).total_seconds() / 60.0
        out.append((p, minutes))
    return out

def build_schedule(phases: List[Dict], total_sim_minutes: float) -> List[Dict]:
    """
    Allocate compressed simulation minutes to each phase proportionally to its real duration.
    Adds fields: sim_start_minute, sim_end_minute, sim_duration_minute.
    """
    pdurs = phase_durations(phases)
    total_real = sum(m for _, m in pdurs)
    alloc = []
    acc = 0.0
    for p, minutes in pdurs:
        share = minutes / total_real if total_real > 0 else 0
        sim_minutes = total_sim_minutes * share
        seg = dict(p)  # copy
        seg["sim_duration_minute"] = sim_minutes
        seg["sim_start_minute"] = acc
        seg["sim_end_minute"] = acc + sim_minutes
        acc += sim_minutes
        alloc.append(seg)
    return alloc

def choose_endpoint(mix_cpu: float) -> str:
    return "cpu" if random.random() < mix_cpu else "files"

def rand_payload(n: int) -> str:
    if n <= 0: return ""
    # Keep it simple and deterministic enough
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choices(alphabet, k=n))

def call_cpu(base_url: str, cpuTask: Dict) -> Tuple[bool, float, int]:
    """
    POST /work/cpu  with JSON: { iterations, payloadSize }
    Returns: ok, latency_ms, 'bytes' (we record the request size notionally as payloadSize).
    """
    body = {
        "iterations": cpuTask.get("iterations", 1000),
        "payloadSize": cpuTask.get("payloadBytes", cpuTask.get("payloadSize", 10000))  # allow old key
    }
    t0 = time.perf_counter()
    try:
        r = requests.post(f"{base_url}/work/cpu", json=body, timeout=60)
        ok = (200 <= r.status_code < 300)
    except Exception:
        ok = False
    dt = (time.perf_counter() - t0) * 1000.0

    # For CPU we don't actually send/receive big data; record payloadSize as a proxy "bytes touched"
    return ok, dt, int(body["payloadSize"])


def call_files(base_url: str, fileTask: Dict) -> Tuple[bool, float, int]:
    """
    POST /work/files with JSON: { fileCount, fileSizeBytes, prefix }
    Streams ZIP response; we discard content but measure bytes via headers or streamed sum.
    Returns: ok, latency_ms, bytes_out (approx uncompressed creation or streamed).
    """
    if not fileTask.get("enabled", False):
        return True, 0.0, 0

    body = {
        "fileCount": fileTask.get("files", fileTask.get("fileCount", 5)),
        "fileSizeBytes": fileTask.get("fileSizeBytes", 131072),
        "prefix": fileTask.get("prefix", "blob")
    }

    t0 = time.perf_counter()
    bytes_streamed = 0
    ok = False
    try:
        with requests.post(f"{base_url}/work/files", json=body, stream=True, timeout=180) as r:
            ok = (200 <= r.status_code < 300)
            # Prefer headers if present
            hdr_cnt = r.headers.get("X-Files-Count")
            hdr_sz  = r.headers.get("X-File-Size-Bytes")
            if hdr_cnt is not None and hdr_sz is not None:
                try:
                    bytes_streamed = int(hdr_cnt) * int(hdr_sz)
                except ValueError:
                    bytes_streamed = 0  # fallback to streaming count below if needed

            # Consume stream but discard (avoid large memory usage)
            for chunk in r.iter_content(chunk_size=1<<20):
                if chunk:
                    # If headers were missing or invalid, count actual streamed bytes
                    if hdr_cnt is None or hdr_sz is None:
                        bytes_streamed += len(chunk)
    except Exception:
        ok = False

    dt = (time.perf_counter() - t0) * 1000.0
    return ok, dt, bytes_streamed


async def async_call_cpu(client, base_url, cpuTask):
    body = {
        "iterations": cpuTask.get("iterations", 1000),
        "payloadSize": cpuTask.get("payloadBytes", cpuTask.get("payloadSize", 10000))
    }
    t0 = time.perf_counter()
    try:
        r = await client.post(f"{base_url}/work/cpu", json=body, timeout=60)
        await r.aread()
        ok = (200 <= r.status_code < 300)
    except Exception:
        ok = False
    dt = (time.perf_counter() - t0) * 1000.0
    return ok, dt, int(body["payloadSize"])

async def async_call_files(client, base_url, fileTask):
    if not fileTask.get("enabled", False):
        return True, 0.0, 0
    body = {
        "fileCount": fileTask.get("files", fileTask.get("fileCount", 5)),
        "fileSizeBytes": fileTask.get("fileSizeBytes", 131072),
        "prefix": fileTask.get("prefix", "blob")
    }
    t0 = time.perf_counter()
    bytes_streamed = 0
    ok = False
    try:
        async with client.stream("POST", f"{base_url}/work/files", json=body, timeout=180) as r:
            ok = (200 <= r.status_code < 300)
            hdr_cnt = r.headers.get("X-Files-Count")
            hdr_sz  = r.headers.get("X-File-Size-Bytes")
            if hdr_cnt is not None and hdr_sz is not None:
                try:
                    bytes_streamed = int(hdr_cnt) * int(hdr_sz)
                except ValueError:
                    bytes_streamed = 0
            async for chunk in r.aiter_bytes():
                if chunk:
                    if hdr_cnt is None or hdr_sz is None:
                        bytes_streamed += len(chunk)
    except Exception:
        ok = False
    dt = (time.perf_counter() - t0) * 1000.0
    return ok, dt, bytes_streamed

def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * max(0.0, min(1.0, t))

# -------------------------
# Simulation
# -------------------------

def run_sim(phases: List[Dict], base_url: str, total_sim_minutes: float, seed: int) -> pd.DataFrame:
    random.seed(seed)

    sched = build_schedule(phases, total_sim_minutes)
    # One-second tick
    total_seconds = int(round(total_sim_minutes * 60))
    start_wall = datetime.now()

    rows = []
    last_phase = None
    for s in range(total_seconds):
        # What minute are we in?
        sim_minute = s / 60.0
        # Find current phase by sim minute
        cur = None
        for p in sched:
            if p["sim_start_minute"] <= sim_minute < p["sim_end_minute"] or \
               (p["sim_end_minute"] == sched[-1]["sim_end_minute"] and sim_minute >= p["sim_start_minute"]):
                cur = p; break
        if cur is None:
            time.sleep(1.0)
            continue

        # Within-phase progress [0..1]
        span = max(cur["sim_duration_minute"], 1e-9)
        t_phase = (sim_minute - cur["sim_start_minute"]) / span
        # Target RPS (linear ramp min->max across phase)
        rps_min = cur["targetRPS"]["min"]
        rps_max = cur["targetRPS"]["max"]
        target_rps = lerp(rps_min, rps_max, t_phase)

        # Choose how many requests to send this second
        req_this_second = max(0, int(round(target_rps + random.random() - 0.5)))

        mix_cpu = cur["mix"]["cpu"]
        mix_file = cur["mix"]["file"]
        cpuTask = cur.get("cpuTask", {})
        fileTask = cur.get("fileTask", {})

        # Fire requests sequentially, I guess.
        succ = 0
        err = 0
        latencies = []
        sent_cpu = 0
        sent_file = 0
        bytes_out = 0

        for _ in range(req_this_second):
            ep = choose_endpoint(mix_cpu)
            if ep == "cpu":
                ok, ms, payload_bytes = call_cpu(base_url, cpuTask)
                sent_cpu += 1
                bytes_out += payload_bytes
            else:
                ok, ms, out_bytes = call_files(base_url, fileTask)
                sent_file += 1
                bytes_out += out_bytes
            latencies.append(ms)
            succ += 1 if ok else 0
            err  += 0 if ok else 1

        # Record result once per second
        rows.append({
            "sim_second": s,
            "sim_minute": sim_minute,
            "phase": cur["phase"],
            "phase_progress": t_phase,
            "target_rps": target_rps,
            "sent_total": req_this_second,
            "sent_cpu": sent_cpu,
            "sent_file": sent_file,
            "succ": succ,
            "err": err,
            "avg_latency_ms": (sum(latencies)/len(latencies)) if latencies else 0.0,
            "bytes": bytes_out,
            "wall_time": start_wall + timedelta(seconds=s)
        })

        # Keep pacing to 1 second per loop
        t_end = time.perf_counter() + 1.0
        while True:
            now = time.perf_counter()
            if now >= t_end: break
            time.sleep(min(0.005, t_end - now))

    df = pd.DataFrame(rows)
    return df

async def run_sim_async(phases: List[Dict], base_url: str, total_sim_minutes: float, seed: int, default_concurrency: int) -> pd.DataFrame:
    random.seed(seed)
    sched = build_schedule(phases, total_sim_minutes)
    total_seconds = int(round(total_sim_minutes * 60))
    start_wall = datetime.now()
    rows = []

    async with httpx.AsyncClient(http2=False) as client:
        for s in range(total_seconds):
            sim_minute = s / 60.0
            cur = None
            for p in sched:
                if p["sim_start_minute"] <= sim_minute < p["sim_end_minute"] or \
                   (p["sim_end_minute"] == sched[-1]["sim_end_minute"] and sim_minute >= p["sim_start_minute"]):
                    cur = p; break
            if cur is None:
                await asyncio.sleep(1.0)
                continue

            span = max(cur["sim_duration_minute"], 1e-9)
            t_phase = (sim_minute - cur["sim_start_minute"]) / span
            phase_conc = cur.get("concurrency", default_concurrency)
            rps_min = cur["targetRPS"]["min"]
            rps_max = cur["targetRPS"]["max"]
            r_u = lerp(rps_min, rps_max, t_phase)
            target_rps = phase_conc * r_u
            req_this_second = max(0, int(round(target_rps + random.random() - 0.5)))

            mix_cpu = cur["mix"]["cpu"]
            cpuTask = cur.get("cpuTask", {})
            fileTask = cur.get("fileTask", {})

            # Prepare endpoints for this second
            endpoints = ["cpu" if random.random() < mix_cpu else "files" for _ in range(req_this_second)]

            # --- Async worker pattern ---
            sem = asyncio.Semaphore(phase_conc)
            async def one(ep):
                async with sem:
                    if ep == "cpu":
                        return await async_call_cpu(client, base_url, cpuTask) + ("cpu",)
                    else:
                        return await async_call_files(client, base_url, fileTask) + ("file",)

            tasks = [one(ep) for ep in endpoints]
            t0 = time.perf_counter()
            results = await asyncio.gather(*tasks) if tasks else []
            dt = time.perf_counter() - t0

            succ = 0
            err = 0
            latencies = []
            sent_cpu = 0
            sent_file = 0
            bytes_out = 0

            for ok, ms, bytes_val, ep in results:
                latencies.append(ms)
                if ep == "cpu":
                    sent_cpu += 1
                    bytes_out += bytes_val
                else:
                    sent_file += 1
                    bytes_out += bytes_val
                succ += 1 if ok else 0
                err  += 0 if ok else 1

            rows.append({
                "sim_second": s,
                "sim_minute": sim_minute,
                "phase": cur["phase"],
                "phase_progress": t_phase,
                "target_rps": target_rps,
                "sent_total": req_this_second,
                "sent_cpu": sent_cpu,
                "sent_file": sent_file,
                "succ": succ,
                "err": err,
                "avg_latency_ms": (sum(latencies)/len(latencies)) if latencies else 0.0,
                "bytes": bytes_out,
                "wall_time": start_wall + timedelta(seconds=s)
            })

            # Keep pacing to 1 second per loop
            t_end = time.perf_counter() + 1.0
            while True:
                now = time.perf_counter()
                if now >= t_end: break
                await asyncio.sleep(min(0.005, t_end - now))

    df = pd.DataFrame(rows)
    return df

# -------------------------
# Plotting
# -------------------------

def phase_colors(phases: List[Dict]):
    # Assign fixed colors by phase name (user asked for different colors per time section)
    return {
        "Morning":  "#1f77b4",
        "Lunch":    "#ff7f0e",
        "Afternoon":"#2ca02c",
        "Dinner":   "#d62728",
        "Late":     "#9467bd",
        "Night":    "#8c564b",
    }

# I am not a plot pro, so I ask my best friend to help me out.
def plot_metric(df: pd.DataFrame, metric: str, out_png: Path, smooth="ema", win=15, ema_alpha=0.2):
    """
    Draw one continuous line with per-segment colors by phase.
    smooth: None | 'rolling' | 'ema'
    win:    rolling window in seconds
    ema_alpha: smoothing factor for EMA (0<alpha<=1)
    """
    # sort and select
    g = df.sort_values("sim_minute").reset_index(drop=True)
    x = g["sim_minute"].to_numpy()
    y = g[metric].to_numpy()
    phases = g["phase"].to_numpy()

    # smoothing
    ys = y.copy()
    if smooth == "rolling" and win > 1:
        ys = pd.Series(y).rolling(win, min_periods=1, center=True).mean().to_numpy()
    elif smooth == "ema" and 0 < ema_alpha <= 1:
        ys = pd.Series(y).ewm(alpha=ema_alpha, adjust=False).mean().to_numpy()

    # build colored segments (continuous)
    segs = []
    seg_colors = []
    colors = phase_colors(df["phase"].unique())  # you already have this
    for i in range(len(x) - 1):
        segs.append([(x[i], ys[i]), (x[i+1], ys[i+1])])
        # color by the phase of the segment's start
        seg_colors.append(colors.get(phases[i], "#333333"))

    fig, ax = plt.subplots(figsize=(11, 4))
    lc = LineCollection(segs, colors=seg_colors, linewidths=1.8)
    ax.add_collection(lc)
    ax.set_xlim(float(x.min()), float(x.max()))
    ymin = float(pd.Series(ys).min())
    ymax = float(pd.Series(ys).max())
    pad = (ymax - ymin) * 0.05 if ymax > ymin else 1.0
    ax.set_ylim(ymin - pad, ymax + pad)

    # light phase bands + labeled ticks at phase centers
    # contiguous runs of same phase
    run_id = (g["phase"] != g["phase"].shift()).cumsum()
    for _, run in g.groupby(run_id):
        ph = run["phase"].iloc[0]
        x0 = float(run["sim_minute"].min())
        x1 = float(run["sim_minute"].max())
        ax.axvspan(x0, x1, color=colors.get(ph, "#000000"), alpha=0.08, lw=0)

    centers = g.groupby("phase")["sim_minute"].agg(["min", "max"])
    centers["center"] = (centers["min"] + centers["max"]) / 2.0
    ax.set_xticks(centers["center"].tolist())
    ax.set_xticklabels(centers.index.tolist(), rotation=0)

    ax.set_xlabel("Simulated day (compressed) — phases")
    ax.set_ylabel(metric.replace("_", " "))
    ax.set_title(metric.replace("_", " "))
    # create legend proxies for phases
    handles = [plt.Line2D([0], [0], color=colors[p], lw=2, label=p) for p in centers.index.tolist()]
    ax.legend(handles=handles, ncols=3, fontsize=8)
    fig.tight_layout()
    fig.savefig(out_png, dpi=160)
    plt.close(fig)


def make_plots(df: pd.DataFrame, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    # Some useful metrics that works on my windows
    metrics = [
        "sent_total",      # achieved RPS per second
        "avg_latency_ms",
        "succ",
        "err",
        "sent_cpu",
        "sent_file",
        "bytes"
    ]

    # Write CSV for analysis
    csv_path = out_dir / "simulation_timeseries.csv"
    df.to_csv(csv_path, index=False)

    # Produce PNGs
    for m in metrics:
        plot_metric(df, m, out_dir / f"{m}.png")

    print(f"[OK] Saved CSV → {csv_path}")
    print(f"[OK] Saved plots → {out_dir}")

# -------------------------
# Main
# -------------------------

def main():
    # fix this if you open the folder elsewhere
    path = os.getcwd()
    config_path = os.path.join(path, 'config/continous.json')

    ap = argparse.ArgumentParser(description="Run time-compressed day simulation against Spring endpoints.")
    ap.add_argument("--config", default=config_path, help="Path to day-design JSON (array of phases).")
    ap.add_argument("--base-url", default="http://localhost:8080", help="Server base URL (no trailing slash)")
    ap.add_argument("--total-sim-minutes", type=float, default=2.4,
                    help="Compressed duration for the whole day (e.g., 24 = 1 min per real hour).")
    ap.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility.")
    ap.add_argument("--out", default="sim_out", help="Output directory for CSV and PNGs.")
    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        phases = json.load(f)

    # Basic validation
    required = ["phase","start","end","targetRPS","mix"]
    for p in phases:
        for k in required:
            if k not in p:
                raise ValueError(f"Phase missing key: {k} → {p}")

    df = asyncio.run(run_sim_async(phases, args.base_url.rstrip("/"), args.total_sim_minutes, args.seed, 1))
    make_plots(df, Path(args.out))

if __name__ == "__main__":
    main()

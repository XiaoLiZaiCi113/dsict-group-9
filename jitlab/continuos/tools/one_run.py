#!/usr/bin/env python3
import argparse, subprocess, os, sys, shlex, time
from datetime import datetime

def find_pid(cmd_substr):
    try:
        out = subprocess.check_output(["pgrep","-fa",cmd_substr], text=True)
        return int(out.strip().splitlines()[0].split()[0])
    except Exception:
        return None

def run():
    ap = argparse.ArgumentParser(description="Run monitor + simulation + plots")
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--config", default="config/continous.json")
    ap.add_argument("--sim-minutes", type=float, default=2.4)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--server-cmd-substr", default="jitlab-0.0.1-SNAPSHOT.jar")
    ap.add_argument("--outdir", default="sim_runs")
    args = ap.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(args.outdir, exist_ok=True)

    pid = find_pid(args.server_cmd_substr)
    if not pid:
        print(f"[one_run] Could not find server PID by '{args.server_cmd_substr}'. Start the server first.", file=sys.stderr)
        sys.exit(1)
    print(f"[one_run] Using server PID: {pid}")

    sim_out = os.path.join(args.outdir, f"sim_{ts}")
    os.makedirs(sim_out, exist_ok=True)
    mon_csv = os.path.join(sim_out, f"monitor_{ts}.csv")

    # Estimate duration (add 10% margin)
    duration = args.sim_minutes * 60 * 1.1

    mon_cmd = ["sudo", "python3", "tools/monitor.py",
               "--pid", str(pid),
               "--interval", "1",
               "--duration", str(duration),
               "--out", mon_csv]
    print("[one_run] START monitor:", " ".join(shlex.quote(x) for x in mon_cmd))
    mon_p = subprocess.Popen(mon_cmd, stdout=sys.stdout, stderr=sys.stderr)

    sim_cmd = ["python3", "tools/run-simulation.py",
               "--base-url", args.base_url,
               "--config", args.config,
               "--total-sim-minutes", str(args.sim_minutes),
               "--seed", str(args.seed),
               "--out", sim_out]
    print("[one_run] START simulation:", " ".join(shlex.quote(x) for x in sim_cmd))
    ret = subprocess.run(sim_cmd).returncode
    print(f"[one_run] simulation exited with code {ret}")

    # Wait for monitor to finish
    print("[one_run] WAIT monitor to finish…")
    deadline = time.time() + duration + 15
    while mon_p.poll() is None and time.time() < deadline:
        time.sleep(0.2)
    if mon_p.poll() is None:
        print("[one_run] monitor did not exit in time; terminating…", file=sys.stderr)
        mon_p.terminate()
        try:
            mon_p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            print("[one_run] monitor still alive; killing…", file=sys.stderr)
            mon_p.kill()

    # Plot monitor results (outputs timestamped files in sim_out)
    env = os.environ.copy()
    env["MPLBACKEND"] = "Agg"
    plot_cmd = [
        "python3", "tools/plot_monitor.py",
        "--monitor", mon_csv,
        "--title", f"JITLab Monitor {ts}",
        "--outdir", sim_out,
        "--suffix", ts
    ]
    print("[one_run] START plot_monitor:", " ".join(shlex.quote(x) for x in plot_cmd))
    try:
        subprocess.check_call(plot_cmd, env=env)
    except subprocess.CalledProcessError as e:
        print(f"[one_run] plot_monitor failed (exit {e.returncode}).", file=sys.stderr)

    print("\n✅ Done.")
    print(f"Monitor CSV: {mon_csv}")
    print(f"Simulation output: {sim_out}/simulation_timeseries.csv and PNGs")
    print(f"Monitor plots: {sim_out}/plot_power_{ts}.png, plot_cpu_mem_{ts}.png, plot_energy_{ts}.png")

if __name__ == "__main__":
    run()
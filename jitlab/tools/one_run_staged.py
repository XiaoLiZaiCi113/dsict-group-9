#!/usr/bin/env python3
import argparse, subprocess, os, shlex, sys, time
from datetime import datetime

def find_pid(cmd_substr):
    try:
        out = subprocess.check_output(["pgrep","-fa",cmd_substr], text=True)
        return int(out.strip().splitlines()[0].split()[0])
    except Exception:
        return None

def run():
    ap = argparse.ArgumentParser(description="Run monitor + staged loader + staged plots")
    ap.add_argument("--baseUrl", required=True)
    ap.add_argument("--stageFile", required=True)
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--title", default="JITLab Staged Run")
    ap.add_argument("--server-cmd-substr", default="jitlab-0.0.1-SNAPSHOT.jar")
    ap.add_argument("--outdir", default="runs")
    args = ap.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(args.outdir, exist_ok=True)

    pid = find_pid(args.server_cmd_substr)
    if not pid:
        print(f"[one_run_staged] Could not find server PID by '{args.server_cmd_substr}'. Start the server first.", file=sys.stderr)
        sys.exit(1)
    print(f"[one_run_staged] Using server PID: {pid}")

    load_csv = os.path.join(args.outdir, f"load_staged_{ts}.csv")
    mon_csv  = os.path.join(args.outdir, f"monitor_{ts}.csv")

    # Estimate duration from stageFile
    import json
    with open(args.stageFile, "r") as f:
        stages = json.load(f)
    duration = sum(float(s.get("duration_sec", 10)) for s in stages) + 5

    mon_cmd = ["sudo","python3","tools/monitor.py","--pid",str(pid),
               "--interval","1","--duration",str(duration),
               "--out",mon_csv]
    print("[one_run_staged] START monitor:", " ".join(shlex.quote(x) for x in mon_cmd))
    mon_p = subprocess.Popen(mon_cmd, stdout=sys.stdout, stderr=sys.stderr)

    load_cmd = ["python3","tools/load_staged.py",
                "--baseUrl",args.baseUrl,
                "--stageFile",args.stageFile,
                "--timeout",str(args.timeout),
                "--out",load_csv]
    print("[one_run_staged] START staged loader:", " ".join(shlex.quote(x) for x in load_cmd))
    ret = subprocess.run(load_cmd).returncode
    print(f"[one_run_staged] staged loader exited with code {ret}")

    # Poll the monitor instead of wait(); enforce a hard deadline
    print("[one_run_staged] WAIT monitor to finish…")
    deadline = time.time() + duration + 15
    while mon_p.poll() is None and time.time() < deadline:
        time.sleep(0.2)
    if mon_p.poll() is None:
        print("[one_run_staged] monitor did not exit in time; terminating…", file=sys.stderr)
        mon_p.terminate()
        try:
            mon_p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            print("[one_run_staged] monitor still alive; killing…", file=sys.stderr)
            mon_p.kill()

    # Plot (force headless backend)
    env = os.environ.copy()
    env["MPLBACKEND"] = "Agg"
    plot_cmd = ["python3","tools/plot_staged.py","--load",load_csv,"--monitor",mon_csv,"--title",args.title]
    print("[one_run_staged] START plot_staged:", " ".join(shlex.quote(x) for x in plot_cmd))
    try:
        subprocess.check_call(plot_cmd, env=env)
    except subprocess.CalledProcessError as e:
        print(f"[one_run_staged] plotting failed (exit {e.returncode}). "
              f"Check CSV paths:\n  load: {load_csv}\n  monitor: {mon_csv}", file=sys.stderr)
        sys.exit(e.returncode)

    print("\n✅ Done.")
    print(f"CSV (load):     {load_csv}")
    print(f"CSV (monitor):  {mon_csv}")
    print("PNGs:           See per-stage folders and compare/")

if __name__ == "__main__":
    run()
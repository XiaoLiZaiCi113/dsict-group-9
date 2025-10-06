
#!/usr/bin/env python3
import argparse, asyncio, time, json, csv, random, math
from typing import Dict, List, Tuple
import httpx

# ------------------------ Utilities ------------------------

def pick_weighted(weights: Dict[str, float]) -> str:
    """Return a key according to its weight (weights need not sum to 1)."""
    total = sum(max(0.0, w) for w in weights.values())
    if total <= 0:
        # default: uniform among keys
        keys = list(weights.keys())
        return random.choice(keys) if keys else ""
    r = random.random() * total
    cum = 0.0
    for k, w in weights.items():
        cum += max(0.0, w)
        if r <= cum:
            return k
    # fallback
    return next(iter(weights.keys()))

def join_url(base: str, path: str) -> str:
    if not base.endswith("/") and not path.startswith("/"):
        return base + "/" + path
    if base.endswith("/") and path.startswith("/"):
        return base[:-1] + path
    return base + path

def now_sec() -> int:
    return int(time.time())

# ------------------------ Request task ------------------------

async def one_request(client: httpx.AsyncClient, method: str, url: str, body, timeout: float, out_q: asyncio.Queue, stage_label: str):
    t0 = time.perf_counter()
    code = -1
    try:
        if method.upper() == "GET":
            r = await client.get(url, timeout=timeout, params=body if isinstance(body, dict) else None)
        else:
            r = await client.post(url, json=body, timeout=timeout)
        await r.aread()  # include transfer time
        code = r.status_code
    except Exception:
        code = -1
    dt_ms = (time.perf_counter() - t0) * 1000.0
    await out_q.put((now_sec(), dt_ms, code, stage_label))

# ------------------------ Aggregator ------------------------

async def aggregator_task(out_q: asyncio.Queue, stop_evt: asyncio.Event, csv_path: str):
    """
    Aggregates samples into per-second buckets, keyed by (ts, stage_label).
    Flushes rows whose second has passed. Writes CSV header on open.
    """
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts","stage","rps","avg_ms","p50_ms","p95_ms","ok","err"])
        buckets = {}  # (ts, stage) -> dict: {"lats":[], "ok":int, "err":int}
        while True:
            # Drain queue with a short timeout to batch efficiently
            try:
                ts, dt_ms, code, stage = await asyncio.wait_for(out_q.get(), timeout=0.2)
                key = (ts, stage)
                b = buckets.get(key)
                if b is None:
                    b = {"lats":[], "ok":0, "err":0}
                    buckets[key] = b
                b["lats"].append(dt_ms)
                if 200 <= code < 300:
                    b["ok"] += 1
                else:
                    b["err"] += 1
                out_q.task_done()
            except asyncio.TimeoutError:
                pass

            # Flush any bucket strictly older than the current wall second
            cur = now_sec()
            to_flush = [(ts, stg) for (ts, stg) in buckets.keys() if ts < cur]
            for key in sorted(to_flush):
                b = buckets.pop(key)
                lats = b["lats"]
                lats.sort()
                n = len(lats)
                if n > 0:
                    p50 = lats[min(n-1, int(round(0.50*(n-1))))]
                    p95 = lats[min(n-1, int(round(0.95*(n-1))))]
                    avg = sum(lats)/n
                    w.writerow([key[0], key[1], n, f"{avg:.3f}", f"{p50:.3f}", f"{p95:.3f}", b["ok"], b["err"]])
                else:
                    w.writerow([key[0], key[1], 0, "", "", "", 0, 0])
                f.flush()

            if stop_evt.is_set() and out_q.empty():
                # final flush any remaining buckets
                for key in sorted(buckets.keys()):
                    b = buckets[key]
                    lats = b["lats"]
                    lats.sort()
                    n = len(lats)
                    if n > 0:
                        p50 = lats[min(n-1, int(round(0.50*(n-1))))]
                        p95 = lats[min(n-1, int(round(0.95*(n-1))))]
                        avg = sum(lats)/n
                        w.writerow([key[0], key[1], n, f"{avg:.3f}", f"{p50:.3f}", f"{p95:.3f}", b["ok"], b["err"]])
                    else:
                        w.writerow([key[0], key[1], 0, "", "", "", 0, 0])
                    f.flush()
                return

# ------------------------ Open-loop stage runner ------------------------

async def run_stage_openloop(client: httpx.AsyncClient, base_url: str, stage: dict, timeout: float, out_q: asyncio.Queue):
    label = stage.get("label", "stage")
    duration = float(stage.get("duration_sec", 10.0))
    rate = float(stage.get("rate", 0.0))  # target RPS
    if rate < 0: rate = 0.0
    mix = stage.get("mix", {})  # path -> weight
    if not mix:
        return  # nothing to do
    payloads = stage.get("payload", {})  # path -> body spec
    concurrency_cap = stage.get("concurrency_cap", None)
    if concurrency_cap is None:
        # heuristic cap ~ 20% of rate (tunable). Ensure >=1 when rate>0.
        concurrency_cap = max(1, int(math.ceil(rate * 0.2))) if rate > 0 else 1
    sem = asyncio.Semaphore(concurrency_cap)

    # Helper to materialize body from payload spec
    def build_body_for(path: str):
        spec = payloads.get(path)
        if spec is None:
            return {}
        # If list -> pick random element
        if isinstance(spec, list):
            return random.choice(spec) if spec else {}
        # If dict with "id_pool" -> pick random id and replace
        if isinstance(spec, dict) and "id_pool" in spec:
            pool = spec.get("id_pool") or []
            picked = random.choice(pool) if pool else None
            # shallow copy and drop id_pool
            b = {k:v for k,v in spec.items() if k != "id_pool"}
            if picked is not None:
                b["id"] = picked
            return b
        # Otherwise return as-is
        return spec

    async def pump():
        if rate <= 0:
            await asyncio.sleep(duration)
            return
        interval = 1.0 / rate
        loop = asyncio.get_running_loop()
        next_t = loop.time()
        end_t = next_t + duration
        tasks = []

        while True:
            now = loop.time()
            if now >= end_t:
                break
            # schedule next send at or after next_t
            if now < next_t:
                await asyncio.sleep(next_t - now)
            else:
                # we're behind; don't sleep
                pass

            path = pick_weighted(mix)
            if not path:
                next_t += interval
                continue
            url = join_url(base_url, path)
            body = build_body_for(path)

            await sem.acquire()
            async def _send_once(method, url, body):
                try:
                    await one_request(client, method, url, body, timeout, out_q, label)
                finally:
                    sem.release()

            # Default method: POST except for common read-only paths
            method = "GET" if path.endswith("/feed") or path.endswith("/menu") or path.endswith("/status") else "POST"
            task = asyncio.create_task(_send_once(method, url, body))
            tasks.append(task)

            next_t += interval

        # Wait for in-flight requests to finish
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    await pump()

# ------------------------ Closed-loop fallback (original behavior) ------------------------

async def closed_loop(client: httpx.AsyncClient, url: str, body, timeout: float, concurrency: int, warmup_sec: int, run_sec: int, out_path: str):
    out_q = asyncio.Queue()
    stop_evt = asyncio.Event()
    agg_task = asyncio.create_task(aggregator_task(out_q, stop_evt, out_path))

    async def worker():
        while not stop_evt.is_set():
            await one_request(client, "POST", url, body, timeout, out_q, "default")

    tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]

    await asyncio.sleep(warmup_sec)
    await asyncio.sleep(run_sec)

    stop_evt.set()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await out_q.join()
    await agg_task

# ------------------------ Main ------------------------

async def main():
    ap = argparse.ArgumentParser(description="Stage-based open-loop HTTP load generator (food-ordering day model friendly).")
    # Back-compat args
    ap.add_argument("--url", help="(closed-loop mode) Single URL to POST to.")
    ap.add_argument("--body", default="{}", help="JSON string body for closed-loop mode.")
    ap.add_argument("--concurrency", type=int, default=8, help="Closed-loop worker count.")
    ap.add_argument("--warmupSec", type=int, default=10)
    ap.add_argument("--runSec", type=int, default=120)
    ap.add_argument("--out", default="load_timeseries.csv", help="CSV timeseries output path.")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--http2", action="store_true", help="Enable HTTP/2 if supported by server.")

    # Stage/open-loop args
    ap.add_argument("--baseUrl", help="Base URL for stage mode, e.g., http://localhost:8080")
    ap.add_argument("--stageFile", help="JSON file describing stages (array). If set, uses open-loop stage mode.")
    args = ap.parse_args()

    # If stageFile provided -> open-loop stage mode
    if args.stageFile:
        with open(args.stageFile, "r") as f:
            stages = json.load(f)
            if not isinstance(stages, list):
                raise SystemExit("stageFile must contain a JSON array of stage objects.")
        if not args.baseUrl:
            raise SystemExit("--baseUrl is required when using --stageFile")

        out_q = asyncio.Queue()
        stop_evt = asyncio.Event()
        agg_task = asyncio.create_task(aggregator_task(out_q, stop_evt, args.out))

        async with httpx.AsyncClient(http2=args.http2) as client:
            for stg in stages:
                label = stg.get("label", "stage")
                duration = float(stg.get("duration_sec", 10.0))
                print(f"--> Running stage '{label}' for {duration:.2f}s at rate={stg.get('rate', 0)} rps")
                await run_stage_openloop(client, args.baseUrl, stg, args.timeout, out_q)
                # small grace to allow late completions to land under correct ts
                await asyncio.sleep(0.05)

        stop_evt.set()
        await out_q.join()
        await agg_task
        return

    # Else closed-loop legacy mode
    if not args.url:
        raise SystemExit("Either --stageFile or --url must be provided.")
    body = json.loads(args.body)
    async with httpx.AsyncClient(http2=args.http2) as client:
        await closed_loop(client, args.url, body, args.timeout, args.concurrency, args.warmupSec, args.runSec, args.out)

if __name__ == "__main__":
    asyncio.run(main())

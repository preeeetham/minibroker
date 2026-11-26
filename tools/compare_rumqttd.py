#!/usr/bin/env python3
"""Run identical loadgen scenarios against minibroker and rumqttd.

Brokers are started fresh for every run and alternated (A, B, A, B...) so
slow drift in machine state hits both equally. Broker CPU and RSS are sampled
with `ps` during each run.

    tools/.venv/bin/python tools/compare_rumqttd.py
"""

import json
import os
import statistics
import subprocess
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
OUT = os.path.join(ROOT, "bench", "results")

BROKERS = {
    "minibroker": {
        "cmd": [os.path.join(ROOT, "target/release/minibroker"), "--stats-secs", "0", "--queue", "8192"],
        "protocol": "minibroker",
    },
    "rumqttd": {
        "cmd": [os.path.expanduser("~/.cargo/bin/rumqttd"), "-q", "-c", os.path.join(ROOT, "bench/rumqttd.toml")],
        "protocol": "mqtt",
    },
}

SCENARIOS = [
    ("1→1 @ 10k/s", dict(publishers=1, subscribers=1, rate=10000)),
    ("1→1 @ 50k/s", dict(publishers=1, subscribers=1, rate=50000)),
    ("4→4 @ 5k/s each", dict(publishers=4, subscribers=4, rate=5000)),
    ("1→20 @ 2k/s", dict(publishers=1, subscribers=20, rate=2000)),
    ("8→1 @ 10k/s each", dict(publishers=8, subscribers=1, rate=10000)),
]
REPEATS = 3
DURATION = 8


def sample(pid, stop, out):
    while not stop.is_set():
        try:
            cpu, rss = subprocess.check_output(["ps", "-o", "%cpu=,rss=", "-p", str(pid)]).split()
            out.append((float(cpu), int(rss) / 1024))
        except subprocess.CalledProcessError:
            return
        time.sleep(0.5)


def run_once(broker, scenario):
    b = BROKERS[broker]
    proc = subprocess.Popen(b["cmd"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.0)
    samples, stop = [], threading.Event()
    t = threading.Thread(target=sample, args=(proc.pid, stop, samples))
    t.start()
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "r")
        args = [PY, "-I", os.path.join(ROOT, "tools/loadgen.py"), "--protocol", b["protocol"],
                "--duration", str(DURATION), "--size", "64", "--out", out]
        for k, v in scenario.items():
            args += [f"--{k}", str(v)]
        subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        stop.set()
        t.join()
        proc.terminate()
        proc.wait()
        with open(out + ".json") as f:
            res = json.load(f)
    busy = [c for c, _ in samples[2:-1]] or [0]
    res["broker_cpu_pct"] = statistics.mean(busy)
    res["broker_rss_mb"] = max(r for _, r in samples) if samples else float("nan")
    return res


def main():
    results = {}
    for name, sc in SCENARIOS:
        for rep in range(REPEATS):
            for broker in BROKERS:
                r = run_once(broker, sc)
                results.setdefault(name, {}).setdefault(broker, []).append(r)
                l = r["latency_us"]
                print(f"[{rep+1}/{REPEATS}] {name:18} {broker:10} lost={r['lost']:<6} "
                      f"p50={l['p50']:>8} p99={l['p99']:>9} cpu={r['broker_cpu_pct']:5.1f}% "
                      f"rss={r['broker_rss_mb']:.1f}MB", flush=True)

    def med(rs, f):
        return statistics.median(f(r) for r in rs)

    rows = []
    for name, _ in SCENARIOS:
        for broker in BROKERS:
            rs = results[name][broker]
            rows.append({
                "scenario": name, "broker": broker,
                "delivered_per_sec": med(rs, lambda r: r["delivered_msgs_per_sec"]),
                "lost": med(rs, lambda r: r["lost"]),
                "p50": med(rs, lambda r: r["latency_us"]["p50"]),
                "p99": med(rs, lambda r: r["latency_us"]["p99"]),
                "p999": med(rs, lambda r: r["latency_us"]["p99.9"]),
                "cpu": med(rs, lambda r: r["broker_cpu_pct"]),
                "rss": med(rs, lambda r: r["broker_rss_mb"]),
            })
    with open(os.path.join(OUT, "rumqttd_comparison.json"), "w") as f:
        json.dump({"rows": rows, "raw": results}, f, indent=1)

    lines = ["| scenario | broker | delivered/s | lost | p50 µs | p99 µs | p99.9 µs | broker CPU | broker RSS |",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in rows:
        lines.append(f"| {r['scenario']} | {r['broker']} | {r['delivered_per_sec']:,.0f} | {r['lost']:,.0f} | "
                     f"{r['p50']:.0f} | {r['p99']:.0f} | {r['p999']:.0f} | {r['cpu']:.0f}% | {r['rss']:.1f} MB |")
    table = "\n".join(lines)
    with open(os.path.join(OUT, "rumqttd_comparison.md"), "w") as f:
        f.write(table + "\n")
    print(table)

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return
    names = [n for n, _ in SCENARIOS]
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.2))
    colors = {"minibroker": "#3b6ea8", "rumqttd": "#d08a1a"}
    w = 0.38
    for ax, key, label in [(axes[0], "p50", "p50 latency (µs)"), (axes[1], "p99", "p99 latency (µs)"),
                           (axes[2], "cpu", "broker CPU (% of one core)")]:
        for i, broker in enumerate(BROKERS):
            vals = [next(r[key] for r in rows if r["scenario"] == n and r["broker"] == broker) for n in names]
            ax.bar([x + (i - 0.5) * w for x in range(len(names))], vals, w, label=broker, color=colors[broker])
        ax.set_xticks(range(len(names)))
        ax.set_xticklabels(names, rotation=20, ha="right", fontsize=8)
        ax.set_title(label)
        ax.legend()
    fig.suptitle(f"minibroker vs rumqttd 0.20 (MQTT 3.1.1, QoS 0), 64 B payloads, median of {REPEATS} runs")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "rumqttd_comparison.png"), dpi=110)


if __name__ == "__main__":
    main()

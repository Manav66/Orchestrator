r"""Example pipeline: a real log-file analyzer.

This one exists to show actual Python logic flowing through real task
dependencies -- not placeholder functions that just return a fixed
dict. Every task here does real work: generating realistic data,
regex-parsing it, running genuine aggregate statistics, and writing a
real report file to disk. Task outputs are real data (lists, dicts)
passed as arguments into the next task, exactly the way flowctl is
meant to be used.

    flowctl run examples/log_analysis_pipeline.py

Shape of the graph (three independent analyses fan out from the parsed
log, then converge into one report -- a realistic "gather multiple
angles on the same dataset, then summarize" pattern):

    generate_sample_log
            |
    parse_log_entries
       /    |    \
 status  suspicious  latency
 _summary  _ips        _stats
       \    |    /
      generate_report

Nothing here touches the network -- the "log" is generated locally
with a fixed random seed so every run produces the same input data,
which keeps this reliable to demo without depending on the outside
world.
"""
from __future__ import annotations

import random
import re
import statistics
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from flowctl.core.pipeline import Pipeline
from flowctl.core.task import task

_LOG_PATH = Path(tempfile.gettempdir()) / "flowctl_demo_access.log"
_REPORT_PATH = Path(tempfile.gettempdir()) / "flowctl_demo_log_report.md"

_PATHS = ["/", "/login", "/api/orders", "/api/users", "/static/app.css", "/checkout", "/health"]
_METHODS = ["GET", "GET", "GET", "POST", "GET", "POST"]
_STATUS_WEIGHTS = [(200, 78), (201, 4), (301, 3), (404, 8), (500, 4), (503, 3)]

_LOG_LINE_RE = re.compile(
    r'^(?P<ip>\d+\.\d+\.\d+\.\d+) - - \[(?P<ts>[^\]]+)\] '
    r'"(?P<method>[A-Z]+) (?P<path>\S+) HTTP/1.1" '
    r'(?P<status>\d{3}) (?P<size>\d+) (?P<response_ms>\d+)ms$'
)


@task()
def generate_sample_log():
    """Write a realistic Apache-style access log with ~600 requests
    from a mix of normal traffic and a couple of misbehaving clients
    (one hammering /login, one scraping every path fast enough to be
    a bot) -- the input the rest of the pipeline actually has to deal
    with, exactly like a real log-analysis job would.
    """
    rng = random.Random(20260915)  # fixed seed -> identical demo data every run
    normal_ips = [f"10.0.0.{i}" for i in range(2, 40)]
    noisy_ips = ["203.0.113.7", "198.51.100.23"]  # a brute-forcer + a scraper

    start = datetime(2026, 9, 15, 0, 0, 0)
    lines = []

    for i in range(560):
        ip = rng.choice(normal_ips)
        path = rng.choice(_PATHS)
        method = rng.choice(_METHODS)
        status = rng.choices(*zip(*_STATUS_WEIGHTS))[0]
        response_ms = max(3, int(rng.gauss(80, 40)))
        ts = start + timedelta(seconds=i * 4 + rng.randint(0, 3))
        lines.append(
            f'{ip} - - [{ts.strftime("%d/%b/%Y:%H:%M:%S")}] '
            f'"{method} {path} HTTP/1.1" {status} {rng.randint(200, 4000)} {response_ms}ms'
        )

    # The brute-forcer: many fast POSTs to /login, mostly rejected.
    for i in range(45):
        ts = start + timedelta(seconds=200 + i)
        lines.append(
            f'{noisy_ips[0]} - - [{ts.strftime("%d/%b/%Y:%H:%M:%S")}] '
            f'"POST /login HTTP/1.1" {rng.choice([401, 401, 401, 200])} 512 {rng.randint(5, 20)}ms'
        )

    # The scraper: hits every path very fast, unusually consistent timing.
    for i in range(80):
        ts = start + timedelta(seconds=500 + i)
        lines.append(
            f'{noisy_ips[1]} - - [{ts.strftime("%d/%b/%Y:%H:%M:%S")}] '
            f'"GET {rng.choice(_PATHS)} HTTP/1.1" 200 1024 {rng.randint(4, 9)}ms'
        )

    rng.shuffle(lines)
    _LOG_PATH.write_text("\n".join(lines) + "\n")
    return str(_LOG_PATH)


@task(depends_on=[generate_sample_log])
def parse_log_entries(log_path):
    """Regex-parse the raw log into structured records. This is the
    real parsing step every downstream task builds on -- if a line
    doesn't match the expected format it's counted and skipped rather
    than silently dropped, since a real log parser has to handle
    malformed input without crashing the whole job.
    """
    text = Path(log_path).read_text()
    entries = []
    unparseable = 0
    for line in text.splitlines():
        if not line.strip():
            continue
        m = _LOG_LINE_RE.match(line)
        if not m:
            unparseable += 1
            continue
        entries.append(
            {
                "ip": m.group("ip"),
                "timestamp": m.group("ts"),
                "method": m.group("method"),
                "path": m.group("path"),
                "status": int(m.group("status")),
                "size": int(m.group("size")),
                "response_ms": int(m.group("response_ms")),
            }
        )
    if unparseable:
        print(f"[parse_log_entries] skipped {unparseable} unparseable line(s)")
    return entries


@task(depends_on=[parse_log_entries])
def compute_status_summary(entries):
    """Real aggregation: status code distribution and overall error
    rate (4xx + 5xx as a fraction of total requests).
    """
    counts = Counter(e["status"] for e in entries)
    total = len(entries)
    errors = sum(n for status, n in counts.items() if status >= 400)
    return {
        "total_requests": total,
        "by_status": dict(sorted(counts.items())),
        "error_rate_pct": round(errors / total * 100, 2) if total else 0.0,
    }


@task(depends_on=[parse_log_entries])
def detect_suspicious_ips(entries):
    """Real anomaly detection, not a hardcoded answer: count requests
    per IP, then flag any IP whose request count is more than two
    standard deviations above the mean -- exactly the kind of
    statistical outlier check a real traffic-monitoring script would
    run, and it should organically catch the brute-forcer/scraper
    traffic generated above without knowing their IPs in advance.
    """
    per_ip = Counter(e["ip"] for e in entries)
    counts = list(per_ip.values())
    if len(counts) < 2:
        return {"suspicious": [], "mean_requests_per_ip": counts[0] if counts else 0}

    mean = statistics.mean(counts)
    stdev = statistics.stdev(counts)
    threshold = mean + 2 * stdev

    suspicious = sorted(
        (
            {"ip": ip, "requests": n, "threshold": round(threshold, 1)}
            for ip, n in per_ip.items()
            if n > threshold
        ),
        key=lambda row: -row["requests"],
    )
    return {"suspicious": suspicious, "mean_requests_per_ip": round(mean, 1)}


@task(depends_on=[parse_log_entries])
def compute_latency_stats(entries):
    """Real percentile computation (p50/p95/p99) over response times,
    per path -- the kind of breakdown you'd actually want to know
    which endpoint is slow, not just an overall average.
    """
    by_path = defaultdict(list)
    for e in entries:
        by_path[e["path"]].append(e["response_ms"])

    def percentile(values, pct):
        values = sorted(values)
        idx = min(len(values) - 1, int(len(values) * pct / 100))
        return values[idx]

    return {
        path: {
            "count": len(times),
            "p50_ms": percentile(times, 50),
            "p95_ms": percentile(times, 95),
            "p99_ms": percentile(times, 99),
        }
        for path, times in sorted(by_path.items())
    }


@task(depends_on=[compute_status_summary, detect_suspicious_ips, compute_latency_stats])
def generate_report(status_summary, suspicious, latency):
    """Merge all three analyses into one real Markdown report on disk
    -- the payoff step, exactly like a real ops script would email or
    upload a summary once the analysis is done.
    """
    lines = [
        "# Access log analysis",
        "",
        f"Total requests: **{status_summary['total_requests']}**  ",
        f"Error rate: **{status_summary['error_rate_pct']}%**",
        "",
        "## Status code breakdown",
        "",
    ]
    for status, count in status_summary["by_status"].items():
        lines.append(f"- `{status}`: {count}")

    lines += ["", "## Suspicious IPs", ""]
    if suspicious["suspicious"]:
        lines.append(f"(baseline: ~{suspicious['mean_requests_per_ip']} requests/IP)")
        lines.append("")
        for row in suspicious["suspicious"]:
            lines.append(f"- `{row['ip']}` -- {row['requests']} requests (threshold {row['threshold']})")
    else:
        lines.append("None detected.")

    lines += ["", "## Latency by path (ms)", "", "| path | count | p50 | p95 | p99 |", "|---|---|---|---|---|"]
    for path, stats in latency.items():
        lines.append(f"| `{path}` | {stats['count']} | {stats['p50_ms']} | {stats['p95_ms']} | {stats['p99_ms']} |")

    report = "\n".join(lines) + "\n"
    _REPORT_PATH.write_text(report)
    print(f"[generate_report] wrote {_REPORT_PATH}")
    return str(_REPORT_PATH)


pipeline = Pipeline(
    "log_analysis",
    [
        generate_sample_log,
        parse_log_entries,
        compute_status_summary,
        detect_suspicious_ips,
        compute_latency_stats,
        generate_report,
    ],
)

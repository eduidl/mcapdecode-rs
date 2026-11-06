#!/usr/bin/env python3
"""Compare benchmark results across revisions with interleaved rounds.

The current `benches/` and `dev/mcapbench` are copied onto every revision, so all of
them run the same cases. Revisions are measured in turn, round after round, so drift
on the host spreads over all of them instead of favouring whichever ran first.

    python3 dev/mcapbench/bench_compare.py v0.10.0 WORKTREE
    python3 dev/mcapbench/bench_compare.py --rounds 5 v0.10.0 HEAD

`WORKTREE` is the working tree including uncommitted changes. The first revision is
the reference for the change column.
"""

import argparse
import hashlib
import json
import os
import pathlib
import shutil
import statistics
import subprocess
import sys
import time

REPO = pathlib.Path(__file__).resolve().parents[2]
SUITES = ["decode", "arrow", "reader"]
OVERLAY = ["dev/mcapbench", "mcapdecode/mcapdecode/benches"]


def run(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def extract(rev, dest):
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    if rev == "WORKTREE":
        files = run(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=REPO,
            capture_output=True,
        ).stdout.split(b"\0")
        for name in filter(None, files):
            src = REPO / os.fsdecode(name)
            if src.is_file():
                (dest / os.fsdecode(name)).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dest / os.fsdecode(name))
    else:
        archive = run(["git", "archive", rev], cwd=REPO, capture_output=True).stdout
        run(["tar", "-x", "-C", str(dest)], input=archive)
    for path in OVERLAY:
        shutil.rmtree(dest / path, ignore_errors=True)
        shutil.copytree(REPO / path, dest / path)
    # Releases before the function-level benches named the decode bench `decoder`.
    manifest = dest / "mcapdecode/mcapdecode/Cargo.toml"
    manifest.write_text(manifest.read_text().replace('name = "decoder"\n', 'name = "decode"\n'))
    # Revisions sharing a crate version map to the same artifact names in the shared
    # target directory. Fresh mtimes force cargo to rebuild instead of reusing them.
    now = time.time()
    for path in dest.rglob("*"):
        if path.is_file():
            os.utime(path, (now, now))


def build(src, target, bin_dir):
    output = run(
        ["cargo", "bench", "-p", "mcapdecode", "--features", "arrow"]
        + [arg for suite in SUITES for arg in ("--bench", suite)]
        + ["--no-run", "--target-dir", str(target), "--message-format", "json"],
        cwd=src,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout
    bin_dir.mkdir(parents=True, exist_ok=True)
    for line in output.splitlines():
        event = json.loads(line)
        if event.get("reason") == "compiler-artifact" and event.get("executable"):
            if event["target"]["name"] in SUITES and "bench" in event["target"]["kind"]:
                shutil.copy2(event["executable"], bin_dir / event["target"]["name"])
    missing = [suite for suite in SUITES if not (bin_dir / suite).exists()]
    if missing:
        sys.exit(f"{src}: missing bench executables {missing}")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def key(rev):
    """Directory- and baseline-safe name for a revision."""
    return rev.replace("/", "_")


def collect(criterion_home, revs, rounds):
    """Return {case: {rev: [mean_ns per round]}}."""
    by_key = {key(rev): rev for rev in revs}
    results = {}
    for bench in criterion_home.rglob("benchmark.json"):
        name = bench.parent.name
        if "@r" not in name:
            continue
        name, rnd = name.rsplit("@r", 1)
        rev = by_key.get(name)
        if rev is None or int(rnd) > rounds:
            continue
        case = json.loads(bench.read_text())["full_id"]
        mean = json.loads((bench.parent / "estimates.json").read_text())["mean"]["point_estimate"]
        results.setdefault(case, {}).setdefault(rev, []).append(mean)
    return results


def format_time(ns):
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if ns >= scale:
            return f"{ns / scale:.3g} {unit}"
    return f"{ns:.3g} ns"


def report(results, revs):
    header = ["case"] + [f"{rev} median (min–max)" for rev in revs]
    header += [f"{rev} vs {revs[0]}" for rev in revs[1:]]
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for case in sorted(results):
        per_rev = results[case]
        medians = {rev: statistics.median(per_rev[rev]) for rev in revs if rev in per_rev}
        cells = [case]
        for rev in revs:
            if rev not in per_rev:
                cells.append("-")
                continue
            values = per_rev[rev]
            cells.append(
                f"{format_time(medians[rev])} "
                f"({format_time(min(values))}–{format_time(max(values))})"
            )
        for rev in revs[1:]:
            if rev in medians and revs[0] in medians:
                cells.append(f"{(medians[rev] / medians[revs[0]] - 1) * 100:+.1f}%")
            else:
                cells.append("-")
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("revs", nargs="+", help="git revisions, or WORKTREE")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--cooldown", type=int, default=90, help="seconds between builds and measuring")
    parser.add_argument("--work-dir", type=pathlib.Path, default=REPO / "target/bench-compare")
    parser.add_argument("--report-only", action="store_true", help="summarize an earlier run")
    args = parser.parse_args()

    work = args.work_dir.resolve()
    criterion_home = work / "criterion"
    if not args.report_only:
        bins = {}
        for rev in args.revs:
            print(f"building {rev}", flush=True)
            src = work / "src" / key(rev)
            extract(rev, src)
            bins[rev] = work / "bin" / key(rev)
            build(src, work / "target", bins[rev])
        for i, a in enumerate(args.revs):
            for b in args.revs[i + 1 :]:
                if all(digest(bins[a] / s) == digest(bins[b] / s) for s in SUITES):
                    print(f"warning: {a} and {b} built identical executables", flush=True)

        env = dict(os.environ, CRITERION_HOME=str(criterion_home))
        env.setdefault("RAYON_NUM_THREADS", "8")
        env.setdefault("MCAPBENCH_FIXTURE_DIR", str(work / "fixtures"))
        for rev in args.revs:
            for suite in SUITES:
                run([str(bins[rev] / suite), "--test"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"cooldown {args.cooldown}s", flush=True)
        time.sleep(args.cooldown)
        log = (work / "run.log").open("w")
        for rnd in range(1, args.rounds + 1):
            for rev in args.revs:
                load = " ".join(f"{x:.2f}" for x in os.getloadavg())
                print(f"round {rnd} {rev} (load {load})", flush=True)
                for suite in SUITES:
                    run(
                        [str(bins[rev] / suite), "--bench", "--noplot", "--save-baseline", f"{key(rev)}@r{rnd}"],
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                    )

    print(report(collect(criterion_home, args.revs, args.rounds), args.revs))


if __name__ == "__main__":
    main()

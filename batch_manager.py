#!/usr/bin/env python3
"""
batch_manager.py

Lightweight Batch Analysis Manager for particle-physics style workflows.

Features:
- Read config.yaml describing datasets and parameter sets
- Templated command execution with placeholders {input}, {output}, and {param_*}
- Parallel runs (local machine) with automatic retries
- Per-job log files (stdout+stderr)
- Resume capability via state JSON file
- Dry-run mode
- Simple CSV aggregation and optional ROOT merging via uproot
- Optional Slurm sbatch file generation (does NOT submit)

Usage:
    python batch_manager.py --config config.yaml

Dependencies:
    pip install pyyaml tqdm pandas imperfect-slugify
    optional for ROOT merging: pip install uproot
"""

import argparse
import yaml
import os
import json
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tqdm import tqdm
import time
import pandas as pd
import sys
import re

# optional import for ROOT histogram merging
try:
    import uproot
    UPROOT_AVAILABLE = True
except Exception:
    UPROOT_AVAILABLE = False

# small utility to slugify file names safely without heavy deps
def slugify(s: str) -> str:
    s = s.strip().replace(" ", "_")
    s = re.sub(r"[^\w\-_\.]", "", s)
    return s[:200]

def load_config(path):
    with open(path) as f:
        cfg = yaml.safe_load(f)
    return cfg

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)
    return p

def prepare_jobs(cfg):
    """
    Accepts cfg dict and returns list of job dicts:
    job = {
      'id': str,
      'input': path,
      'output': outdir,
      'params': {...},
      'cmd_template': str
    }
    """
    jobs = []
    base_output = cfg.get("base_output_dir", "results")
    ensure_dir(base_output)

    datasets = cfg.get("datasets", [])
    common_params = cfg.get("parameters", {})
    cmd_template = cfg.get("command_template", "")
    if not cmd_template:
        raise RuntimeError("config must include command_template (e.g. 'python run_analysis.py {input} --out {output} --cut {param_cut}')")

    for ds_idx, ds in enumerate(datasets):
        inpath = ds.get("path")
        if inpath is None:
            continue
        name = ds.get("name") or Path(inpath).stem
        outdir = ds.get("output") or os.path.join(base_output, slugify(name))
        ensure_dir(outdir)

        # merge params: common params overridden by ds-specific
        params = dict(common_params)
        params.update(ds.get("parameters", {}))

        job_id = f"{ds_idx:04d}_{slugify(name)}"
        jobs.append({
            "id": job_id,
            "input": inpath,
            "output_dir": outdir,
            "params": params,
            "cmd_template": cmd_template
        })
    return jobs

def build_command(job):
    # placeholders:
    # {input}, {output}, {param_<name>}
    cmd = job["cmd_template"]
    cmd = cmd.replace("{input}", str(job["input"]))
    cmd = cmd.replace("{output}", str(job["output_dir"]))
    # replace param placeholders
    for k, v in job["params"].items():
        ph = "{param_" + k + "}"
        cmd = cmd.replace(ph, str(v))
    return cmd

def write_state(state_file, state):
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)

def load_state(state_file):
    if os.path.exists(state_file):
        with open(state_file) as f:
            return json.load(f)
    return {}

def run_single_job(job, cfg, global_dir, dry_run=False):
    """
    Execute a single job; return dict with status and paths.
    """
    job_id = job["id"]
    outdir = job["output_dir"]
    ensure_dir(outdir)
    log_path = os.path.join(outdir, f"{job_id}.log")
    attempts = 0
    max_retries = cfg.get("retry_failed", 2)
    timeout = cfg.get("per_job_timeout_seconds", None)  # optional
    success = False
    last_returncode = None
    cmd = build_command(job)

    # expand environment variables in command (use shell expand)
    # We'll run via shell=True to allow complex command templates (but be careful)
    while attempts <= max_retries and not success:
        attempts += 1
        if dry_run:
            with open(log_path, "a") as f:
                f.write(f"[DRY-RUN] Would run: {cmd}\n")
            return {"job_id": job_id, "status": "dry-run", "attempts": attempts, "log": log_path, "returncode": 0, "output_dir": outdir}
        start = time.time()
        with open(log_path, "ab") as logf:
            logf.write(f"--- RUN ATTEMPT {attempts} cmd: {cmd}\n".encode())
            try:
                # note: shell=True so templates can include redirects; cmd is a string
                proc = subprocess.run(cmd, shell=True, stdout=logf, stderr=logf, timeout=timeout)
                last_returncode = proc.returncode
                if proc.returncode == 0:
                    success = True
                else:
                    # non-zero exit code: log and maybe retry
                    logf.write(f"--- non-zero return code {proc.returncode}\n".encode())
            except subprocess.TimeoutExpired as e:
                last_returncode = -999
                logf.write(f"--- TIMEOUT after {timeout} seconds\n".encode())
            except Exception as e:
                last_returncode = -998
                logf.write(f"--- EXCEPTION: {repr(e)}\n".encode())
        duration = time.time() - start
        # if not success and we will retry, append a message
        if not success and attempts <= max_retries:
            with open(log_path, "a") as f:
                f.write(f"Retrying (attempt {attempts}/{max_retries + 1})...\n")
            time.sleep(cfg.get("retry_backoff_seconds", 1))

    status = "success" if success else "failed"
    return {"job_id": job_id, "status": status, "attempts": attempts, "log": log_path, "returncode": last_returncode, "output_dir": outdir}

def aggregate_results(cfg, jobs):
    """
    Simple aggregation:
    - find CSV files in each job output_dir matching pattern (configurable)
    - merge them with pandas.concat and write aggregated CSV
    - optionally merge ROOT files with uproot (if available and configured)
    """
    agg_dir = cfg.get("aggregation_output_dir", "aggregated")
    ensure_dir(agg_dir)
    csv_pattern = cfg.get("aggregation_csv_pattern", "*.csv")  # glob inside each outdir
    collected_csvs = []
    for job in jobs:
        outdir = job["output_dir"]
        for p in Path(outdir).glob(csv_pattern):
            collected_csvs.append(str(p))
    aggregated_csv = os.path.join(agg_dir, "aggregated.csv")
    if collected_csvs:
        try:
            dfs = []
            for f in collected_csvs:
                try:
                    df = pd.read_csv(f)
                    df["_source_file"] = os.path.relpath(f)
                    dfs.append(df)
                except Exception as e:
                    print(f"Warning: failed to read CSV {f}: {e}", file=sys.stderr)
            if dfs:
                big = pd.concat(dfs, ignore_index=True, sort=False)
                big.to_csv(aggregated_csv, index=False)
                print(f"Aggregated {len(collected_csvs)} CSVs -> {aggregated_csv}")
            else:
                print("No valid CSVs found to aggregate.")
        except Exception as e:
            print("CSV aggregation failed:", e, file=sys.stderr)
    else:
        print("No CSV files found for aggregation.")

    # optional: ROOT merging if configured
    if cfg.get("aggregate_root", False):
        if not UPROOT_AVAILABLE:
            print("Requested ROOT aggregation but 'uproot' not available. Skipping.")
        else:
            root_pattern = cfg.get("aggregation_root_pattern", "*.root")
            collected_roots = []
            for job in jobs:
                outdir = job["output_dir"]
                for p in Path(outdir).glob(root_pattern):
                    collected_roots.append(str(p))
            if collected_roots:
                # naive merging: copy trees or histogram keys into a single file
                # a robust merge depends on structure; here we'll try to concatenate top-level arrays if consistent
                merged_root = os.path.join(agg_dir, "merged.root")
                # This is a best-effort approach: if trees have the same keys and compatible shapes, concatenate arrays.
                file_objs = [uproot.open(p) for p in collected_roots]
                # pick first file keys
                keys = list(file_objs[0].keys())
                out_dict = {}
                for key in keys:
                    # attempt to read as arrays from each file and concatenate
                    arrs = []
                    try:
                        for f in file_objs:
                            if key in f:
                                try:
                                    a = f[key].array(library="np")
                                    arrs.append(a)
                                except Exception:
                                    pass
                        if arrs:
                            import numpy as np
                            out_arr = np.concatenate(arrs)
                            out_dict[key] = out_arr
                    except Exception:
                        pass
                if out_dict:
                    # write with uproot
                    uproot.recreate(merged_root)
                    with uproot.recreate(merged_root) as out_f:
                        for key, arr in out_dict.items():
                            out_f[key] = arr
                    print(f"Merged ROOT arrays into {merged_root}")
                else:
                    print("No compatible ROOT arrays found to merge.")
            else:
                print("No ROOT files found for aggregation.")

def generate_slurm(job, cfg, sbatch_dir):
    ensure_dir(sbatch_dir)
    job_id = job["id"]
    cmd = build_command(job)
    sbatch_path = os.path.join(sbatch_dir, f"{job_id}.sbatch")
    with open(sbatch_path, "w") as f:
        f.write("#!/bin/bash\n")
        f.write(f"#SBATCH --job-name={job_id}\n")
        f.write(f"#SBATCH --output={job['output_dir']}/{job_id}.sbatch.out\n")
        f.write(f"#SBATCH --error={job['output_dir']}/{job_id}.sbatch.err\n")
        # add optional slurm hints from config
        slurm = cfg.get("slurm", {})
        if "time" in slurm:
            f.write(f"#SBATCH --time={slurm['time']}\n")
        if "partition" in slurm:
            f.write(f"#SBATCH --partition={slurm['partition']}\n")
        if "cpus-per-task" in slurm:
            f.write(f"#SBATCH --cpus-per-task={slurm['cpus-per-task']}\n")
        f.write("\n")
        f.write("set -euo pipefail\n")
        f.write("\n")
        f.write(cmd + "\n")
    return sbatch_path

def main():
    parser = argparse.ArgumentParser(description="Batch Analysis Manager")
    parser.add_argument("--config", "-c", required=True, help="YAML config file")
    parser.add_argument("--workers", "-j", type=int, default=None, help="Number of parallel workers (overrides config)")
    parser.add_argument("--dry-run", action="store_true", help="Do not actually run commands; write logs and report planned actions")
    parser.add_argument("--resume", action="store_true", help="Resume from state.json if present")
    parser.add_argument("--aggregate-only", action="store_true", help="Skip running jobs; only aggregate results from outputs")
    parser.add_argument("--generate-slurm", action="store_true", help="Generate sbatch files for each job (does not submit)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    jobs = prepare_jobs(cfg)
    state_file = cfg.get("state_file", "state.json")
    state = load_state(state_file) if args.resume else {}

    # If workers not provided, use config or default to cpu_count
    import multiprocessing
    default_workers = cfg.get("parallel_jobs", max(1, multiprocessing.cpu_count() - 1))
    workers = args.workers if args.workers is not None else default_workers
    print(f"Prepared {len(jobs)} jobs. Using {workers} workers. Dry-run={args.dry_run}")

    # if generate slurm, produce sbatch files and exit
    if args.generate_slurm:
        sbatch_dir = cfg.get("sbatch_dir", "sbatch_files")
        ensure_dir(sbatch_dir)
        for job in jobs:
            p = generate_slurm(job, cfg, sbatch_dir)
            print(f"Wrote {p}")
        print("SLURM sbatch generation complete.")
        return

    if args.aggregate_only:
        aggregate_results(cfg, jobs)
        return

    # prepare a list of jobs that still need to run
    job_map = {job["id"]: job for job in jobs}
    to_run = []
    for job in jobs:
        jid = job["id"]
        prev = state.get(jid, {})
        if prev.get("status") == "success":
            continue
        to_run.append(job)

    if not to_run:
        print("No jobs to run (all marked success). You can pass --resume to overwrite or remove state.json to rerun.")
    else:
        print(f"{len(to_run)} jobs to run (resume={'on' if args.resume else 'off'}). Starting...")

    results = {}
    # run jobs with thread pool
    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = {}
        for job in to_run:
            futures[exe.submit(run_single_job, job, cfg, os.getcwd(), args.dry_run)] = job["id"]
        for fut in tqdm(as_completed(futures), total=len(futures)):
            res = fut.result()
            jid = res["job_id"]
            results[jid] = res
            # update state file
            state[jid] = res
            write_state(state_file, state)

    # combine previous successful jobs (if any) with new results for reporting
    all_status = state
    n_success = sum(1 for v in all_status.values() if v.get("status") == "success")
    n_failed = sum(1 for v in all_status.values() if v.get("status") == "failed")
    n_dry = sum(1 for v in all_status.values() if v.get("status") == "dry-run")

    print("Run complete.")
    print(f"Success: {n_success}, Failed: {n_failed}, Dry-run: {n_dry}")
    # optionally aggregate
    if cfg.get("auto_aggregate", True):
        aggregate_results(cfg, jobs)

    # final report write
    report_path = cfg.get("final_report", "run_report.md")
    with open(report_path, "w") as f:
        f.write("# Batch Manager Run Report\n\n")
        f.write(f"Total jobs: {len(jobs)}\n\n")
        f.write(f"- Success: {n_success}\n")
        f.write(f"- Failed: {n_failed}\n")
        f.write(f"- Dry-run: {n_dry}\n\n")
        f.write("## Per-job summary\n\n")
        for jid, info in sorted(all_status.items()):
            f.write(f"- {jid}: {info.get('status')} (attempts={info.get('attempts')}, returncode={info.get('returncode')}) log: {info.get('log')}\n")
    print(f"Wrote final report to {report_path}")

if __name__ == "__main__":
    main()

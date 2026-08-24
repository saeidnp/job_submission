#!/usr/bin/env python3

from argparse import ArgumentParser
from datetime import datetime
import glob
import json
import os
import subprocess
from pprint import pprint
from submit_job import CMD_REPORT_FILE, get_cluster_name


def read_records(cluster_name):
    """Yields every cmd_report.jsonl record for the given cluster, oldest first (i.e. in the
    order they were appended -- submission order).
    """
    if not CMD_REPORT_FILE.exists():
        return
    with open(CMD_REPORT_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if record.get("cluster") == cluster_name:
                yield record


def resolve_output_paths(rep):
    """Resolves the actual SLURM output file(s) for a job record, substituting the %j/%x/%A/%a
    placeholders the same way SLURM itself would (see SLURMHandler.set_logging_paths()).

    Returns a list of existing paths. For a non-array job this is at most one path. For an array
    job, the record doesn't know which task indices actually ran, so this globs for whichever
    per-task files exist under the array's job id.
    """
    template = rep["scheduler_args"].get("--output") or rep["scheduler_args"].get("-o")
    if not template:
        return []
    if not os.path.isabs(template):
        template = os.path.join(rep.get("exp_dir", "."), template)
    job_id = rep["job_id"]
    job_name = rep.get("name") or ""
    if "%A" in template or "%a" in template:
        pattern = (
            template.replace("%A", str(job_id)).replace("%a", "*").replace("%x", job_name)
        )
        return sorted(glob.glob(pattern))
    path = template.replace("%j", str(job_id)).replace("%x", job_name)
    return [path] if os.path.exists(path) else []


def print_tail(rep, n_lines):
    paths = resolve_output_paths(rep)
    if not paths:
        print(
            "No output file found for job {} (expected pattern: {!r}).".format(
                rep["job_id"], rep["scheduler_args"].get("--output") or rep["scheduler_args"].get("-o")
            )
        )
        return
    if len(paths) > 1:
        print(
            "Job {} is an array job with multiple per-task output files; pick one:".format(
                rep["job_id"]
            )
        )
        for p in paths:
            print(" ", p)
        return
    path = paths[0]
    print("Log file: {}".format(path))
    with open(path, "r", errors="replace") as f:
        lines = f.readlines()
    for line in lines[-n_lines:]:
        print(line, end="")


def print_sacct(rep):
    job_id = rep["job_id"]
    fields = "JobID,JobName,Partition,State,ExitCode,Elapsed,Timelimit,ReqMem,MaxRSS,NNodes,NCPUS"
    requested = rep.get("scheduler_args", {})
    print(
        "Requested: time={} mem={} cpus-per-task={}".format(
            requested.get("--time"),
            requested.get("--mem", requested.get("--mem-per-cpu")),
            requested.get("--cpus-per-task"),
        )
    )
    try:
        proc = subprocess.run(
            ["sacct", "-j", str(job_id), "--format", fields],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        print("`sacct` was not found on this machine -- are you on a SLURM login node?")
        return
    if proc.returncode != 0:
        print("sacct failed:")
        print(proc.stderr, end="")
        return
    print(proc.stdout, end="")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-j", "--job-id", type=str)
    parser.add_argument("-f", "--format", type=str, nargs="+", default=None)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--cmd", action="store_true")
    parser.add_argument("-n", type=int, default=None)
    parser.add_argument(
        "--name-contains",
        type=str,
        default=None,
        help="With --list, only show jobs whose name contains this substring.",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="With --list, only show jobs submitted on/after this date (YYYY/MM/DD or YYYY-MM-DD).",
    )
    parser.add_argument(
        "--tail",
        nargs="?",
        type=int,
        const=20,
        default=None,
        metavar="N",
        help="Show the last N lines (default 20) of the job's SLURM output file, resolved from "
        "its recorded --output pattern -- no need to reconstruct the filename by hand.",
    )
    parser.add_argument(
        "--sacct",
        action="store_true",
        help="Show `sacct` info (actual exit code, time/memory used, etc.) for the job, next to "
        "what was requested at submission time.",
    )
    opts = parser.parse_args()
    assert opts.job_id is not None or opts.list, "Must specify job id or --list"
    if opts.cmd:
        assert opts.job_id is not None, "When --cmd is specified, must specify a job id."
        assert opts.format is None, "When --cmd is specified, --format is not allowed."
    if opts.tail is not None:
        assert opts.job_id is not None, "When --tail is specified, must specify a job id."
    if opts.sacct:
        assert opts.job_id is not None, "When --sacct is specified, must specify a job id."

    # Get the current cluster name
    cluster_name = get_cluster_name()
    records = list(read_records(cluster_name))

    if opts.list:
        ## List all jobs submitted to this cluster ##
        if opts.name_contains is not None:
            records = [r for r in records if opts.name_contains in (r.get("name") or "")]
        if opts.since is not None:
            since_dt = datetime.strptime(opts.since.replace("-", "/"), "%Y/%m/%d")
            records = [
                r
                for r in records
                if datetime.strptime(r["submission_time"], "%Y/%m/%d %H:%M:%S") >= since_dt
            ]
        if not records:
            print("No jobs submitted to this cluster ({})".format(cluster_name))
        elif opts.n is not None and opts.n > 0:
            records = records[-opts.n :]
        pprint(records)
    else:
        ## Print the report of a specific job ##
        # A job id can in principle be reused by SLURM after a long time; take the most recent
        # match if there's more than one.
        matches = [r for r in records if str(r.get("job_id")) == str(opts.job_id)]
        if not matches:
            print(
                "The job id {} under cluster {} is not found.".format(
                    opts.job_id, cluster_name
                )
            )
            exit(1)
        rep = matches[-1]
        if opts.cmd:
            ## Print the command for the job ##
            full_cmd = ["submit_job"]
            for k, v in rep["scheduler_args"].items():
                if k not in ["--mail-user", "--mail-type", "--output"]:
                    full_cmd.append(f"{k} {v}")
            full_cmd.append("--")
            full_cmd.append(rep["cmd"])
            full_cmd = " ".join(full_cmd)
            print(f"Experiment directory: {rep['exp_dir']}")
            print(f"Full command: {full_cmd}")
        elif opts.sacct:
            ## Show actual sacct info (exit code, time/mem used) for the job ##
            print_sacct(rep)
        elif opts.tail is not None:
            ## Tail the job's SLURM output file ##
            print_tail(rep, opts.tail)
        elif opts.format is not None:
            ## Print the report in a specific format ##
            for field in opts.format:
                if field in rep:
                    print("{:15} {}".format(field + ":", rep[field]))
                else:
                    print("{:15} -----(Not found)-----".format(field + ":"))
        else:
            ## Print the whole report for the job ##
            pprint(rep)

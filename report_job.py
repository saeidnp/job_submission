#!/usr/bin/env python3

from argparse import ArgumentParser
from datetime import datetime
import json
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
    opts = parser.parse_args()
    assert opts.job_id is not None or opts.list, "Must specify job id or --list"
    if opts.cmd:
        assert opts.job_id is not None, "When --cmd is specified, must specify a job id."
        assert opts.format is None, "When --cmd is specified, --format is not allowed."

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

#!/usr/bin/env python3

from argparse import ArgumentParser
import json
from pprint import pprint
from submit_job import CMD_REPORT_FILE, get_cluster_name


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-j", "--job-id", type=str)
    parser.add_argument("-f", "--format", type=str, nargs="+", default=None)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--cmd", action="store_true")
    parser.add_argument("-n", type=int, default=None)
    opts = parser.parse_args()
    assert opts.job_id is not None or opts.list, "Must specify job id or --list"
    if opts.cmd:
        assert opts.job_id is not None, "When --cmd is specified, must specify a job id."
        assert opts.format is None, "When --cmd is specified, --format is not allowed."

    # Get the current cluster name
    cluster_name = get_cluster_name()
    # Load the command reports file
    with open(CMD_REPORT_FILE, "r") as f:
        reports = json.load(f)

    if opts.list:
        ## List all jobs submitted to this cluster ##
        if cluster_name not in reports:
            print("No jobs submitted to this cluster ({})".format(cluster_name))
        if opts.n is None or opts.n <= 0:
            to_print = reports[cluster_name]
        else:
            to_print = dict(list(reports[cluster_name].items())[-opts.n:])
        pprint(to_print)
    else:
        ## Print the report of a specific job ##
        if cluster_name not in reports or opts.job_id not in reports[cluster_name]:
            print(
                "The job id {} under cluster {} is not found.".format(
                    opts.job_id, cluster_name
                )
            )
            exit(1)
        rep = reports[cluster_name][opts.job_id]
        if opts.cmd:
            ## Print the command for the job ##
            full_cmd = ["submit_job"]
            for k,v in rep["scheduler_args"].items():
                if k not in ["--main-user", "--mail-type", "--output"]:
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
            rep = {k: v for k, v in rep.items() if k in opts.format}
        else:
            ## Print the whole report for the job ##
            pprint(rep)

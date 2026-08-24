#!/usr/bin/env python3

"""
This script is used to submit batch jobs to a cluster using the SLURM scheduler.
It provides a SLURMHandler class that handles the job submission process, resolving argument
aliases, setting logging paths, exporting environment variables, and updating the command
report file.

The script also defines helper functions for printing headers and output lines in a pretty format.

Cluster identification:
The current cluster is identified by evaluating the per-cluster `"detect"` rule(s) declared in
default.json (see `get_cluster_name()`), rather than a hardcoded list of hostnames/node names.
Adding support for a new cluster is therefore just a matter of adding a new block (with a
`"detect"` rule) to default.json -- no changes to this script are needed. Supported rule types:
  - {"type": "cc_cluster"}: matches if the $CC_CLUSTER environment variable (set on Alliance
    Canada/Digital Research Alliance login nodes) equals the cluster's name (or an explicit
    "value" override). This is the most reliable signal where it's available.
  - {"type": "scontrol_clustername"}: matches if `scontrol show config`'s ClusterName equals the
    cluster's name (or an explicit "value" override). Works from a login node without a running
    job. NOTE: a cluster's public name doesn't always match its ClusterName -- e.g. Trillium's
    GPU and CPU partitions are two distinct SLURM clusters, with ClusterName "grillium" and
    "trillium" respectively -- always use an explicit "value" when they differ, and prefer
    "cc_cluster" where it's known to be unambiguous.
  - {"type": "hostname_suffix", "value": "..."}: matches if `dnsdomainname` ends with "value".
  - {"type": "sinfo_contains", "value": "..."}: matches if "value" is a substring of the node
    list printed by `sinfo -h -o %N`. Useful as a last-resort fallback where no cc_cluster/
    scontrol signal is available (e.g. UBC's "arc" cluster).
A cluster may declare more than one rule (as a list); the first rule that matches wins. Clusters
are evaluated in the order they appear in default.json, and within a cluster its rules are
evaluated in the order listed -- there is no separate confidence-based reordering. So when there's
any chance of two clusters' rules both matching the same machine (e.g. a broad sinfo_contains
substring that could coincidentally appear elsewhere), list the more specific/reliable cluster
and/or rule earlier in the file.

Arguments:
--script: used to change the default worker script (default: _run.sh at this repo's root).
  The given path is tried relative to ROOT_DIR first, then relative to the experiment directory
  (cwd) if not found there -- so a project-local worker script works too, not just one that lives
  inside this shared repo.
--local: runs the worker command directly (via os.system), skipping SLURM entirely.
--dry-run: prints what would be submitted (or run locally, if combined with --local) and exits
  without actually submitting/running anything.
Useful SLURM options (and SLURM option aliases):
--cores <N>: specifies the number of CPU cores needed
--gpu <N>: specifies the number of GPUs needed
-w <node_name>: specifies a node name
--array <first_idx>-<last_idx>: submits an array job with indices in [first_idx, last_idx]

Note: This script assumes that the SLURM scheduler is installed and configured on the cluster.
"""


import os
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime
import re


# NOTE: SLURM argument/value pairs should be passed with whitespaces in between and not equal signs.
VERBOSE = True if "SUBMIT_JOB_VERBOSE" in os.environ else False
# Paths
EXP_DIR = Path(os.getcwd())
ROOT_DIR = Path(__file__).parent.absolute()
CONFIG_FILE = ROOT_DIR / "default.json"
CMD_REPORT_FILE = ROOT_DIR / "cmd_report.jsonl"
REPORTS_DIR = EXP_DIR / "batch_job_reports"

_default_json_cache = None


def _load_default_json():
    """Reads (and caches) default.json for the lifetime of this process."""
    global _default_json_cache
    if _default_json_cache is None:
        with open(CONFIG_FILE) as f:
            _default_json_cache = json.load(f)
    return _default_json_cache


def get_cluster_name():
    """Identifies the current cluster by evaluating the "detect" rule(s) declared for each
    cluster in default.json (see the module docstring for the supported rule types).

    Raises:
        Exception: If no cluster's detection rule(s) matched the current machine.

    Returns:
        str: Cluster name (a top-level key of default.json)
    """
    json_data = _load_default_json()

    # Lazily-computed, cluster-independent signals shared by every rule of a given type.
    signals = {}

    def cc_cluster():
        if "cc_cluster" not in signals:
            signals["cc_cluster"] = os.environ.get("CC_CLUSTER")
        return signals["cc_cluster"]

    def scontrol_clustername():
        if "scontrol_clustername" not in signals:
            name = None
            retcode, out = subprocess.getstatusoutput("scontrol show config")
            if retcode == 0:
                m = re.search(r"^\s*ClusterName\s*=\s*(\S+)", out, re.MULTILINE)
                if m:
                    name = m.group(1)
            signals["scontrol_clustername"] = name
        return signals["scontrol_clustername"]

    def dns_domain():
        if "dns_domain" not in signals:
            retcode, out = subprocess.getstatusoutput("dnsdomainname")
            signals["dns_domain"] = out if retcode == 0 else None
        return signals["dns_domain"]

    def sinfo_nodes():
        if "sinfo_nodes" not in signals:
            retcode, out = subprocess.getstatusoutput("sinfo -h -o %N")
            signals["sinfo_nodes"] = out if retcode == 0 else None
        return signals["sinfo_nodes"]

    def rule_matches(cluster_name, rule):
        rtype = rule["type"]
        if rtype == "cc_cluster":
            value = cc_cluster()
            return value is not None and value == rule.get("value", cluster_name)
        elif rtype == "scontrol_clustername":
            value = scontrol_clustername()
            return value is not None and value == rule.get("value", cluster_name)
        elif rtype == "hostname_suffix":
            value = dns_domain()
            return bool(value) and value.endswith(rule["value"])
        elif rtype == "sinfo_contains":
            value = sinfo_nodes()
            return value is not None and rule["value"] in value
        else:
            raise ValueError(
                "Unknown cluster 'detect' rule type {!r} for cluster {!r}.".format(
                    rtype, cluster_name
                )
            )

    # Try clusters in the order they appear in default.json, and each cluster's rules in the
    # order listed. First match wins -- see the module docstring on ordering clusters/rules
    # deliberately when there's any chance of an overlap.
    for cluster_name, cluster_conf in json_data.items():
        if cluster_name == "__all__":
            continue
        rules = cluster_conf.get("detect", [])
        if isinstance(rules, dict):
            rules = [rules]
        for rule in rules:
            if rule_matches(cluster_name, rule):
                return cluster_name

    raise Exception(
        "Could not identify the current cluster from any 'detect' rule in {}. Checked "
        "signals: CC_CLUSTER={!r}, scontrol ClusterName={!r}, dnsdomainname={!r}, "
        "sinfo nodes={!r}. If this is a new cluster, add a block with a 'detect' rule for "
        "it to default.json.".format(
            CONFIG_FILE,
            cc_cluster(),
            scontrol_clustername(),
            dns_domain(),
            sinfo_nodes(),
        )
    )


class SLURMHandler:
    """Handles submitting a job to the SLURM scheduler."""

    job_name_arguments = ["-J", "--job-name"]

    def __init__(self, args, flags, cluster_name):
        self.cluster_name = cluster_name
        self.args = args
        self.flags = flags
        self.resolve_multi_args()
        self.dry_run = "--dry-run" in self.flags
        if self.dry_run:
            self.flags.remove("--dry-run")
        self.local = False
        if "--local" in self.flags:
            self.flags.remove("--local")
            self.local = True
        else:
            self.submisison_command = "sbatch"
            self.submit_job_script = ROOT_DIR / "_run.sh"
            self.reports_dir = REPORTS_DIR
            # Check the arguments and verify it meets the scheduler's requirements
            self.verify_args()
            # Resolve custom argument aliases
            self.resolve_aliases()
            assert (
                self.submit_job_script.exists()
            ), "Missing SLURM run script at {}.".format(self.submit_job_script)
            # Provide log file paths to the scheduler (if not already set by the user in command-line)
            self.set_logging_paths()
            # A dry run shouldn't have any side effects, including creating this directory.
            if not self.dry_run and not self.reports_dir.exists():
                self.reports_dir.mkdir(exist_ok=True)

    def verify_args(self):
        # Check SLURM arguments and make sure the required ones are existing
        if all([x not in self.args for x in self.job_name_arguments]):
            raise Exception(
                "Experiment name not provided. Use {} to provide one.".format(
                    " or ".join(self.job_name_arguments)
                )
            )
        if "--script" in self.args:
            script_path = self.args.pop("--script")
            # Try relative to this repo's own directory first (the historical/default behaviour,
            # and where an absolute path resolves regardless), then fall back to the experiment
            # directory (cwd) -- lets a worker script live next to a project's own code instead of
            # inside this shared repo.
            root_candidate = ROOT_DIR / script_path
            exp_candidate = EXP_DIR / script_path
            if root_candidate.exists():
                self.submit_job_script = root_candidate
            elif exp_candidate.exists():
                self.submit_job_script = exp_candidate
            else:
                raise FileNotFoundError(
                    "Worker script {!r} not found (checked {} and {}).".format(
                        script_path, root_candidate, exp_candidate
                    )
                )

    def resolve_aliases(self):
        if "--cores" in self.args:
            assert (
                "--cpus-per-task" not in self.args
            ), "Both --cores and --cpus-per-task were found in SLURM arguments."
            self.args["--cpus-per-task"] = self.args.pop("--cores")
        if "--gpu" in self.args:
            assert (
                "--gres" not in self.args
            ), "Both --gpu and --gres were found in SLURM arguments."
            self.args["--gres"] = "gpu:{}".format(self.args.pop("--gpu"))

    def set_logging_paths(self):
        # Provide log file paths to the scheduler (if not already set by the user in command-line)
        if "--output" not in self.args and "-o" not in self.args:
            if "--array" in self.args or "-a" in self.args:
                self.args["--output"] = str(self.reports_dir / "results-%A_%a-%x.out")
            else:
                self.args["--output"] = str(self.reports_dir / "results-%j-%x.out")

    def export_args(self, **kwargs):
        for k, v in kwargs.items():
            os.environ[k] = v
        return "--export=ALL"

    def get_job_name(self):
        for k in self.job_name_arguments:
            if k in self.args:
                return self.args[k]

    def print(self, script_args_str):
        ## Print scheduler arguments
        print(self.get_header("SLURM arguments ({})".format(self.cluster_name)))
        # Extract job name from the arguments.
        job_name = self.get_job_name()
        print(self.get_output_line("Job name", job_name))
        for k, v in self.args.items():
            if k in self.job_name_arguments:
                continue
            print(self.get_output_line(k.lstrip("-"), v))
        ## Print scheduler flags
        for k in self.flags:
            print(self.get_output_line(k.lstrip("-"), "(flag)"))
        ## Print script arguments
        print(self.get_header("Script arguments"))
        print(self.get_output_line(script_args_str))

    def submit(self, script_args, verbose=False):
        script_args_str = " ".join(script_args)
        if self.local:
            if self.dry_run:
                print(self.get_header("Dry run"))
                print(self.get_output_line("Would run locally", script_args_str))
                print(self.get_output_line("Job was NOT run"))
                print(self.get_header(None))
                return 0
            os.system(script_args_str)
        else:
            # Get all arguments in string format
            scheduler_args_str = " ".join(
                "{} {}".format(k, v) for (k, v) in self.args.items()
            )
            scheduler_args_str = scheduler_args_str + " " + " ".join(self.flags)

            # Set and export environment variables to be used later by the SLURM run script.
            export_vars = dict(
                _MY_CMD="{}".format(script_args_str),
                _MY_EXPDIR=str(EXP_DIR),
                IS_BATCH_JOB="1",
            )
            scheduler_export_args = self.export_args(**export_vars)
            scheduler_args_str = scheduler_args_str + " " + scheduler_export_args

            self.print(script_args_str)

            cmd = " ".join(
                [
                    self.submisison_command,
                    scheduler_args_str,
                    str(self.submit_job_script),
                ]
            )
            if verbose or self.dry_run:
                print(cmd)
            if self.dry_run:
                print(self.get_header("Dry run"))
                print(self.get_output_line("Job was NOT submitted"))
                print(self.get_header(None))
                return 0
            os.putenv("_MY_SCHEDULER_CMD", cmd)
            os.putenv("_MY_BATCH_JOB", "1")
            proc = subprocess.Popen(
                cmd,
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
                universal_newlines=True,
                shell=True,
            )
            stdout = proc.stdout.read()
            stderr = proc.stderr.read()
            proc.communicate()
            returncode = proc.returncode
            proc.stdout.close()
            proc.stderr.close()
            if verbose:
                print(stdout)
                print(stderr)
            # Extract and print the job id
            jobid = self.jobid_from_stdout(stdout, stderr)
            print(self.get_header("Job submission"))
            print(self.get_output_line("Job ID", jobid))
            print(self.get_header(None))
            self.update_cmd_report(script_args_str, jobid)
            return returncode

    def update_cmd_report(self, script_args_str, jobid):
        """Appends a single-line JSON record for this submission to CMD_REPORT_FILE (a JSONL
        log, one record per submitted job). Appending is the whole point: it never reads the
        existing file, so there's no read-modify-write step and therefore no race between
        concurrent submitters clobbering each other's record -- unlike the old single-JSON-blob
        format this replaced.
        """
        record = {
            "cluster": self.cluster_name,
            "job_id": int(jobid),
            "name": self.get_job_name(),
            "cmd": script_args_str,
            "exp_dir": str(EXP_DIR),
            "scheduler_args": self.args,
            "scheduler_flags": self.flags,
            "submission_time": datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
        }
        with open(CMD_REPORT_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")

    @staticmethod
    def jobid_from_stdout(stdout, stderr):
        prefix = "Submitted batch job "
        msg = re.findall(prefix + r"[0-9]+", stdout)
        assert (
            len(msg) == 1
        ), "Unexpected stdout from the sbatch command:\nSTDOUT:\n{}\n{}\nSTDERR:\n{}".format(
            stdout, "-" * 10, stderr
        )
        msg = msg[0]
        jobid = msg[len(prefix) :]
        return jobid

    def resolve_multi_args(self):
        self.args = {
            k: v[-1] if isinstance(v, list) else v for (k, v) in self.args.items()
        }

    @staticmethod
    def get_header(header, width=30, dashes_width=15):
        """Returns the given header in pretty printing format (something like "#---- {header} ----").
            In case the header is None, returns a string of the format "#------" with its length
            matching the header
        Args:
            header: The header string itself
            width: The width of the header (will pad the header if shorter than this argument)
            dashes_width: The additional width around the (padded) header
        """
        if header is None:
            return "#" + "-" * (dashes_width * 2 + width + 1)
        ldashes = max(
            0, (width - len(header)) // 2
        )  # Number of padding dashes on the left of the header
        rdashes = max(
            0, (width - len(header) + 1) // 2
        )  # Number of padding dashes on the right of the header
        assert len(header) + ldashes + rdashes == width
        return "#{} {} {}".format(
            "-" * (dashes_width - 1 + ldashes), header, "-" * (rdashes + dashes_width)
        )

    @staticmethod
    def get_output_line(k, v=None):
        """Returns the given key-value pair in pretty printing format."""
        if v is None:
            return "# {}".format(k)
        return "# {:20}: {}".format(k, v)


def get_scheduler_handler(cluster_name, scheduler_args, scheduler_flags):
    # Add default arguments (if not already set by the user in command-line)
    updated_args = default_scheduler_args(cluster_name=cluster_name)
    for k, v in scheduler_args.items():
        if k not in updated_args:
            updated_args[k] = v
        else:
            updated_args[k] = (
                updated_args[k] + [v]
                if isinstance(updated_args[k], list)
                else [updated_args[k], v]
            )
    scheduler_args = updated_args

    return SLURMHandler(
        args=scheduler_args, flags=scheduler_flags, cluster_name=cluster_name
    )


def default_scheduler_args(cluster_name):
    """Identifies the cluster (PLAI/CC) and create the default parameters (DEFAULT_SLURM_ARGS)
        accordingly based on the json file at <ROOT_DIR>/default.json

    Raises:
        ValueError: If the cluster has no matching block in default.json, or if the email
            address is not set (neither in default.json nor via _MY_SCHEDULER_EMAIL).

    Returns:
        dict: a dictionary containing default SLURM arguments
    """
    DEFAULT_SCHEDULER_ARGS = {}
    json_data = _load_default_json()
    if cluster_name not in json_data:
        raise ValueError(
            "No default.json entry for cluster {!r}. Add a block for it in {} "
            "(with mail/time/account defaults and a 'detect' rule) before submitting "
            "jobs there.".format(cluster_name, CONFIG_FILE)
        )
    # Set the cluster-independent default parameters
    if "__all__" in json_data:
        d = json_data["__all__"]
        if "--mail-user" in d:
            assert (
                d["--mail-user"] is not None and len(d["--mail-user"]) > 0
            ), "The email address is not set in {}".format(CONFIG_FILE)
        for k, v in d.items():
            DEFAULT_SCHEDULER_ARGS[k] = v
    # Add the cluster-specific parameters to the default parameters, skipping the "detect" rule(s)
    for k, v in json_data[cluster_name].items():
        if k == "detect":
            continue
        DEFAULT_SCHEDULER_ARGS[k] = v
    if "--mail-user" not in DEFAULT_SCHEDULER_ARGS:
        raise ValueError("The email address is not set in {}".format(CONFIG_FILE))
    mail_user_placeholder = "<YOUR_EMAIL_GOES_HERE>"
    if DEFAULT_SCHEDULER_ARGS["--mail-user"] == mail_user_placeholder:
        mail_adr = os.environ.get("_MY_SCHEDULER_EMAIL", None)
        if mail_adr is None:
            raise ValueError(
                "The email address is not set in {} and was not set as an "
                "environment variable in _MY_SCHEDULER_EMAIL".format(CONFIG_FILE)
            )
        DEFAULT_SCHEDULER_ARGS["--mail-user"] = mail_adr
    return DEFAULT_SCHEDULER_ARGS


def arglist2dicts(arg_list):
    """Converts a list of arguments to a dictionary of arguments
        and a list of flags.
        Example: -J job-name --time=12:00:00 --dryrun --verbose -> {"-J": "job-name", "--time": "12:00:00"}, ["--dryrun", "--verbose"]

    Args:
        arg_list: a list of arguments

    Returns:
        args: a dictionary of arguments. If an argument appears multiple times, it will be stored as the key mapped to the list of values.
        flags: a list of flags
    """
    args = {}
    flags = []

    def add_arg(k, v):
        # Note: this only catches the same flag appearing twice in THIS arg_list (i.e. actually
        # repeated on the command line). A scalar value colliding with a default.json default is
        # a separate, expected code path (see get_scheduler_handler) and does not warn here.
        if k in args:
            print(
                "Warning: {} was passed more than once on the command line ({!r} then {!r}); "
                "the last value will be used.".format(k, args[k], v),
                file=sys.stderr,
            )
            args[k] = args[k] + [v] if isinstance(args[k], list) else [args[k], v]
        else:
            args[k] = v

    i = 0
    while i < len(arg_list):
        cur_arg = arg_list[i]
        if not cur_arg.startswith("-"):
            raise ValueError("Argument should start with - or -- ({})".format(cur_arg))
        if "=" in cur_arg:
            # It's an argument name and value with an equal sign in between
            k, v = cur_arg.split("=", maxsplit=1)
            add_arg(k, v)
            i += 1
        elif i + 1 >= len(arg_list) or arg_list[i + 1].startswith("-"):
            # It's a flag
            flags.append(cur_arg)
            i += 1
        else:
            # It's an argument name and requires a value
            add_arg(cur_arg, arg_list[i + 1])
            i += 2
    return args, flags


def parse_arguments(all_args):
    split_idx = all_args.index("--") if "--" in all_args else len(all_args)
    script_args = all_args[split_idx + 1 :]
    slurm_args = all_args[:split_idx]

    return arglist2dicts(slurm_args), script_args


if __name__ == "__main__":
    # Print current time
    now = datetime.now()
    current_time = now.strftime("%Y/%m/%d %H:%M:%S")
    print("Current Time: {}".format(current_time))

    # Parse arguments
    script_path = sys.argv[0]
    (scheduler_args, scheduler_flags), script_args = parse_arguments(sys.argv[1:])
    cluster_name = get_cluster_name()
    scheduler_handler = get_scheduler_handler(
        cluster_name, scheduler_args, scheduler_flags
    )

    # Submit the job
    returncode = scheduler_handler.submit(script_args, verbose=VERBOSE)
    sys.exit(returncode)

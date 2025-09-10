#!/usr/bin/env python3

"""
This script is used to submit batch jobs to a cluster using the SLURM scheduler.
It provides a SchedulerHandler class that handles the job submission process.
The script reads SLURM arguments from the command line and submits the job using the specified scheduler.
It also provides methods for resolving argument aliases, setting logging paths, exporting environment variables,
and updating the command report file.

The SchedulerHandler class is a base class that can be extended to support different schedulers.
The SLURMHandler class is a subclass of SchedulerHandler that specifically handles SLURM job submission.

The script also defines helper functions for printing headers and output lines in a pretty format.

Arguments:
--script: used to change the default worker script. The scripts should be placed under ROOT_DIR directory e.g. <ROOT_DIR>/_run_python.sh
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
import json


# NOTE: SLURM argument/value pairs should be passed with whitespaces in between and not equal signs.
VERBOSE = True if "SUBMIT_JOB_VERBOSE" in os.environ else False
# Paths
EXP_DIR = Path(os.getcwd())
ROOT_DIR = Path(__file__).parent.absolute()
CMD_REPORT_FILE = ROOT_DIR / "cmd_report.json"
REPORTS_DIR = EXP_DIR / "batch_job_reports"


def get_cluster_name():
    """Identifies the cluster (PLAI/CC/ARC)

    Raises:
        Exception: If there is no SLURM environment
        Exception: If the SLURM cluster machines are not recognized (neither of UBC or ComputeCanada-cedar)

    Returns:
        str: Cluster name
    """
    retcode, hostname = subprocess.getstatusoutput("dnsdomainname")
    if hostname.endswith("narval.calcul.quebec"):
        cluster = "narval"
    elif hostname.endswith(".calculquebec.ca"):
        cluster = "beluga"
    elif hostname.endswith(".calcul.quebec"):
        cluster = "rorqual"
    elif hostname.endswith(".fir.alliancecan.ca"):
        cluster = "fir"
    else:
        retcode, slurm_nodes = subprocess.getstatusoutput("sinfo -h -o %N")
        cluster = None
        if retcode == 0:
            if "plai[" in slurm_nodes:
                cluster = "plai"
            elif "cdr[" in slurm_nodes:
                cluster = "cedar"
            elif "ubc-ml[" in slurm_nodes:
                cluster = "submit-ml"
            elif "se[" in slurm_nodes:
                cluster = "arc"
            elif "rack" in slurm_nodes:
                cluster = "vulcan"
            else:
                raise Exception(
                    "Unexpected SLURM nodes. Make sure you are on either of borg (UBC), arc (UBC), submit-ml (UBC) cedar (ComputeCanada), narval (ComputeCanada), or beluga (ComputeCanada)."
                )
        else:
            raise Exception("No SLURM environment found (sinfo failed).")
    return cluster


class SchedulerHandler:
    def __init__(self, args, flags, cluster_name):
        self.cluster_name = cluster_name
        self.args = args
        self.flags = flags
        self.resolve_multi_args()
        self.local = False
        if "--local" in self.flags:
            self.flags.remove("--local")
            self.local = True
        else:
            # The follownig arguments are required for the scheduler to work
            # They will be set by the child class
            self.submisison_command = None  # Job submission command e.g., sbatch
            self.submit_job_script = (
                ROOT_DIR / "_run.sh"
            )  # Path to the script used to submit the job e.g., run.sh
            self.reports_dir = REPORTS_DIR  # Path to the directory where the job reports will be stored
            # Check the arguments and verify it meets the scheduler's requirements
            self.verify_args()
            # Resolve custom argument aliases
            self.resolve_aliases()
            assert (
                self.submit_job_script.exists()
            ), "Missing submit job sctipt at {}.".format(self.submit_job_script)
            # Provide log file paths to the scheduler (if not already set by the user in command-line)
            self.set_logging_paths()
            if not self.reports_dir.exists():
                self.reports_dir.mkdir(exist_ok=True)

    @property
    def scheduler_type(self):
        raise NotImplementedError()

    @property
    def job_name_arguments(self):
        raise NotImplementedError()

    def verify_args(self):
        # Check SLURM arguments and make sure the required ones are existing
        if all([x not in self.args for x in self.job_name_arguments]):
            raise Exception(
                "Experiment name not provided. Use {} to provide one.".format(
                    " or ".join(self.job_name_arguments)
                )
            )
        if "--script" in self.args:
            self.submit_job_script = ROOT_DIR / self.args.pop("--script")

    def resolve_aliases(self):
        raise NotImplementedError()

    def set_logging_paths(self):
        raise NotImplementedError()

    def export_args(self, **kwargs):
        raise NotImplementedError()

    def get_job_name(self):
        for k in self.job_name_arguments:
            if k in self.args:
                return self.args[k]

    def print(self, script_args_str):
        ## Print scheduler arguments
        print(
            self.get_header(
                "{} arguments ({})".format(self.scheduler_type, cluster_name)
            )
        )
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
        if self.local:
            os.system(" ".join(script_args))
        else:
            # Get all arguments in string format
            script_args_str = " ".join(script_args)
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
            if verbose:
                print(cmd)
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
        if not CMD_REPORT_FILE.exists():
            reports = {}
        else:
            with open(CMD_REPORT_FILE, "r") as f:
                reports = json.load(f)
        if self.cluster_name not in reports:
            reports[self.cluster_name] = {}
        jobid = int(jobid)
        if jobid in reports[self.cluster_name]:
            print(
                f"Error in updating the cmd reports: Job ID {jobid} already exists in the command report under cluster {self.cluster_name}."
            )
            return
        new_report = {
            "name": self.get_job_name(),
            "cmd": script_args_str,
            "exp_dir": str(EXP_DIR),
            "scheduler_args": self.args,
            "scheduler_flags": self.flags,
            "submission_time": datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
        }
        reports[self.cluster_name][jobid] = new_report
        with open(CMD_REPORT_FILE, "w") as f:
            json.dump(reports, f, indent=4)

    @staticmethod
    def jobid_from_stdout(stdout, stderr):
        raise NotImplementedError()

    def resolve_multi_args(self):
        raise NotImplementedError()

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


class SLURMHandler(SchedulerHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.submisison_command = "sbatch"
        self.submit_job_script = ROOT_DIR / "_run.sh"
        assert (
            self.submit_job_script.exists()
        ), "Missing SLURM run sctipt at {}.".format(self.submit_job_script)

    @property
    def scheduler_type(self):
        return "SLURM"

    @property
    def job_name_arguments(self):
        return ["-J", "--job-name"]

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
        res = "--export=ALL"
        return res

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


class PBSHandler(SchedulerHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.submisison_command = "qsub"
        self.submit_job_script = ROOT_DIR / "_run.sh"

    @property
    def scheduler_type(self):
        return "PBS"

    @property
    def job_name_arguments(self):
        return ["-N", "--job-name"]

    def resolve_aliases(self):
        pass

    def set_logging_paths(self):
        # Provide log file paths to the scheduler (if not already set by the user in command-line)
        if "-o" not in self.args:
            self.args["-o"] = self.reports_dir / "results.out"  # results-%j-%N.out
        if "-e" not in self.args:
            self.args["-e"] = self.reports_dir / "results.err"

    def export_args(self, **kwargs):
        for k, v in kwargs.items():
            os.environ[k] = v
        res = "-V"
        return res

    @staticmethod
    def jobid_from_stdout(stdout, stderr):
        assert (
            len(stdout) > 0
        ), "Unexpected stdout from the qsub command:\nSTDOUT:\n{}\n{}\nSTDERR:\n{}".format(
            stdout, "-" * 10, stderr
        )
        jobid = stdout.split(".")[0]
        return jobid

    def resolve_multi_args(self):
        self.args_updated = {
            k: v[-1] if isinstance(v, list) else v
            for (k, v) in self.args.items()
            if k != "-l"
        }
        if "-l" not in self.args:
            self.args = self.args_updated
            return
        self.args_updated["-l"] = self.args["-l"]
        self.args = self.args_updated
        ## Handle the -l arguments, removing the repeated items
        if not isinstance(self.args["-l"], list):
            self.args["-l"] = [self.args["-l"]]
        # Put all the requested resources in a dictionary to remove duplicates
        resource_dict = {}
        for resource_list in self.args["-l"]:
            for resource in resource_list.split(","):
                k, v = resource.split("=", maxsplit=1)
                resource_dict[k] = v
        # Assemble the resource dictionary back to a string
        resources_str = ",".join(
            ["{}={}".format(k, v) for k, v in resource_dict.items()]
        )
        self.args["-l"] = resources_str


def get_scheduler_handler(cluster_name, scheduler_args, scheduler_flags):
    scheduler_machine_names = {
        "SLURM": ["plai", "submit-ml", "cedar", "narval", "beluga", "arc", "vulcan", "rorqual", "fir"],
        "PBS": [],
    }
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

    if cluster_name in scheduler_machine_names["SLURM"]:
        return SLURMHandler(
            args=scheduler_args, flags=scheduler_flags, cluster_name=cluster_name
        )
    elif cluster_name in scheduler_machine_names["PBS"]:
        return PBSHandler(
            args=scheduler_args, flags=scheduler_flags, cluster_name=cluster_name
        )
    else:
        raise Exception(
            "Schduler type for the current cluster ({}) is unknown.".format(
                cluster_name
            )
        )


def default_scheduler_args(cluster_name):
    """Identifies the cluster (PLAI/CC) and create the default parameters (DEFAULT_SLURM_ARGS)
        accordingly based on the json file at <ROOT_DIR>/default.json

    Raises:
        Exception: If there is no SLURM environment
        Exception: If the SLURM cluster machines are not recognized (neither of UBC or ComputeCanada-cedar)

    Returns:
        dict: a dictionary containing default SLURM arguments
    """
    DEFAULT_SCHEDULER_ARGS = {}
    # Read the json config file
    config_file_path = str(ROOT_DIR / "default.json")
    with open(config_file_path) as json_file:
        json_data = json.load(json_file)
    # Set the cluster-independent default parameters
    if "__all__" in json_data:
        d = json_data["__all__"]
        if "--mail-user" in d:
            assert (
                d["--mail-user"] is not None and len(d["--mail-user"]) > 0
            ), "The email address is not set in {}".format(config_file_path)
        for k, v in d.items():
            DEFAULT_SCHEDULER_ARGS[k] = v
    # Add the cluster-specific parameters to the default parameters
    for k, v in json_data[cluster_name].items():
        DEFAULT_SCHEDULER_ARGS[k] = v
    if "--mail-user" not in DEFAULT_SCHEDULER_ARGS:
        raise ValueError("The email address is not set in {}".format(config_file_path))
    mail_user_placeholder = "<YOUR_EMAIL_GOES_HERE>"
    if DEFAULT_SCHEDULER_ARGS["--mail-user"] == mail_user_placeholder:
        mail_adr = os.environ.get("_MY_SCHEDULER_EMAIL", None)
        if mail_adr is None:
            raise ValueError(
                "The email address is not set in {} and was not set as an "
                "enironment variable in _MY_SCHEDULER_EMAIL".format(config_file_path)
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
    # arg_list = map(lambda x: x.split("="), arg_list)
    # arg_list = [x for item in arg_list for x in item]
    args = {}
    flags = []
    i = 0
    while i < len(arg_list):
        cur_arg = arg_list[i]
        if not cur_arg.startswith("-"):
            raise ValueError("Argument should start with - or -- ({})".format(cur_arg))
        if "=" in cur_arg:
            # It's an argument name and value with an equal sign in between
            k, v = cur_arg.split("=", maxsplit=1)
            if k in args:
                args[k] = args[k] + [v] if isinstance(args[k], list) else [args[k], v]
            else:
                args[k] = v
            i += 1
        elif i + 1 >= len(arg_list) or arg_list[i + 1].startswith("-"):
            # It's a flag
            flags.append(cur_arg)
            i += 1
        else:
            # It's an argument name and requires a value
            k, v = cur_arg, arg_list[i + 1]
            if k in args:
                args[k] = args[k] + [v] if isinstance(args[k], list) else [args[k], v]
            else:
                args[k] = v
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

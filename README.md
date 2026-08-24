# Job submission script instructions
## Quick start
- First, set your default job submission arguments in [default.json](default.json) (__most importantly, your email address__)
- Make sure the [submit_job.py](submit_job.py) and [report_job.py](report_job.py) are executable. It can be made executable by
    ```
    chmod +x submit_job.py report_job.py
    ```
- __Setting it up as a bash function__: You can add the following code to your bashrc or zshrc file. It defines makes this submissions script accessible in your terminal!
    ```
    submit_job(){
        <PATH_TO_WHERE_THIS_REPO_IS_CLONED>/submit_job.py $*
    }

    report_job(){
        <PATH_TO_WHERE_THIS_REPO_IS_CLONED>/report_job.py $*
    }
    ```

    Remember to replace `<PATH_TO_WHERE_THIS_REPO_IS_CLONED>` with the actual path to where you cloned this repo. For example, I clone this repo at `~/.dotfiles/job_submission`, which means I use `${HOME}/.dotfiles/job_submission/` for the path above. To make the changes take effect, run `source ~/.bashrc` or `source ~/.zshrc` depending on which shell you use. Alternatively, you can log out and log back in again.

### Submitting a generic job

- Run the following
    ```
    submit_job -J <job_name> --time <time_limit> --mem <mem_limit> -- <WORKER_CMD>
    ```
    where `<WORKER_CMD>` is the command to be run by the worker.

- Example:
    ```
    submit_job -J test-job --time 2:00:00 --mem 2G -- hostname
    ```
    it submits a job with the specified name, time limit and memory limit which runs the exact command `hostname` which prints the worker machine's name.

- __NOTE__: the job submission script ([_run.sh](_run.sh)) runs `source ENV` in the working directory (i.e., the directory you are in at the time of running the `submit_job` command), if the file `ENV` exists. It gives the ability to set up the requirements for example, setting environment variables or activating a Python virtual environment. A simple example of the contents of an `ENV` file is `source <path_to_virtual_env>/bin/activate`.

- The job outputs (both stdout and stderr) are logged in a directory called `batch_job_reports` in the working directory. This `batch_job_reports` directory will be created if it does not exist already. Each output file has the naming format of `results-<job_id>-<job_name>.out` for non-array jobs and `results-<job_id>_<array_index>-<job_name>.out` for array jobs.

### What command did I run for this job?
Once a job is submitted successfully through `submit_job` and a job id is assigned to it by SLURM, it appends a record to `cmd_report.jsonl`, a plain-text log stored where this repo is cloned (I clone this repo somewhere under `~/.dotfiles`) -- one JSON object per line, one line per submitted job (across all clusters and all experiment directories). Each record includes the cluster, job id, scheduler arguments (e.g. requested time, memory, etc.), script command, experiment directory, and submission time. Being append-only, it's also just a plain-text file you can `grep`/`tail` directly if you want. You can run `report_job` to query it more conveniently.

- `report_job -j <JOB_ID>`: prints the full record information for the job with the specified job id.
- `report_job -j <JOB_ID> --cmd`: prints only the script command that was run for the job with the specified job id.
- `report_job --list -n <N>`: prints the last `N` jobs submitted (to the current cluster).
- `report_job --list --name-contains <substr>`: only lists jobs whose name contains `<substr>`.
- `report_job --list --since <YYYY-MM-DD>`: only lists jobs submitted on/after that date. Can be combined with `-n`/`--name-contains`.

## How it works (in more details)

This is the job submission command format
```
submit_job <JOB_SUBMISSION_ARGS> <SLURM_ARGS> -- <WORKER_ARGS>
```
It automatically detects the cluster you're on (based on the `detect` rule declared for each cluster in [default.json](default.json)), sets the default arguments for that cluster, and submits a SLURM job that runs [`_run.sh`](_run.sh) (we call that the "worker script") with `<WORKER_ARGS>` carried over to it.
- `<JOB_SUBMISSION_ARGS>` are the arguments to the job submission script itself ([submit_job.py](submit_job.py)). Here is the list of supported arguments:
    - `--script <path>`: specifies an alternative worker script to use instead of the default [`_run.sh`](_run.sh). `<path>` is tried relative to this repo's own directory (where `submit_job.py` lives) first, then relative to your current working directory if not found there.
    - `--local`: runs the worker command directly on the current machine (via `os.system`), instead of submitting it to SLURM. Useful for testing the worker command itself.
    - `--dry-run`: prints the SLURM arguments and the exact command that would be submitted (or run locally, if combined with `--local`), then exits without actually submitting/running anything.
- `<SLURM_ARGS>` are SLURM arguments. These commands are directly passed to the `sbatch` command (see [here](https://slurm.schedmd.com/sbatch.html) for the list of sbatch arguments). Additionally, This script supports the following config aliases:
    - `--cores <N>` (alias for `--cpus-per-task`): specifies the number of CPU cores needed.
    - `--gpu <N>` (alias for `--gres=gpu:<N>`): specifies the number of GPUs needed.

    Here is a list of frequently used SLURM arguments:
    - `-J <job_name>`: specifies the job's name.
    - `--time <time_limit>`: specifies the job's time limit.
    - `--mem <mem_limit>`: specifies the job's memory limit.
    - `-w <node_name>`: specifies the node name to submit the job to.
    - `--array <first_idx>-<last_idx>`: submits an array job with indices in [first_idx, last_idx].
- `<WORKER_ARGS>` are the arguments to the user script. These arguments will be directly passed to the worker script in an environment variable called `_MY_CMD` (see the [`_run.sh`](_run.sh) file.)

### How the cluster is detected / adding a new cluster

Each cluster's block in [default.json](default.json) declares one or more `"detect"` rules —
this is how `submit_job`/`report_job` figure out which cluster (and therefore which defaults)
apply on the machine they're run from, without any cluster-specific code. Supported rule types:

- `{"type": "cc_cluster"}`: matches if the `$CC_CLUSTER` environment variable (set on Alliance
  Canada/Digital Research Alliance login nodes) equals the cluster's name. Add an explicit
  `"value": "..."` to match against something other than the cluster's own key.
- `{"type": "scontrol_clustername"}`: matches if `scontrol show config`'s `ClusterName` equals
  the cluster's name (or an explicit `"value"`). Works from a login node, no job needs to be
  running. __Careful__: a cluster's public name doesn't always match its actual `ClusterName` —
  e.g. Trillium's GPU and CPU partitions are two distinct SLURM clusters, with `ClusterName`s
  `grillium` and `trillium` respectively — pass an explicit `"value"` whenever they differ.
- `{"type": "hostname_suffix", "value": "..."}`: matches if `dnsdomainname` ends with `"value"`.
- `{"type": "sinfo_contains", "value": "..."}`: matches if `"value"` is a substring of the node
  list printed by `sinfo -h -o %N`. Useful as a last-resort fallback where no `cc_cluster`/
  `scontrol` signal is available (e.g. UBC's `arc` cluster).

A cluster's `"detect"` value can be a single rule or a list of rules. Clusters are tried in the
order they appear in `default.json`, and within a cluster its rules are tried in the order
listed — the first rule (for any cluster) that matches wins. There's no separate
confidence-based reordering, so if there's ever a chance that two clusters' rules could both
match the same machine (e.g. a broad `sinfo_contains` substring that could coincidentally show up
elsewhere), list the more specific/reliable cluster and/or rule earlier in the file.

__To add a new cluster__: add a new top-level block to `default.json` with its scheduler defaults
(mail/time/account/etc., same shape as the existing blocks) and a `"detect"` rule. That's it — no
changes to `submit_job.py` are needed.
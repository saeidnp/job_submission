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
Once a job is submitted successfully through `submit_job` and a job id is assigned to it by SLURM, it leaves a record in a json file stored where this repo is cloned (I clone this repo somewhere under `~/.dotfiles`). This record is a mapping from the job id to the job specifications. This specification includes the scheduler arguments (e.g. requested time, memory, etc.), script command, and submission time. You can run `report_job` to query this file.

- `report_job -j <JOB_ID>`: prints the full record information for the job with the specified job id.
- `report_job -j <JOB_ID> --cmd`: prints only the script command that was run for the job with the specified job id.
- `report_job --list -n <N>`: prints the last `N` jobs submitted.

## How it works (in more details)

This is the job submission command format
```
submit_job <JOB_SUBMISSION_ARGS> <SLURM_ARGS> -- <WORKER_ARGS>
```
It automatically detects the cluster you're on, sets the default arguments for that cluster (currently PLAI and cedar clusters are supported) and submits a SLURM job with `<SLURM_ARGS>` configuration. The submitted job will run one of the scripts in [batch_job_files](batch_job_files/) (we call that "worker script") and `<WORKER_ARGS>` are carried over to this worker script.
- `<JOB_SUBMISSION_ARGS>` are the arguments to the job submission script itself ([submit_job.py](submit_job.py)). Here is the list of supported arguments:
    - `--script <path>`: specifies the worker script. default: [_run_python.sh](batch_job_files/_run_python.sh)
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
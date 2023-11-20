#!/bin/bash

### The following chunk of code is to mimic a SLURM_TMPDIR directory, if not already supported by the system ##
#_MANUAL_TMPDIR=0
#if ! [ -v "SLURM_TMPDIR" ]; then
#    export SLURM_TMPDIR=/tmp/saeidnp/${SLURM_JOB_ID}/
#    echo "Creating $SLURM_TMPDIR"
#    mkdir -p $SLURM_TMPDIR
#    _MANUAL_TMPDIR=1
#fi
#
#cleanup() {
#    # Remove the temporary directory, if created manually
#    if [[ $_MANUAL_TMPDIR -eq 1 ]]; then
#        echo "Removing $SLURM_TMPDIR"
#        rm -rf $SLURM_TMPDIR
#    fi
#}
#
## Set up a trap to call the cleanup function when the job is terminated
#trap 'cleanup' EXIT
#
## Set up a trap to call the cleanup function when the job is terminated due to time limit
#trap 'cleanup' TERM
### -------------------------------------------------------------------------------------------------------- ##

export _MY_JOB_ID=${SLURM_JOB_ID}
echo "sbatch command: ${_MY_SCHEDULER_CMD}"
echo "Script command: ${_MY_CMD}"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Host machine: $(hostname)"
echo ${_MY_EXPDIR}
cd ${_MY_EXPDIR}

if [[ -f "ENV" ]]; then
    source ENV
fi

eval "$_MY_CMD"

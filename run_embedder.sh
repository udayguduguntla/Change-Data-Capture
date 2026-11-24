#!/bin/bash
# Embedding worker: its own process, its own cores. run_pipeline.sh (CDC) stays
# on 1 thread; this one gets the physical cores (i3-10100 = 4).
export OMP_NUM_THREADS=4
export MKL_NUM_THREADS=4
export TOKENIZERS_PARALLELISM=false
exec "${PYTHON:-python}" main.py embed "$@"

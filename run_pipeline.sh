#!/bin/bash
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export TOKENIZERS_PARALLELISM=false
export OPENBLAS_NUM_THREADS=1
export RAY_ON_MEMORY_MONITOR=0

exec "${PYTHON:-python}" main.py run "$@"

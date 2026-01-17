#!/bin/bash
# run_eval_experiments.sh - Run K-of-N evaluation experiments
# Usage: docker exec -it flask_container bash /app/run_eval_experiments.sh

set -e

# Create logs directory
mkdir -p /app/logs

# Timestamp for this run
TS=$(date +%Y%m%d_%H%M%S)

echo "Starting K-of-N Evaluation Experiments - $TS"
echo "=============================================="

# Parameter grid (adjust as needed)
DATASETS="english random"
N_VALUES="20 40 100"
K_VALUES="1 5 10"
TRIALS=10

# Run experiments
for dataset in $DATASETS; do
    for n in $N_VALUES; do
        for k in $K_VALUES; do
            # Skip invalid k >= n
            if [ $k -ge $n ]; then
                continue
            fi

            echo ""
            echo "[EXP] dataset=$dataset, n=$n, k=$k, mode=gdbreach"

            python /app/eval_kofn.py \
                --engine mariadb \
                --dataset $dataset \
                --n $n \
                --k $k \
                --mode gdbreach \
                --trials $TRIALS \
                --output /app/logs/kofn_${dataset}_n${n}_k${k}_gdbreach_${TS}.csv

        done
    done
done

echo ""
echo "=============================================="
echo "All experiments completed!"
echo "Results in /app/logs/"

# Run analysis
echo ""
echo "Running analysis..."
python /app/analysis/analyze_kofn.py --input_dir /app/logs --output_dir /app/results

import os
import statistics
from pathlib import Path
import csv

DATASETS = ["random"]

# K-of-N attack configuration
# Format: {compressible_bytes: [(random_bytes, k, n), ...]}
RUN_PLAN = {
    100: [(100, 100, 500), (100, 100, 1000), (100, 100, 1500)],
    300: [(300, 100, 500), (300, 100, 1000)],
}

def yield_runs(datasets=None, run_plan=None):
    ds = datasets or DATASETS
    rp = run_plan or RUN_PLAN
    for dataset in ds:
        for compressible_byte, configs in rp.items():
            for random_byte, k_val, n_val in configs:
                yield (dataset, compressible_byte, random_byte, k_val, n_val)

def file_exists(p: Path) -> bool:
    try:
        return p.exists()
    except Exception:
        return False

print("=" * 80)
print("GDBreach K-of-N Attack Batch Runner")
print("=" * 80)
print()

for dataset, compressible_byte, random_byte, k_val, n_val in yield_runs():
    print(f"\n{'='*80}")
    print(f"Running: dataset={dataset}, compress={compressible_byte}, random={random_byte}, k={k_val}, n={n_val}")
    print(f"{'='*80}\n")

    # Run attack
    cmd = (
        f"python3 ./test_k_of_n_attack_maria_binary.py "
        f"--dataset {dataset} "
        f"--Compressible_bytes {compressible_byte} "
        f"--Random_bytes {random_byte} "
        f"--k {k_val} "
        f"--n {n_val} "
        f"--trials 10"
    )

    print(f"Executing: {cmd}")
    ret = os.system(cmd)

    if ret != 0:
        print(f"[ERROR] Command failed with return code {ret}")
        continue

    # Result directory
    RESULT_DIR = Path("k_of_n_binary_results") / f"{dataset}_{compressible_byte}_{random_byte}_k{k_val}_n{n_val}"

    if not RESULT_DIR.exists():
        print(f"[WARN] Result directory not found: {RESULT_DIR}")
        continue

    # Calculate statistics from trial CSVs
    accuracies = []
    setup_times = []
    attack_times = []
    total_times = []
    db_queries = []

    for trial_idx in range(10):
        trial_csv = RESULT_DIR / f"trial_{trial_idx}.csv"

        if not file_exists(trial_csv):
            print(f"[WARN] Missing CSV for trial {trial_idx}")
            continue

        # Count correct guesses in top-k
        try:
            with open(trial_csv, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

                if len(rows) > 0:
                    # Top-k results are already in the CSV
                    correct_count = sum(1 for row in rows if row.get('is_correct') == '1')
                    accuracy = correct_count / len(rows) if len(rows) > 0 else 0.0
                    accuracies.append(accuracy)
                    print(f"  Trial {trial_idx}: accuracy={accuracy:.4f} ({correct_count}/{len(rows)} correct)")
        except Exception as e:
            print(f"[ERROR] Failed to read {trial_csv}: {e}")
            continue

    # Write summary
    if accuracies:
        avg_acc = statistics.mean(accuracies)
        std_acc = statistics.stdev(accuracies) if len(accuracies) > 1 else 0.0
        min_acc = min(accuracies)
        max_acc = max(accuracies)

        summary_file = RESULT_DIR / "summary.txt"
        with open(summary_file, "w", encoding="utf-8") as f:
            f.write(f"GDBreach K-of-N Attack Summary\n")
            f.write(f"{'='*80}\n\n")
            f.write(f"Configuration:\n")
            f.write(f"  Dataset: {dataset}\n")
            f.write(f"  Compressible bytes: {compressible_byte}\n")
            f.write(f"  Random bytes: {random_byte}\n")
            f.write(f"  k (secrets): {k_val}\n")
            f.write(f"  n (total guesses): {n_val}\n")
            f.write(f"  Trials: {len(accuracies)}\n\n")

            f.write(f"Results:\n")
            f.write(f"  Average accuracy: {avg_acc:.4f}\n")
            f.write(f"  Std deviation: {std_acc:.4f}\n")
            f.write(f"  Min accuracy: {min_acc:.4f}\n")
            f.write(f"  Max accuracy: {max_acc:.4f}\n\n")

            f.write(f"Trial-wise accuracies:\n")
            for idx, acc in enumerate(accuracies):
                f.write(f"  Trial {idx}: {acc:.4f}\n")

        print(f"\n{'='*40}")
        print(f"Summary for {dataset}_{compressible_byte}_{random_byte}_k{k_val}_n{n_val}:")
        print(f"  Average accuracy: {avg_acc:.4f} ± {std_acc:.4f}")
        print(f"  Range: [{min_acc:.4f}, {max_acc:.4f}]")
        print(f"  Summary saved to: {summary_file}")
        print(f"{'='*40}")
    else:
        print(f"[WARN] No valid results parsed for this configuration")

print(f"\n{'='*80}")
print("All batch runs completed!")
print(f"{'='*80}")

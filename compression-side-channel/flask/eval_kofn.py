#!/usr/bin/env python3
"""
eval_kofn.py - K-of-N Attack Evaluation Harness
Paper-style evaluation for G-DBREACH k-of-n inclusion attacks

Usage:
  docker exec -it flask_container python /app/eval_kofn.py \
    --engine mariadb --dataset english --n 100 --k 5 \
    --mode gdbreach --trials 20 --output /app/logs/results.csv

Metrics tracked:
  - Top-k accuracy (exact match)
  - Per-string accuracy (TP/FP/TN/FN)
  - Number of DB queries (table updates)
  - Wall-clock time
"""
import os
import sys
import csv
import json
import time
import random
import string
import argparse
from datetime import datetime

# Path setup
if "/app" not in sys.path:
    sys.path.insert(0, "/app")

# ===== Argument Parser =====
def parse_args():
    parser = argparse.ArgumentParser(description="K-of-N Attack Evaluation")
    parser.add_argument("--engine", default="mariadb", choices=["mariadb", "mongo"],
                        help="Database engine")
    parser.add_argument("--dataset", default="english", choices=["english", "random", "email"],
                        help="Dataset type")
    parser.add_argument("--n", type=int, default=100, help="Total candidates")
    parser.add_argument("--k", type=int, default=5, help="Number of secrets (k < n)")
    parser.add_argument("--mode", default="gdbreach", choices=["baseline", "gdbreach"],
                        help="Attack mode: baseline (linear) or gdbreach (binary search)")
    parser.add_argument("--trials", type=int, default=10, help="Number of trials")
    parser.add_argument("--output", default="/app/logs/eval_results.csv",
                        help="Output CSV path")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    return parser.parse_args()

# ===== Environment Setup =====
def setup_environment(mode):
    """Set environment variables based on attack mode"""
    # Common settings
    os.environ["DBREACH_COMP_BASE"] = os.environ.get("DBREACH_COMP_BASE", "100")
    os.environ["DBREACH_PHASE_SPAN"] = os.environ.get("DBREACH_PHASE_SPAN", "100")
    os.environ["DBREACH_MAX_ROW_SIZE"] = os.environ.get("DBREACH_MAX_ROW_SIZE", "200")
    os.environ["DBREACH_FILLER_ROWS"] = os.environ.get("DBREACH_FILLER_ROWS", "600")
    os.environ["DBREACH_LOG_FULL"] = "0"

    if mode == "baseline":
        # Linear search mode (original DBREACH)
        os.environ["DBREACH_MODE"] = "linear"
    else:
        # Binary search mode (G-DBREACH)
        os.environ["DBREACH_MODE"] = "binary"

# ===== Dataset Generators =====
def load_english_words():
    """Load English word list"""
    filepath = "/app/resources/10000-english.txt"
    if not os.path.exists(filepath):
        # Fallback: generate pseudo-English words
        return [f"word{i:05d}" for i in range(10000)]
    with open(filepath, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def generate_random_strings(count, min_len=10, max_len=20, rng=None):
    """Generate random alphanumeric strings"""
    if rng is None:
        rng = random.Random()
    return [''.join(rng.choices(string.ascii_lowercase, k=rng.randint(min_len, max_len)))
            for _ in range(count)]

def generate_email_addresses(count, rng=None):
    """Generate synthetic email addresses"""
    if rng is None:
        rng = random.Random()
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.org", "university.edu"]
    emails = []
    for _ in range(count):
        name_len = rng.randint(5, 12)
        name = ''.join(rng.choices(string.ascii_lowercase, k=name_len))
        domain = rng.choice(domains)
        emails.append(f"{name}@{domain}")
    return emails

def get_dataset(dataset_type, count, rng=None):
    """Get dataset based on type"""
    if dataset_type == "english":
        words = load_english_words()
        if rng:
            rng.shuffle(words)
        else:
            random.shuffle(words)
        return words[:count] if len(words) >= count else words + generate_random_strings(count - len(words), rng=rng)
    elif dataset_type == "email":
        return generate_email_addresses(count, rng)
    else:  # random
        return generate_random_strings(count, rng=rng)

# ===== Attack Runner =====
def run_single_trial(engine, dataset_type, n, k, mode, trial_id, rng, verbose=False):
    """
    Run a single k-of-n attack trial

    Returns:
        dict with trial results
    """
    import utils.mariadb_utils as utils

    if mode == "gdbreach":
        import dbreacher_impl_binary_search as dbreach_impl
    else:
        # For baseline, we'd use linear search implementation
        # Currently using same impl but could add linear version
        import dbreacher_impl_binary_search as dbreach_impl

    import k_of_n_attacker_binary as kofn_attacker

    # Get configuration
    COMP_BASE = int(os.getenv("DBREACH_COMP_BASE", "100"))
    PHASE_SPAN = int(os.getenv("DBREACH_PHASE_SPAN", "100"))
    MAX_ROW_SIZE = int(os.getenv("DBREACH_MAX_ROW_SIZE", "200"))

    # Database connection
    db_host = os.getenv("DB_HOST", "mariadb_container")
    db_port = int(os.getenv("DB_PORT", "3306"))
    db_name = os.getenv("DB_NAME", "flask_db")
    db_user = os.getenv("DB_USER", "root")
    db_pass = os.getenv("DB_PASSWORD", "your_root_password")
    datadir = os.getenv("MARIA_DATADIR", "/var/lib/mysql")

    controller = utils.MariaDBController(db_name, db_host, db_port, db_user, db_pass, datadir)

    # Create/reset table
    tablename = f"victimtable_t{trial_id}"
    controller.create_basic_table(tablename, varchar_len=MAX_ROW_SIZE + 100)

    # Generate dataset
    all_candidates = get_dataset(dataset_type, n, rng)

    # Select k secrets (first k after shuffle)
    secrets = all_candidates[:k]
    true_indices = set(range(k))

    # Shuffle candidates so secrets are not at front
    candidate_order = list(range(n))
    rng.shuffle(candidate_order)
    guesses = [all_candidates[i] for i in candidate_order]

    # Map secret indices in shuffled order
    secret_set = set(secrets)
    true_positions = set()
    for i, g in enumerate(guesses):
        if g in secret_set:
            true_positions.add(i)

    # Insert secrets into table
    for idx, secret in enumerate(secrets):
        controller.insert_row(tablename, idx + 1, secret)

    # Create attacker
    start_idx = 10000
    filler_charset = tuple(string.printable)
    compress_char_ascii = ord('*')

    dbreacher = dbreach_impl.DBREACHerImpl(
        controller, tablename, start_idx,
        MAX_ROW_SIZE, filler_charset, compress_char_ascii
    )

    attacker = kofn_attacker.kOfNAttacker(k, dbreacher, guesses, tiesOn=True)

    # Setup phase
    setup_start = time.time()
    setup_success = False
    setup_attempts = 0

    for attempt in range(1, 11):
        setup_attempts = attempt
        if attacker.setUp():
            setup_success = True
            break

    setup_time = time.time() - setup_start

    if not setup_success:
        # Return failure result
        controller.drop_table(tablename)
        return {
            "trial_id": trial_id,
            "success": False,
            "error": "setup_failed",
            "setup_attempts": setup_attempts,
        }

    # Attack phase
    attack_start = time.time()
    attack_success = attacker.tryAllGuesses(verbose=verbose)
    attack_time = time.time() - attack_start

    if not attack_success:
        controller.drop_table(tablename)
        return {
            "trial_id": trial_id,
            "success": False,
            "error": "attack_failed",
        }

    # Get results
    top_k_results = attacker.getTopKGuesses()
    predicted_guesses = set([g for _, g in top_k_results])

    # Calculate metrics
    # Top-k exact match accuracy
    topk_exact_match = 1 if predicted_guesses == secret_set else 0

    # Per-string metrics
    tp = len(predicted_guesses.intersection(secret_set))
    fp = len(predicted_guesses - secret_set)
    fn = len(secret_set - predicted_guesses)
    tn = n - k - fp

    # Accuracy = (TP + TN) / N
    per_string_accuracy = (tp + tn) / n if n > 0 else 0

    # Precision = TP / (TP + FP)
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0

    # Recall = TP / (TP + FN)
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0

    # F1 = 2 * (Precision * Recall) / (Precision + Recall)
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    # Top-k partial accuracy = TP / k
    topk_partial_accuracy = tp / k if k > 0 else 0

    # Query counts
    db_queries = dbreacher.db_count

    # Cleanup
    controller.drop_table(tablename)

    return {
        "trial_id": trial_id,
        "success": True,
        "topk_exact_match": topk_exact_match,
        "topk_partial_accuracy": topk_partial_accuracy,
        "per_string_accuracy": per_string_accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "db_queries": db_queries,
        "setup_time": setup_time,
        "attack_time": attack_time,
        "total_time": setup_time + attack_time,
        "setup_attempts": setup_attempts,
    }

# ===== Main Evaluation Loop =====
def main():
    args = parse_args()

    # Validate k < n
    if args.k >= args.n:
        print(f"[ERROR] k ({args.k}) must be less than n ({args.n})")
        sys.exit(1)

    # Setup environment
    setup_environment(args.mode)

    # Initialize RNG
    if args.seed is not None:
        rng = random.Random(args.seed)
    else:
        rng = random.Random()

    # Print configuration
    print("=" * 60)
    print("K-of-N Attack Evaluation")
    print("=" * 60)
    print(f"Engine:   {args.engine}")
    print(f"Dataset:  {args.dataset}")
    print(f"n={args.n}, k={args.k}")
    print(f"Mode:     {args.mode}")
    print(f"Trials:   {args.trials}")
    print(f"Output:   {args.output}")
    print("=" * 60)

    # Prepare results storage
    results = []

    # Run trials
    for trial_id in range(1, args.trials + 1):
        print(f"\n[Trial {trial_id}/{args.trials}]")

        result = run_single_trial(
            args.engine, args.dataset, args.n, args.k,
            args.mode, trial_id, rng, args.verbose
        )

        # Add metadata
        result["engine"] = args.engine
        result["dataset"] = args.dataset
        result["n"] = args.n
        result["k"] = args.k
        result["mode"] = args.mode
        result["timestamp"] = datetime.now().isoformat()

        results.append(result)

        if result["success"]:
            print(f"  Accuracy: {result['topk_partial_accuracy']:.3f} "
                  f"({result['tp']}/{args.k}), "
                  f"Queries: {result['db_queries']}, "
                  f"Time: {result['total_time']:.2f}s")
        else:
            print(f"  FAILED: {result.get('error', 'unknown')}")

    # Ensure output directory exists
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Write results to CSV
    if results:
        fieldnames = [
            "engine", "dataset", "n", "k", "mode", "trial_id", "timestamp",
            "success", "topk_exact_match", "topk_partial_accuracy",
            "per_string_accuracy", "precision", "recall", "f1",
            "tp", "fp", "fn", "tn",
            "db_queries", "setup_time", "attack_time", "total_time",
            "setup_attempts", "error"
        ]

        with open(args.output, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(results)

        print(f"\n[OUTPUT] Results written to {args.output}")

    # Print summary
    successful = [r for r in results if r["success"]]
    if successful:
        avg_accuracy = sum(r["topk_partial_accuracy"] for r in successful) / len(successful)
        avg_queries = sum(r["db_queries"] for r in successful) / len(successful)
        avg_time = sum(r["total_time"] for r in successful) / len(successful)

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Successful trials: {len(successful)}/{args.trials}")
        print(f"Mean accuracy:     {avg_accuracy:.3f}")
        print(f"Mean DB queries:   {avg_queries:.1f}")
        print(f"Mean time:         {avg_time:.2f}s")
        print("=" * 60)

if __name__ == "__main__":
    main()

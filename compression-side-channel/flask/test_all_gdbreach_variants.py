"""
G-DBREACH Complete Test Suite

Tests all variants of G-DBREACH attacks:
1. Standard DBreach (baseline)
2. Gallop-DBreach (binary search optimization)
3. Group-DBreach (grouping optimization)
4. Gallop+Group (combined)
5. Ghost-DBreach (relative scores)

Usage:
    python3 test_all_gdbreach_variants.py --dataset random --k 10 --n 50
"""

import utils.mariadb_utils as utils
import dbreacher
import dbreacher_impl
import dbreacher_impl_binary_search
import decision_attacker
import decision_attacker_binary
import decision_attacker_grouping
import decision_attacker_grouping_binary
import decision_attacker_rel_scores
import random
import string
import time
import sys
import argparse
from pathlib import Path

# Configuration
table = "victimtable"
db_name = "flask_db"
maxRowSize = 200

# Argument parsing
parser = argparse.ArgumentParser(description="G-DBREACH Complete Benchmark")
parser.add_argument('--dataset', choices=['random', 'english', 'emails'], default='random')
parser.add_argument('--k', type=int, default=10, help='Number of secrets')
parser.add_argument('--n', type=int, default=50, help='Total number of guesses')
parser.add_argument('--trials', type=int, default=3, help='Number of trials')
parser.add_argument('--output', type=str, default='gdbreach_benchmark.csv', help='Output CSV file')
args = parser.parse_args()

# Database connection (use root for FLUSH TABLES privilege)
control = utils.MariaDBController(
    db_name,
    host="mariadb_container",
    port=3306,
    user="root",
    password="your_root_password",
    datadir="/var/lib/mysql",
)

# Load dataset
possibilities = []
if args.dataset == "random":
    with open("./resources/10000-english-long.txt") as f:
        for line in f:
            possibilities.append(line.strip().lower())
elif args.dataset == "english":
    with open("./resources/10000-english.txt") as f:
        for line in f:
            possibilities.append(line.strip().lower())
elif args.dataset == "emails":
    with open("./resources/fake-emails.txt") as f:
        for line in f:
            possibilities.append(line.strip().lower())

print("="*80)
print("G-DBREACH Complete Benchmark")
print("="*80)
print(f"Dataset: {args.dataset}")
print(f"Secrets (k): {args.k}")
print(f"Total guesses (n): {args.n}")
print(f"Trials: {args.trials}")
print("="*80)
print()

# Test configurations
VARIANTS = [
    {
        "name": "Standard DBreach",
        "dbreacher_class": dbreacher_impl.DBREACHerImpl,
        "attacker_class": decision_attacker.decisionAttacker,
        "uses_binary_search": False,
        "uses_grouping": False,
    },
    {
        "name": "Gallop-DBreach (Binary Search)",
        "dbreacher_class": dbreacher_impl_binary_search.DBREACHerImpl,
        "attacker_class": decision_attacker_binary.decisionAttacker,
        "uses_binary_search": True,
        "uses_grouping": False,
    },
    {
        "name": "Group-DBreach (Grouping)",
        "dbreacher_class": dbreacher_impl.DBREACHerImpl,
        "attacker_class": decision_attacker_grouping.decisionAttacker,
        "uses_binary_search": False,
        "uses_grouping": True,
    },
    {
        "name": "Gallop+Group (Binary+Grouping)",
        "dbreacher_class": dbreacher_impl_binary_search.DBREACHerImpl,
        "attacker_class": decision_attacker_grouping_binary.decisionAttacker,
        "uses_binary_search": True,
        "uses_grouping": True,
    },
    {
        "name": "Ghost-DBreach (Relative Scores)",
        "dbreacher_class": dbreacher_impl.DBREACHerImpl,
        "attacker_class": decision_attacker_rel_scores.decisionAttacker,
        "uses_binary_search": False,
        "uses_grouping": False,
    },
]

# Results storage
results = []

# CSV Header
print("variant,trial,k,n,accuracy,setup_time,attack_time,total_time,db_queries")

for variant in VARIANTS:
    print(f"\n{'='*80}")
    print(f"Testing: {variant['name']}")
    print(f"{'='*80}")

    for trial_idx in range(args.trials):
        print(f"\nTrial {trial_idx + 1}/{args.trials}")

        # Prepare data
        random.shuffle(possibilities)
        trial_possibilities = possibilities[:args.n]

        # Drop and recreate table
        control.drop_table(table)
        control.create_basic_table(
            table,
            varchar_len=maxRowSize,
            compressed=True,
            encrypted=True
        )

        # Insert k secrets
        guesses = []
        correct_guesses = set()
        for secret_idx in range(args.k):
            secret = trial_possibilities[secret_idx]
            control.insert_row(table, secret_idx, secret)
            guesses.append(secret)
            correct_guesses.add(secret)

        # Add wrong guesses
        for secret_idx in range(args.k, args.n):
            wrong_guess = trial_possibilities[secret_idx]
            guesses.append(wrong_guess)

        # Prepare filler charset
        fillerCharSet = string.printable.replace(string.ascii_lowercase, '').replace('*', '')
        if args.dataset == "emails":
            fillerCharSet = fillerCharSet.replace('_', '').replace('.', '').replace('@', '')

        # Create DBREACHer
        if variant["uses_binary_search"]:
            # Binary search version requires additional parameters
            guess_len = [len(g) for g in guesses]
            random_guess_len = max(guess_len)
            compressible_bytes = 100
            random_bytes = 100

            dbreacher_inst = variant["dbreacher_class"](
                control,
                table,
                args.k,  # startIdx
                maxRowSize,
                fillerCharSet,
                ord('*'),
                compressible_bytes,
                random_bytes,
                guesses,
                random_guess_len
            )
        else:
            dbreacher_inst = variant["dbreacher_class"](
                control,
                table,
                args.k,
                maxRowSize,
                fillerCharSet,
                ord('*')
            )

        # Create attacker
        attacker = variant["attacker_class"](dbreacher_inst, guesses)

        # Run attack
        startRound = time.time()

        # Setup
        setupStart = time.time()
        success = attacker.setUp()
        setupEnd = time.time()

        if not success:
            print("  setUp failed, retrying...")
            success = attacker.setUp()
            setupEnd = time.time()

        if not success:
            print("  setUp failed after retry, skipping variant")
            continue

        # Try all guesses
        print("  Running attack...")

        if variant["uses_grouping"]:
            # Grouping variants may use different method
            try:
                success = attacker.tryAllGuesses(verbose=False)
            except AttributeError:
                # Fallback if tryGroupedGuesses exists
                success = attacker.tryGroupedGuesses(guesses, verbose=False)
        else:
            success = attacker.tryAllGuesses(verbose=False)

        attackEnd = time.time()

        if not success:
            print("  Attack failed, skipping")
            continue

        endRound = time.time()

        # Get results
        refScores = attacker.getGuessAndReferenceScores()

        # Calculate accuracy (decision attack - threshold based)
        pcts = [(1 - (b - b_yes) / max(b_no, 1), g) for g, (b_no, b, b_yes) in refScores]
        pcts.sort(reverse=True)

        # Top-k accuracy
        top_k_guesses = [g for _, g in pcts[:args.k]]
        correct_count = sum(1 for g in top_k_guesses if g in correct_guesses)
        accuracy = correct_count / args.k if args.k > 0 else 0.0

        setup_time = setupEnd - setupStart
        attack_time = attackEnd - setupEnd
        total_time = endRound - startRound
        db_queries = dbreacher_inst.db_count

        print(f"  Accuracy: {accuracy:.4f} ({correct_count}/{args.k} correct)")
        print(f"  Setup time: {setup_time:.2f}s")
        print(f"  Attack time: {attack_time:.2f}s")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  DB queries: {db_queries}")

        # CSV output
        print(f"{variant['name']},{trial_idx},{args.k},{args.n},{accuracy},{setup_time:.2f},{attack_time:.2f},{total_time:.2f},{db_queries}")

        # Store results
        results.append({
            "variant": variant["name"],
            "trial": trial_idx,
            "k": args.k,
            "n": args.n,
            "accuracy": accuracy,
            "setup_time": setup_time,
            "attack_time": attack_time,
            "total_time": total_time,
            "db_queries": db_queries,
        })

# Summary
print(f"\n{'='*80}")
print("Summary")
print(f"{'='*80}")

for variant in VARIANTS:
    variant_results = [r for r in results if r["variant"] == variant["name"]]

    if not variant_results:
        print(f"\n{variant['name']}: No successful trials")
        continue

    avg_accuracy = sum(r["accuracy"] for r in variant_results) / len(variant_results)
    avg_setup = sum(r["setup_time"] for r in variant_results) / len(variant_results)
    avg_attack = sum(r["attack_time"] for r in variant_results) / len(variant_results)
    avg_total = sum(r["total_time"] for r in variant_results) / len(variant_results)
    avg_queries = sum(r["db_queries"] for r in variant_results) / len(variant_results)

    print(f"\n{variant['name']}:")
    print(f"  Average accuracy: {avg_accuracy:.4f}")
    print(f"  Average setup time: {avg_setup:.2f}s")
    print(f"  Average attack time: {avg_attack:.2f}s")
    print(f"  Average total time: {avg_total:.2f}s")
    print(f"  Average DB queries: {avg_queries:.0f}")

print(f"\n{'='*80}")
print("Benchmark completed!")
print(f"{'='*80}")

import utils.mariadb_utils as utils
import dbreacher
import dbreacher_impl_binary_search
import k_of_n_attacker_binary
import random
import string
import time
import sys
import argparse
from pathlib import Path

table = "victimtable"
db_name = "flask_db"

control = utils.MariaDBController(
    db_name,
    host="mariadb_container",
    port=3306,
    datadir="/var/lib/mysql",
)

parser = argparse.ArgumentParser(description="GDBreach K-of-N attack")
parser.add_argument('--dataset', choices=['random', 'english', 'emails'], required=True)
parser.add_argument('--Compressible_bytes', type=int, required=True)
parser.add_argument('--Random_bytes', type=int, required=True)
parser.add_argument('--k', type=int, default=100, help='Number of secrets (k in k-of-n)')
parser.add_argument('--n', type=int, default=1500, help='Total number of guesses (n in k-of-n)')
parser.add_argument('--trials', type=int, default=10, help='Number of trials to run')
args = parser.parse_args()

len_of_Compressible_bytes = args.Compressible_bytes
len_of_Random_bytes = args.Random_bytes
maxRowSize = len_of_Compressible_bytes + len_of_Random_bytes
k_value = args.k
n_value = args.n
num_trials = args.trials

# Output directory
OUT_DIR = Path("k_of_n_binary_results") / f"{args.dataset}_{len_of_Compressible_bytes}_{len_of_Random_bytes}_k{k_value}_n{n_value}"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Load dataset
possibilities = []
if args.dataset == "random":
    with open("./resources/10000-english-long.txt") as f:
        for line in f:
            word = line.strip().lower()
            possibilities.append(word)
elif args.dataset == "english":
    with open("./resources/10000-english.txt") as f:
        for line in f:
            word = line.strip().lower()
            possibilities.append(word)
elif args.dataset == "emails":
    with open("./resources/fake-emails.txt") as f:
        for line in f:
            email = line.strip().lower()
            possibilities.append(email)

print(f"Starting GDBreach K-of-N Attack: k={k_value}, n={n_value}")
print(f"Dataset: {args.dataset}, Compressible bytes: {len_of_Compressible_bytes}, Random bytes: {len_of_Random_bytes}")
print(f"Running {num_trials} trials...")
print()

# CSV header
print("trial,k,n,accuracy,top_k_correct,setup_time,attack_time,total_time,db_queries")

startAllTrials = time.time()

for trial in range(num_trials):
    print(f"\n{'='*80}")
    print(f"Trial {trial + 1}/{num_trials}")
    print(f"{'='*80}")

    # Create CSV for this trial
    trial_csv = OUT_DIR / f"trial_{trial}.csv"

    # Drop and recreate table
    control.drop_table(table)
    control.create_basic_table(
        table,
        varchar_len=maxRowSize,
        compressed=True,
        encrypted=True
    )

    # Shuffle and select candidates
    random.shuffle(possibilities)
    trial_possibilities = possibilities[:n_value]

    # Insert k secrets
    guesses = []
    correct_guesses = set()

    print(f"Inserting {k_value} secrets...")
    for secret_idx in range(k_value):
        secret = trial_possibilities[secret_idx]
        control.insert_row(table, secret_idx, secret)
        guesses.append(secret)
        correct_guesses.add(secret)

    print(f"Correct secrets: {list(correct_guesses)[:10]}{'...' if k_value > 10 else ''}")

    # Add wrong guesses
    print(f"Adding {n_value - k_value} wrong guesses...")
    for secret_idx in range(k_value, n_value):
        wrong_guess = trial_possibilities[secret_idx]
        guesses.append(wrong_guess)

    # Calculate random_guess_len
    guess_len = [len(g) for g in guesses]
    min_len = min(guess_len)
    max_len = max(guess_len)
    random_guess_len = max_len

    print(f"Guess length range: {min_len}-{max_len}, using: {random_guess_len}")

    # Prepare filler charset
    fillerCharSet = string.printable.replace(string.ascii_lowercase, '').replace('*', '')
    if args.dataset == "emails":
        fillerCharSet = fillerCharSet.replace('_', '').replace('.', '').replace('@', '')

    # Create DBREACHer with binary search
    dbreacher = dbreacher_impl_binary_search.DBREACHerImpl(
        control,
        table,
        k_value,  # startIdx
        maxRowSize,
        fillerCharSet,
        ord('*'),
        len_of_Compressible_bytes,
        len_of_Random_bytes,
        guesses,
        random_guess_len
    )

    # Create K-of-N attacker
    attacker = k_of_n_attacker_binary.kOfNAttacker(
        k=k_value,
        dbreacher=dbreacher,
        guesses=guesses,
        tiesOn=True  # Include ties
    )

    # Run attack
    success = False
    startRound = time.time()

    print("Running setUp...")
    setupStart = time.time()
    success = attacker.setUp()
    setupEnd = time.time()

    if not success:
        print("setUp failed, retrying...")
        success = attacker.setUp()
        setupEnd = time.time()

    if success:
        print("setUp successful, trying all guesses...")
        success = attacker.tryAllGuesses(verbose=False)
        attackEnd = time.time()
    else:
        print("setUp failed after retry, skipping trial")
        continue

    endRound = time.time()

    # Get results
    topKGuesses = attacker.getTopKGuesses()

    # Calculate accuracy
    top_k_correct = sum(1 for score, guess in topKGuesses if guess in correct_guesses)
    accuracy = top_k_correct / k_value if k_value > 0 else 0.0

    setup_time = setupEnd - setupStart
    attack_time = attackEnd - setupEnd
    total_time = endRound - startRound

    print(f"\nResults for Trial {trial + 1}:")
    print(f"  Top-{k_value} correct: {top_k_correct}/{k_value}")
    print(f"  Accuracy: {accuracy:.4f}")
    print(f"  Setup time: {setup_time:.2f}s")
    print(f"  Attack time: {attack_time:.2f}s")
    print(f"  Total time: {total_time:.2f}s")
    print(f"  DB queries: {dbreacher.db_count}")

    # Write to CSV
    with open(trial_csv, "w", encoding="utf-8") as csvf:
        csvf.write("guess,score,is_correct\n")
        for score, guess in topKGuesses:
            is_correct = 1 if guess in correct_guesses else 0
            csvf.write(f"{guess},{score},{is_correct}\n")

    # Print to stdout for aggregation
    print(f"{trial},{k_value},{n_value},{accuracy},{top_k_correct},{setup_time},{attack_time},{total_time},{dbreacher.db_count}")

endAllTrials = time.time()

print(f"\n{'='*80}")
print(f"All trials completed in {endAllTrials - startAllTrials:.2f}s")
print(f"Results saved to: {OUT_DIR}")
print(f"{'='*80}")

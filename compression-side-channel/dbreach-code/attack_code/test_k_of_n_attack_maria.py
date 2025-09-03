# test_k_of_n_attack_maria.py (교체본)
import utils.mariadb_utils as utils
import dbreacher_impl
import decision_attacker
import random
import string
import time
import sys

# ----------------- 인자 파싱(안전) -----------------
mode = "--random"           # 기본 모드
secrets_to_try = [1]        # 기본 k
args = sys.argv[1:]

if len(args) >= 1:
    mode = args[0]
if len(args) >= 2 and args[1] == "--num_secrets":
    try:
        secrets_to_try = [int(a) for a in args[2:]] or [1]
    except Exception:
        secrets_to_try = [1]

# ----------------- 상수/초기화 -----------------
maxRowSize = 200
table = "victimtable"
db_name = "flask_db"

control = utils.MariaDBController(
    db_name,
    host="mariadb_container",
    port=3306,
    datadir="/var/lib/mysql",
    container_name="mariadb_container",
    container_datadir="/var/lib/mysql",
)

# 초기 테이블(압축+암호화) — 첫 실행 시점에 한번 만들어 선행 검증
control.drop_table(table)
control.create_basic_table(
    table,
    varchar_len=maxRowSize,
    compressed=True,
    encrypted=True,   # ★ 논문 전제
)

# 후보군 생성
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
    print(f"[WARN] 알 수 없는 모드 {mode}, --random 으로 진행합니다.")
    mode = "--random"
    for _ in range(2000):
        size = random.randint(10, 20)
        secret = "".join(random.choices(string.ascii_lowercase, k=size))
        possibilities.append(secret)

# fillerCharSet: 소문자/특정 문자를 제거(집합 연산) → 시퀀스로 정렬
fset = set(string.printable) - set(string.ascii_lowercase) - {'*'}
if mode == "--emails":
    fset = fset - {'_', '.', '@'}
fillerCharSet = ''.join(sorted(fset))

print("records_on_page,k,accuracy_n_500,accuracy_n_750,accuracy_n_1000,accuracy_n_1250,accuracy_n_1500,setup_time,per_guess_time")

for num_secrets in secrets_to_try:
    random.shuffle(possibilities)

    # trial 루프 (원 코드대로 1회)
    for trial in range(1):
        # 매 trial마다 테이블을 다시 깨끗하게 (압축+암호화)
        control.drop_table(table)
        control.create_basic_table(
            table,
            varchar_len=maxRowSize,
            compressed=True,
            encrypted=True,   # ★ 반드시 True
        )

        # 시크릿 삽입 + 전체 guess 목록 구성
        guesses = []
        correct_guesses = set()

        # 시크릿을 id 1..k로 삽입(가독성 및 충돌 방지)
        for secret_idx in range(num_secrets):
            secret = possibilities[(trial + secret_idx) % len(possibilities)]
            control.insert_row(table, secret_idx + 1, secret)
            guesses.append(secret)
            correct_guesses.add(secret)

        # 나머지 오답 후보 채우기 (총 1500개까지)
        for secret_idx in range(num_secrets, 1500):
            wrong_guess = possibilities[(trial + secret_idx) % len(possibilities)]
            guesses.append(wrong_guess)

        # 집합 슬라이스
        _500_guesses  = set(guesses[:500])
        _750_guesses  = set(guesses[:750])
        _1000_guesses = set(guesses[:1000])
        _1250_guesses = set(guesses[:1250])
        _1500_guesses = set(guesses[:1500])

        # DBREACHer 구현체 — filler는 시크릿 영역과 충분히 떨어진 곳(startIdx)에서 시작
        # (너비 여유를 크게 주면 경계 세팅이 덜 깨짐)
        startIdx = max(10000, num_secrets + 10)
        dbreach = dbreacher_impl.DBREACHerImpl(
            control, table, startIdx=startIdx,
            maxRowSize=maxRowSize, fillerCharSet=fillerCharSet,
            compressCharAscii=ord('*')
        )

        # decision_attacker: 내가 준 새 버전(시그니처: (db, guesses, fillerCharSet))을 우선 사용,
        # 구버전(인자 2개)도 자동 호환
        try:
            attacker = decision_attacker.decisionAttacker(dbreach, guesses, fillerCharSet)
        except TypeError:
            attacker = decision_attacker.decisionAttacker(dbreach, guesses)

        # 세팅-측정 루프
        success = False
        setupStart = time.time()
        while not success:
            if not attacker.setUp():
                continue
            # setUp 성공 시에만 tryAllGuesses 시도
            success = attacker.tryAllGuesses(verbose=False)
        setupEnd = time.time()

        # 참조 스코어 포함 점수 계산
        refScores = attacker.getGuessAndReferenceScores()
        # 원 코드의 정규화 지표 유지
        pcts = [(1 - (b - b_yes) / max(b_no, 1), g) for g, (b_no, b, b_yes) in refScores]
        pcts.sort(reverse=True)

        # 각 풀에서 상위 k개 정확도
        def topk_acc(pool_set):
            top = [(pct, g) for pct, g in pcts if g in pool_set][:num_secrets]
            return sum(1 for _, g in top if g in correct_guesses) / num_secrets if num_secrets > 0 else 0.0

        accuracy_500  = topk_acc(_500_guesses)
        accuracy_750  = topk_acc(_750_guesses)
        accuracy_1000 = topk_acc(_1000_guesses)
        accuracy_1250 = topk_acc(_1250_guesses)
        accuracy_1500 = sum(1 for _, g in pcts[:num_secrets] if g in correct_guesses) / num_secrets

        end = time.time()
        per_guess_time = (end - setupEnd) / max(len(guesses), 1)

        print(
            f"{num_secrets},{num_secrets},"
            f"{accuracy_500},{accuracy_750},{accuracy_1000},{accuracy_1250},{accuracy_1500},"
            f"{setupEnd - setupStart},{per_guess_time}"
        )

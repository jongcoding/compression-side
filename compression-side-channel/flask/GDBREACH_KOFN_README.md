# GDBreach K-of-N Attack

이 문서는 GDBreach (Guided DBreach) 알고리즘을 사용한 K-of-N 공격 구현 및 실행 방법을 설명합니다.

## 파일 구조

```
flask/
├── k_of_n_attacker_binary.py          # K-of-N 공격자 (바이너리 서치 버전)
├── test_k_of_n_attack_maria_binary.py # 단일 실행 테스트 스크립트
├── run_k_of_n_binary.py               # 배치 실행 스크립트
├── dbreacher_impl_binary_search.py    # GDBreach 구현 (바이너리 서치 최적화)
├── decision_attacker_binary.py        # Decision Attacker (바이너리 서치)
└── resources/                         # 데이터셋 디렉토리
    ├── 10000-english.txt
    ├── 10000-english-long.txt
    └── fake-emails.txt
```

## K-of-N 공격이란?

**K-of-N 공격**은 N개의 후보 중에서 실제로 데이터베이스에 존재하는 K개의 시크릿을 찾아내는 공격입니다.

- **K**: 데이터베이스에 삽입된 실제 시크릿의 개수
- **N**: 전체 후보군의 크기 (K개 정답 + (N-K)개 오답)
- **목표**: 압축 사이드 채널을 이용해 상위 K개 추측값이 실제 시크릿과 일치하도록 함

예: K=100, N=1500 → 1500개 후보 중 실제 100개 시크릿을 찾아냄

## GDBreach vs DBreach 차이점

### DBreach (기존 방법)
- **선형 탐색**: 압축이 발생할 때까지 한 바이트씩 증가
- **복잡도**: O(N)
- **느림**: 많은 DB 쿼리 필요

### GDBreach (개선 방법)
- **바이너리 서치**: 압축이 발생하는 정확한 바이트 수를 바이너리 서치로 탐색
- **복잡도**: O(log N)
- **빠름**: DB 쿼리 수 대폭 감소

## 실행 방법

### 1. Docker 환경 시작

먼저 Docker Desktop이 실행 중인지 확인한 후:

```bash
cd c:\Users\ialle\Desktop\compression-side-1\compression-side-channel
docker-compose up -d
```

컨테이너 상태 확인:
```bash
docker-compose ps
```

### 2. 단일 테스트 실행

Flask 컨테이너에서 K-of-N 공격을 실행합니다:

```bash
docker exec -it flask_container python3 test_k_of_n_attack_maria_binary.py \
  --dataset random \
  --Compressible_bytes 100 \
  --Random_bytes 100 \
  --k 100 \
  --n 500 \
  --trials 10
```

**파라미터 설명:**
- `--dataset`: 데이터셋 선택 (`random`, `english`, `emails`)
- `--Compressible_bytes`: 압축 가능한 바이트 수 (예: 100)
- `--Random_bytes`: 랜덤 바이트 수 (예: 100)
- `--k`: 실제 시크릿 개수 (예: 100)
- `--n`: 전체 후보군 크기 (예: 500, 1000, 1500)
- `--trials`: 반복 실행 횟수 (기본: 10)

### 3. 배치 실행

여러 파라미터 조합을 자동으로 실행:

```bash
docker exec -it flask_container python3 run_k_of_n_binary.py
```

배치 실행 설정은 `run_k_of_n_binary.py` 파일의 `RUN_PLAN` 변수에서 수정 가능:

```python
RUN_PLAN = {
    100: [(100, 100, 500), (100, 100, 1000), (100, 100, 1500)],
    300: [(300, 100, 500), (300, 100, 1000)],
}
# Format: {compressible_bytes: [(random_bytes, k, n), ...]}
```

### 4. 결과 확인

결과는 `k_of_n_binary_results/` 디렉토리에 저장됩니다:

```
k_of_n_binary_results/
└── random_100_100_k100_n500/
    ├── trial_0.csv      # 각 trial의 상위 k개 결과
    ├── trial_1.csv
    ├── ...
    ├── trial_9.csv
    └── summary.txt      # 전체 통계 요약
```

**CSV 형식** (`trial_X.csv`):
```csv
guess,score,is_correct
example_word,0.012345,1
another_word,0.011234,0
...
```

**Summary 형식** (`summary.txt`):
```
Average accuracy: 0.9500
Std deviation: 0.0200
Min accuracy: 0.9200
Max accuracy: 0.9800
```

## 알고리즘 상세

### 1. 초기 설정 (setUp)
```python
attacker.setUp()
```
- Filler 행 삽입
- 첫 번째 바이너리 서치: 압축이 발생하기까지 필요한 압축 가능 바이트 수 찾기

### 2. 모든 추측값 테스트 (tryAllGuesses)
```python
attacker.tryAllGuesses()
```
- 각 추측값을 테이블에 삽입
- 두 번째 바이너리 서치: 추측값마다 압축 신호 측정
- 압축 점수(Compressibility Score) 계산

### 3. 상위 K개 추출 (getTopKGuesses)
```python
topKGuesses = attacker.getTopKGuesses()
```
- 압축 점수가 높은 순으로 정렬
- 상위 K개를 반환 (동점 포함 옵션: `tiesOn=True`)

## 성능 지표

### Accuracy (정확도)
```
accuracy = (상위 K개 중 실제 시크릿 개수) / K
```

예: K=100, 상위 100개 중 95개가 실제 시크릿 → accuracy = 0.95

### Setup Time
- Filler 삽입 및 초기 바이너리 서치 시간

### Attack Time
- 모든 추측값 테스트 시간 (= N × 단일 추측값 테스트 시간)

### DB Queries
- 전체 공격 동안 실행된 데이터베이스 쿼리 수
- GDBreach는 바이너리 서치로 이를 대폭 감소

## 파라미터 튜닝 가이드

### Compressible_bytes
- **낮음 (100)**: 빠르지만 신호가 약함
- **높음 (300-500)**: 느리지만 신호가 강함
- **권장**: 100~300

### Random_bytes
- **의미**: 추측값 뒤에 붙는 랜덤 데이터 길이
- **권장**: Compressible_bytes와 동일하게 설정

### k (시크릿 개수)
- **낮음 (10-50)**: 쉬운 문제, 높은 정확도 기대
- **높음 (100-200)**: 어려운 문제, 낮은 정확도 가능

### n (후보군 크기)
- **500**: 작은 후보군, 빠른 실행
- **1000-1500**: 큰 후보군, 현실적 시나리오

## 문제 해결

### Docker 컨테이너가 시작되지 않음
```bash
# Docker Desktop이 실행 중인지 확인
# 컨테이너 로그 확인
docker-compose logs flask
docker-compose logs mariadb
```

### "Table shrunk too early" 에러
- Filler 삽입 중 테이블이 예상보다 빨리 압축됨
- 해결: `reinsertFillers()` 재시도 (자동 처리됨)

### Accuracy가 너무 낮음
- `Compressible_bytes` 증가 (예: 100 → 300)
- `Random_bytes` 증가
- `k` 감소 (더 쉬운 문제로 변경)

### 실행 시간이 너무 김
- `Compressible_bytes` 감소
- `n` 감소 (후보군 크기 축소)
- `trials` 감소 (반복 횟수 축소)

## 참고 자료

- 원본 DBreach 논문: [DBREACH: Stealing from Databases Using Compression Side Channels](https://eprint.iacr.org/2023/1671)
- GDBreach 구현: https://github.com/bnbourassa/gdbreach-attacks

## 라이선스

This is a research implementation for educational purposes only.

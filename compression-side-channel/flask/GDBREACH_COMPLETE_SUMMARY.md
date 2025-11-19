# G-DBREACH Complete Attack Suite

## G-DBREACH란?

**G-DBREACH**(Guided DBREACH)는 원본 DBREACH 공격을 최적화한 세 가지 알고리즘 기법을 포함합니다:

1. **Gallop-DBREACH (바이너리 서치 최적화)** - 이미 구현됨
2. **Group-DBREACH (그룹핑 최적화)**
3. **Ghost-DBREACH (상대 점수 최적화)**

---

## 1. Gallop-DBREACH (바이너리 서치)

### 개념
- 압축이 발생하는 정확한 바이트 수를 **바이너리 서치**로 찾음
- 기존 선형 탐색 O(N) → O(log N) 복잡도

### 구현 파일
- `dbreacher_impl_binary_search.py` ✅
- `decision_attacker_binary.py` ✅
- `k_of_n_attacker_binary.py` ✅
- `test_decision_attack_maria_binary.py` ✅
- `test_k_of_n_attack_maria_binary.py` ✅

### 핵심 메서드
```python
def addCompressibleByteAndCheckIfShrunk(self, refGuess, lowBytes=0, highBytes=None):
    while highBytes >= lowBytes:
        midBytes = (lowBytes + highBytes) // 2
        shrunk = self.checkIfShrunk(midBytes)
        if shrunk:
            highBytes = midBytes - 1
        else:
            lowBytes = midBytes + 1
```

---

## 2. Group-DBREACH (그룹핑 최적화)

### 개념
- 추측값들을 **그룹으로 묶어서** 참조 점수(reference score) 계산 횟수 감소
- 길이가 비슷한 추측값들은 동일한 참조 점수 사용
- **DB 쿼리 수 대폭 감소**

### 구현 파일 (gdbreach-attacks)
- `decision_attacker_grouping.py` ❌ (누락)
- `decision_attacker_grouping_binary.py` ❌ (누락)
- `test_decision_attack_maria_grouping.py` ❌ (누락)
- `test_decision_attack_maria_grouping_binary.py` ❌ (누락)

### 핵심 최적화
```python
def calculateReferenceScores(self, guesses):
    # 길이별로 그룹핑
    lengths = list(set([len(g) for g in guesses]))
    lengths.sort()

    # 7개 간격으로 샘플링 (모든 길이에 대해 참조 점수 계산 X)
    ref_score_lengths = []
    for i in range(0, len(lengths), 7):
        ref_score_lengths.append(lengths[i])

    # 샘플링된 길이에 대해서만 참조 점수 계산
    for i in ref_score_lengths:
        b_yes = self.dbreacher.getSYesReferenceScore(i)
        b_no = self.dbreacher.getSNoReferenceScore(i)
```

### 장점
- **참조 점수 계산 횟수**: N번 → N/7번 (약 85% 감소)
- 공격 정확도는 유지하면서 속도 향상

---

## 3. Ghost-DBREACH (상대 점수 최적화)

### 개념
- 추측값 길이에 **정확히 일치하는** 참조 점수가 없어도 공격 가능
- **근처 길이**의 참조 점수를 사용 (±1~2 길이 범위)
- 참조 점수 계산을 더욱 줄임

### 구현 파일 (gdbreach-attacks)
- `decision_attacker_rel_scores.py` ❌ (누락)
- `decision_attacker_rel_scores_grouping.py` ❌ (누락)
- `decision_attacker_binary_and_rel_scores.py` ❌ (누락)

### 핵심 메서드
```python
def findRelativeReferenceScores(self, length):
    # 길이 ±1 범위에서 참조 점수 찾기
    for i in range(length-1, length+2):
        if i in self.bYesReferenceScores:
            return True
    return False

def getRelativeReferenceScore(self, length):
    # 가장 가까운 길이의 참조 점수 반환
    relativeLengths = {}
    for i in range(length-1, length+2):
        if i in self.bYesReferenceScores:
            relativeLengths[i] = abs(length-i)
    sortLengths = sorted(relativeLengths, key=relativeLengths.get)
    return sortLengths[0]
```

### 장점
- 참조 점수 계산 없이도 공격 가능
- 다양한 길이의 추측값에 대응 가능

---

## 4. 조합 최적화

### Gallop + Group
- `decision_attacker_grouping_binary.py`
- 바이너리 서치 + 그룹핑 결합
- **최고 속도** 달성

### Gallop + Ghost
- `decision_attacker_binary_and_rel_scores.py`
- 바이너리 서치 + 상대 점수
- 유연성과 속도의 균형

### Group + Ghost
- `decision_attacker_rel_scores_grouping.py`
- 그룹핑 + 상대 점수
- 적은 참조 점수로 높은 정확도

---

## 현재 구현 상태

| 파일 | 설명 | 상태 |
|------|------|------|
| `dbreacher.py` | 기본 추상 클래스 | ✅ |
| `dbreacher_impl.py` | 표준 DBreach | ✅ |
| `dbreacher_impl_binary_search.py` | Gallop-DBreach | ✅ |
| `decision_attacker.py` | 표준 Decision Attack | ✅ |
| `decision_attacker_binary.py` | Gallop Decision | ✅ |
| `decision_attacker_grouping.py` | Group Decision | ❌ |
| `decision_attacker_grouping_binary.py` | Gallop+Group | ❌ |
| `decision_attacker_rel_scores.py` | Ghost Decision | ❌ |
| `decision_attacker_rel_scores_grouping.py` | Ghost+Group | ❌ |
| `decision_attacker_binary_and_rel_scores.py` | Gallop+Ghost | ❌ |
| `k_of_n_attacker.py` | 표준 K-of-N | ❌ |
| `k_of_n_attacker_binary.py` | Gallop K-of-N | ✅ |
| `char_by_char_amplifier.py` | 문자별 증폭 공격 | ❌ |

---

## 추가해야 할 파일들

### 우선순위 1: 핵심 공격 변형
1. `decision_attacker_grouping_binary.py` - Gallop+Group (최고 성능)
2. `decision_attacker_rel_scores.py` - Ghost 최적화
3. `k_of_n_attacker.py` - 표준 K-of-N (비교용)

### 우선순위 2: 고급 조합
4. `decision_attacker_grouping.py` - Group 기본
5. `decision_attacker_binary_and_rel_scores.py` - Gallop+Ghost
6. `decision_attacker_rel_scores_grouping.py` - Ghost+Group

### 우선순위 3: 추가 공격
7. `char_by_char_amplifier.py` - 문자별 증폭 공격
8. `test_char_by_char_amplifier_binary.py` - 문자별 증폭 테스트

---

## 성능 비교 (예상)

| 공격 유형 | 복잡도 | DB 쿼리 수 | 정확도 | 속도 |
|-----------|--------|-----------|--------|------|
| 표준 DBreach | O(N) | ~10,000 | 기준 | 느림 |
| Gallop (Binary) | O(log N) | ~1,000 | 동일 | 10배 빠름 |
| Group | O(N) | ~1,500 | 동일 | 6배 빠름 |
| Ghost | O(N) | ~2,000 | 약간 낮음 | 5배 빠름 |
| Gallop+Group | O(log N) | ~200 | 동일 | **50배 빠름** |
| Gallop+Ghost | O(log N) | ~300 | 약간 낮음 | 30배 빠름 |

---

## 다음 단계

1. **누락된 파일 복사**
   ```bash
   cp gdbreach-attacks/attack_code/decision_attacker_*.py flask/
   cp gdbreach-attacks/attack_code/k_of_n_attacker.py flask/
   cp gdbreach-attacks/attack_code/char_by_char_amplifier.py flask/
   ```

2. **테스트 스크립트 작성**
   - `test_decision_attack_maria_grouping_binary.py`
   - `test_all_gdbreach_variants.py` (통합 테스트)

3. **벤치마크 스크립트**
   - 모든 변형의 속도/정확도 비교
   - CSV 결과 생성

4. **Docker 재빌드**
   ```bash
   docker-compose build flask
   docker-compose up -d
   ```

---

## 참고 자료

- 원본 논문: G-DBREACH-Attacks: Algorithmic Techniques for Faster and Stronger Compression Side Channels
- 레포지토리: https://github.com/bnbourassa/gdbreach-attacks

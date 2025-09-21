import dbreacher
import string


'''

class decisionAttacker():

    def __init__(self, dbreacher : dbreacher.DBREACHer, guesses,random_guess_len):
        self.n = len(guesses)
        self.guesses = guesses
        self.dbreacher = dbreacher
        self.bytesShrunk = dict()
        self.bYesReferenceScores = dict()
        self.bNoReferenceScores = dict()
        self.random_guess_len = random_guess_len
        
        
        
    
    def setUp(self) -> bool:
        print("setUp start... ")
        success = self.dbreacher.reinsertFillers()
        self.bytesShrunk = dict()
        self.bYesReferenceScores = dict()
        self.bNoReferenceScores = dict()
        return success

    def tryAllGuesses(self, verbose = False) -> bool:
        
        print("tryAllGuesses start...")
        
        for guess in self.guesses:
            print("guess : " , guess)
            
        
            if len(guess) not in self.bYesReferenceScores:
                try:
                    b_yes = self.dbreacher.getSYesReferenceScore(len(guess))
                    b_no = self.dbreacher.getSNoReferenceScore(len(guess), string.ascii_lowercase)
                except RuntimeError:
                    return False
                self.bYesReferenceScores[len(guess)] = b_yes
                self.bNoReferenceScores[len(guess)] = b_no
            
            
            
            
            
            shrunk = self.dbreacher.insertGuessAndCheckIfShrunk(guess)
            
            if shrunk:            # <--- return False 대신에 score = 0으로 찍히게 수정
                # score = 0
                self.dbreacher.bytesShrunkForCurrentGuess = 0
                self.dbreacher.compressibilityScoreReady = True
                # return False  ##score = 0
            while not shrunk:
                shrunk = self.dbreacher.addCompressibleByteAndCheckIfShrunk(guess, 1, self.random_guess_len)
            
            # shrunk = self.dbreacher.insertGuessAndCheckIfShrunk(guess)
            
            # if shrunk:
            #     return False
            
            # shrunk = self.dbreacher.addCompressibleByteAndCheckIfShrunk(guess, 1, self.random_guess_len)
            # if shrunk:
                
            score = self.dbreacher.getBytesShrunkForCurrentGuess()
            if verbose:
                print("\"" + guess + "\" bytesShrunk = " + str(score))
            self.bytesShrunk[guess] = score
        return True

    # returns (b_no, b_guess, b_yes) for each guess, normalized such that min(b_no, b_guess, b_yes) is always zero
    def getGuessAndReferenceScores(self):
        bytesList = [(item[1], item[0]) for item in self.bytesShrunk.items()]
        print("bytesList : ", bytesList)
        guessScoreTuples = []
        for b, g in bytesList:
            bYes = self.bYesReferenceScores[len(g)]
            bNo = self.bNoReferenceScores[len(g)]
            min_b = min(bNo, min(b, bYes))
            guessScoreTuples.append((g, (bNo - min_b, b - min_b, bYes - min_b))) #틀린 경우의 상대 점수, 실제 guess의 상대 점수, 맞은 경우의 상대 점수
        return guessScoreTuples
    
'''





class decisionAttacker():

    def __init__(self, dbreacher : dbreacher.DBREACHer, guesses,random_guess_len):
        self.n = len(guesses)
        self.guesses = guesses
        self.dbreacher = dbreacher
        self.bytesShrunk = dict()
        self.bYesReferenceScores = dict()
        self.bNoReferenceScores = dict()
        self.random_guess_len = random_guess_len
        
        
        
    
    def setUp(self) -> bool:
        success = self.dbreacher.reinsertFillers()
        self.bytesShrunk = dict()
        self.bYesReferenceScores = dict()
        self.bNoReferenceScores = dict()
        return success

    def tryAllGuesses(self, verbose = False) -> bool:
        
        
        for guess in self.guesses:
            
        
            if len(guess) not in self.bYesReferenceScores:
                try:
                    b_yes = self.dbreacher.getSYesReferenceScore(len(guess))
                    b_no = self.dbreacher.getSNoReferenceScore(len(guess), string.ascii_lowercase)
                except RuntimeError:
                    return False
                self.bYesReferenceScores[len(guess)] = b_yes
                self.bNoReferenceScores[len(guess)] = b_no
            
            
            
            
            
            shrunk = self.dbreacher.insertGuessAndCheckIfShrunk(guess)
            
            if shrunk:
                score = 0         # <--- return False 대신에 score = 0으로 찍히게 수정, 아래 두줄 추가
                self.dbreacher.bytesShrunkForCurrentGuess = 0
                
                self.dbreacher.compressibilityScoreReady = True
                # return False  ##score = 0
            while not shrunk:
                shrunk = self.dbreacher.addCompressibleByteAndCheckIfShrunk(guess, 1, self.random_guess_len)
            
            # shrunk = self.dbreacher.insertGuessAndCheckIfShrunk(guess)
            
            # if shrunk:
            #     return False
            
            # shrunk = self.dbreacher.addCompressibleByteAndCheckIfShrunk(guess, 1, self.random_guess_len)
            # if shrunk:
                
            # else:
                
                
                
                
            # if self.dbreacher.getBytesShrunkForCurrentGuess() == 100:
            #     print("tryallGuess guess_len : ", self.random_guess_len)
            #     shrunk = False
            #     while not shrunk:
            #         shrunk = self.dbreacher.addCompressibleByteAndCheckIfShrunk(guess, 1, self.min_guess_len)
            # if self.dbreacher.getBytesShrunkForCurrentGuess() == 200:
            #     print("tryallGuess guess_len : ", self.random_guess_len)
            #     shrunk = False
            #     while not shrunk:
            #         shrunk = self.dbreacher.addCompressibleByteAndCheckIfShrunk(guess, 1, self.min_guess_len)
            score = self.dbreacher.getBytesShrunkForCurrentGuess()
            if verbose:
                print("\"" + guess + "\" bytesShrunk = " + str(score))
            self.bytesShrunk[guess] = score
        return True

    # returns (b_no, b_guess, b_yes) for each guess, normalized such that min(b_no, b_guess, b_yes) is always zero
    def getGuessAndReferenceScores(self):
        bytesList = [(item[1], item[0]) for item in self.bytesShrunk.items()]
        guessScoreTuples = []
        for b, g in bytesList:
            bYes = self.bYesReferenceScores[len(g)]
            bNo = self.bNoReferenceScores[len(g)]
            min_b = min(bNo, min(b, bYes))
            guessScoreTuples.append((g, (bNo - min_b, b - min_b, bYes - min_b))) #틀린 경우의 상대 점수, 실제 guess의 상대 점수, 맞은 경우의 상대 점수
        return guessScoreTuples
        
        
        # '''
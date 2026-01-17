import os
import statistics
from pathlib import Path
import csv

DATASETS = ["random"]
# DATASETS = ["random", "english", "emails"]

# RUN_PLAN = {
#     100:  [100, 300, 500, 1000],
#     300:  [300, 500, 1000],
#     500:  [500, 1000],
#     1000: [1000],
# }

RUN_PLAN = {
    100:  [100],
    
}

def yield_runs(datasets=None, run_plan=None):
    ds = datasets or DATASETS
    rp = run_plan or RUN_PLAN
    for dataset in ds:
        for compressible_byte, rand_list in rp.items():
            for random_byte in rand_list:
                yield (dataset, compressible_byte, random_byte)



for dataset, compressible_byte, random_byte in yield_runs():
    # 1) 공격 실행 → CSV로 저장
    cmd1 = (
        f"python3 ./test_decision_attack_maria_binary.py "
        f"--dataset {dataset} "
        f"--Compressible_bytes {compressible_byte} "
        f"--Random_bytes {random_byte} "
        # f"> {out_csv}"
    )
    os.system(cmd1)
    RESULT_DIR = Path("01010") / f"{dataset}_{compressible_byte}_{random_byte}"
    
    accuracies = []
    attack_times = []
    setup_times = []
    
    # for i in range(1): 
    for i in range(0, 10) :
        out_csv = RESULT_DIR / f"trial_{i}.csv"
        out_txt = RESULT_DIR / f"threshold_{i}.txt"
        cmd2 = f"python3 ./find_optimal_threshold.py {out_csv} > {out_txt}"
        os.system(cmd2)

    ###
        with open(out_csv, newline='') as f_csv:
                reader = csv.reader(f_csv)
                lines = list(reader)
                if len(lines) >= 2:
                    second_line = lines[1]
                    try:
                        setup_val = float(second_line[-2])   # 오른쪽에서 두 번째
                        attack_val = float(second_line[-1]) * 100  # 맨 오른쪽
                        setup_times.append(setup_val)
                        attack_times.append(attack_val)
                    except ValueError:
                        pass

            # threshold_i.txt에도 기록 (append 모드)
        if setup_times and attack_times:
            with open(out_txt, "a") as f_txt:
                f_txt.write(f"setup={setup_times[-1]}, attack={attack_times[-1]}, total={setup_times[-1]+attack_times[-1]}\n")
    ###
    
        
        
        
        
        
    with open(out_txt) as f:
        for line in f:
            if "maximum accuracy achieved:" in line:
                acc = float(line.strip().split(":")[-1])
                accuracies.append(acc)
            elif "Total attack time:" in line:
                time_str = line.strip().split(":")[-1].strip()
                try:
                    t = float(time_str)
                    attack_times.append(t)
                except ValueError:
                    pass
    
    
    if accuracies:
        avg_acc = statistics.mean(accuracies)
        avg_setup = statistics.mean(setup_times) if setup_times else None
        avg_attack = statistics.mean(attack_times) if attack_times else None
        total_time = (avg_setup + avg_attack) if (avg_setup is not None and avg_attack is not None) else None

        avg_file = RESULT_DIR / "avg_accuracy.txt"
        with open(avg_file, "w") as f:
            f.write("=== Trial-wise Results ===\n")
            for idx, (s, a) in enumerate(zip(setup_times, attack_times)):
                f.write(f"Trial {idx}: Setup={s}, Attack={a}, Total={s+a}\n")

            f.write("\n=== Averages ===\n")
            f.write(f"Average accuracy over 10 trials : {avg_acc}\n")
            # if avg_setup is not None:
            #     f.write(f"Average setup time over 10 trials : {avg_setup}\n")
            # if avg_attack is not None:
            #     f.write(f"Average attack time over 10 trials : {avg_attack}\n")
            # if total_time is not None:
            #     f.write(f"Total attack time (setup+attack) : {total_time}\n")
    
    
    
    # if accuracies:
    #     avg_acc = statistics.mean(accuracies)
    #     avg_time = statistics.mean(attack_times) if attack_times else None

    #     avg_file = RESULT_DIR / "avg_accuracy.txt"
    #     with open(avg_file, "w") as f:
    #         f.write(f"Average accuracy over 10 trials : {avg_acc}\n")
    #         if avg_time is not None:
    #             f.write(f"Average attack time over 10 trials : {avg_time}\n")
    


















'''
import os

DATASETS = ["random", "english", "emails"]

RUN_PLAN = {
    100:  [100, 300, 500, 1000],
    300:  [300, 500, 1000],
    500:  [500, 1000],
    1000: [1000],
}

def yield_runs(datasets=None, run_plan=None):
    ds = datasets or DATASETS
    rp = run_plan or RUN_PLAN
    for dataset in ds:
        for compressible_byte, rand_list in rp.items():
            for random_byte in rand_list:
                yield (dataset, compressible_byte, random_byte)





arg1 = "test_decision_attack_maria_binary_result.csv"
cmd = "python3 ./test_decision_attack_maria_binary.py" + "" + arg1
os.system(cmd)

            
                
# cmd2 = "python3 ./find_optimal_threshold.py" + arg2
# os.system(cmd2)

# 10번 실행한 결과값의 평균 print
# max accuracy
# setup Time

'''
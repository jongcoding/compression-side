import os
import statistics
from pathlib import Path
import csv
import shutil

DATASETS = ["random"]

RUN_PLAN = {
    100: [100],
}

def yield_runs(datasets=None, run_plan=None):
    ds = datasets or DATASETS
    rp = run_plan or RUN_PLAN
    for dataset in ds:
        for compressible_byte, rand_list in rp.items():
            for random_byte in rand_list:
                yield (dataset, compressible_byte, random_byte)

def file_exists(p: Path) -> bool:
    try:
        return p.exists()
    except Exception:
        return False

for dataset, compressible_byte, random_byte in yield_runs():
    # 1) 공격 실행
    cmd1 = (
        f"python3 ./test_decision_attack_maria_binary.py "
        f"--dataset {dataset} "
        f"--Compressible_bytes {compressible_byte} "
        f"--Random_bytes {random_byte} "
    )
    os.system(cmd1)

    RESULT_DIR = Path("01010") / f"{dataset}_{compressible_byte}_{random_byte}"
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    accuracies = []
    attack_times = []
    setup_times = []

    has_threshold = shutil.which("python3") is not None and Path("./find_optimal_threshold.py").exists()

    for i in range(10):
        out_csv = RESULT_DIR / f"trial_{i}.csv"
        out_txt = RESULT_DIR / f"threshold_{i}.txt"

        # threshold 계산 스크립트가 있으면 실행
        if has_threshold and file_exists(out_csv):
            cmd2 = f"python3 ./find_optimal_threshold.py {out_csv} > {out_txt}"
            os.system(cmd2)
        else:
            # 없으면 빈 파일이라도 만들어 둠(후단 파서가 존재를 가정)
            if not file_exists(out_txt):
                out_txt.write_text("")

        # CSV에서 setup/attack 추출(있을 때만)
        if file_exists(out_csv):
            with open(out_csv, newline='') as f_csv:
                reader = csv.reader(f_csv)
                lines = list(reader)
                if len(lines) >= 2:
                    second = lines[1]
                    try:
                        setup_val = float(second[-2])
                        attack_val = float(second[-1]) * 100.0
                        setup_times.append(setup_val)
                        attack_times.append(attack_val)
                    except Exception:
                        pass

            # threshold_i.txt에도 기록 (append)
            if setup_times and attack_times:
                with open(out_txt, "a") as f_txt:
                    f_txt.write(f"setup={setup_times[-1]}, attack={attack_times[-1]}, total={setup_times[-1]+attack_times[-1]}\n")
        else:
            print(f"[WARN] missing CSV: {out_csv}; skip trial {i}")

    # threshold 출력에서 accuracy/시간 파싱 (존재할 때만)
    for i in range(10):
        out_txt = RESULT_DIR / f"threshold_{i}.txt"
        if not file_exists(out_txt):
            continue
        with open(out_txt, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if "maximum accuracy achieved:" in line:
                    try:
                        accuracies.append(float(line.strip().split(":")[-1]))
                    except Exception:
                        pass
                elif "Total attack time:" in line:
                    try:
                        attack_times.append(float(line.strip().split(":")[-1].strip()))
                    except Exception:
                        pass

    if accuracies:
        avg_acc = statistics.mean(accuracies)
        avg_setup = statistics.mean(setup_times) if setup_times else None
        avg_attack = statistics.mean(attack_times) if attack_times else None

        avg_file = RESULT_DIR / "avg_accuracy.txt"
        with open(avg_file, "w") as f:
            f.write("=== Trial-wise Results ===\n")
            for idx, (s, a) in enumerate(zip(setup_times, attack_times)):
                f.write(f"Trial {idx}: Setup={s}, Attack={a}, Total={s+a}\n")

            f.write("\n=== Averages ===\n")
            f.write(f"Average accuracy over 10 trials : {avg_acc}\n")
            if avg_setup is not None:
                f.write(f"Average setup time over 10 trials : {avg_setup}\n")
            if avg_attack is not None:
                f.write(f"Average attack time over 10 trials : {avg_attack}\n")
    else:
        print(f"[WARN] no accuracies parsed for {RESULT_DIR}")

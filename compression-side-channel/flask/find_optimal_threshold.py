import csv
import numpy as np
import matplotlib.pyplot as plt
import sys
import statistics 

text_type = sys.argv[1]

def is_float(x):
    try:
        float(x)
        return True
    except:
        return False

def is_int_like_zero_or_one(x):
    # '0', '1', '0.0', '1.0' 모두 처리
    try:
        v = float(x)
        return v == 0.0 or v == 1.0
    except:
        return False

fig, ax = plt.subplots()
ax.set(xlabel="threshold", ylabel="accuracy", title="Accuracy of decision attack for different threshold values")
for c in ["snappy", "zlib", "lz4"]:
    true_labels = []
    ref_scores = []
    setup_times = []
    guess_times = []
    total_time = 0
    with open(text_type) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if not row:
                continue
            # "Total time running in seconds:" 처리
            if isinstance(row[0], str) and "Total time running in seconds:" in row[0]:
                # 고정 포맷 가정: 메세지 뒤에 숫자가 이어짐
                total_time = row[0].split("Total time running in seconds:")[-1].strip()
                continue

            # 헤더(예: "true_label,num_secrets,...")나 "Total"로 시작하는 행은 스킵
            if (isinstance(row[0], str) and "Total" in row[0]) or (row[0].strip().lower() == "true_label"):
                continue

            # 데이터 행만 처리: 라벨이 0/1(소수형 포함)인 경우
            if is_int_like_zero_or_one(row[0]):
                # 라벨 파싱 (0.0/1.0 대응)
                tl = int(round(float(row[0])))
                true_labels.append(tl)

                # ref_scores: (b_no, b_guess, b_yes) 모두 float 허용
                # b_no가 0이면 1로 보정
                # 인덱스 존재 여부와 숫자 여부를 확인
                b_no = 1.0
                b_guess = 0.0
                b_yes = 0.0
                if len(row) > 2 and is_float(row[2]):
                    b_no = float(row[2])
                if b_no == 0.0:
                    b_no = 1.0
                if len(row) > 3 and is_float(row[3]):
                    b_guess = float(row[3])
                if len(row) > 4 and is_float(row[4]):
                    b_yes = float(row[4])

                ref_scores.append((b_no, b_guess, b_yes))

                # ↓↓↓ 헤더(문자열) 때문에 깨지던 부분을 데이터 행 안으로 이동
                if len(row) > 5 and is_float(row[5]):
                    setup_times.append(float(row[5]))
                if len(row) > 6 and is_float(row[6]):
                    guess_times.append(float(row[6]))
            else:
                # 라벨이 숫자가 아니면 패스
                pass
                #print(row)

    # print(true_labels)
    true_labels = np.array(true_labels, dtype=int)
    # pcts 계산 시 b_no가 0이 되지 않도록 위에서 보정함
    pcts = [1 - (b - b_yes) / b_no for b_no, b, b_yes in ref_scores] if ref_scores else []

    # print(len(pcts))

    # thresholds = np.arange(0.600, 0.600, 0.001)
    threshold = 0.65
    # thresholds = np.arange( 0.693, 0.694, 0.001)
    # thresholds = np.arange( 0.667, 0.668, 0.001)
    accuracies = []

    if pcts and len(true_labels) == len(pcts):
        labels = np.array([1 if pct >= threshold else 0 for pct in pcts], dtype=int)
        accuracy = 1 - np.sum(np.abs(labels - true_labels)) / labels.shape[0]
    else:
        # 데이터가 없거나 길이가 안 맞는 경우 안전 처리
        accuracy = 0.0

    accuracies.append(accuracy)
    # for threshold in thresholds:
    #     labels = np.array([pct >= threshold for pct in pcts])
    #     accuracy = 1 - np.sum(np.abs(labels - true_labels)) / labels.shape[0]
    #     accuracies.append(accuracy)

    # ax.plot(thresholds, accuracies, label=c)
    
    # f = open(text_type + "_" + c+"_threshold_data.csv", "w")
    # f.write("threshold,accuracy\n")
    # for i in range(0,len(thresholds)):
    # 	f.write(str(thresholds[i])+","+str(accuracies[i]) + "\n")
    # f.close()
    # print(c + " thresholds: "+ str(thresholds))
    # print(c + " accuracies: "+ str(accuracies))

    maximum_accuracy = float(np.max(accuracies)) if accuracies else 0.0
    # maximum_threshold = -0.5 + 0.001*np.argmax(accuracies)
    print(c + ": maximum accuracy achieved: " + str(maximum_accuracy))
    # print(c + ": maximum accuracy threshold: " + str(maximum_threshold))

    # print("Average setup time: ", statistics.mean(setup_times))
    # print("Average guess time: ", statistics.mean(guess_times))
    print("Total attack time: " + str(total_time))

    '''
    print("Errors:")
    labels = np.array([pct >= maximum_threshold for pct in pcts])
    for idx, label in enumerate(labels):
        l = 1 if label else 0
        if l != true_labels[idx]:
            print(str(idx) + "," + str(true_labels[idx]) + ": " + str(pcts[idx]))
    '''

# plt.legend()
# ax.grid()
# plt.savefig("threshold-accuracy.png")
# plt.show()

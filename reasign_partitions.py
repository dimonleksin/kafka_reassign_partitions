import subprocess
import os
import time
import sys
import random

port = str(input("Введи порт кафки для ноды, на которой запускаешь скрипт: "))
action = str(input("Что сделать?[move/return/rebalance]: "))

brokers = {
    "broker_1": 0,
    "broker_2": 0,
    "broker_3": 0,
    "broker_4": 0,
    "broker_5": 0
}

brokers_reasign = {
    "broker_1": 0,
    "broker_2": 0,
    "broker_3": 0,
    "broker_4": 0,
    "broker_5": 0
}

brokers_after_reasign = {
    "broker_1": 0,
    "broker_2": 0,
    "broker_3": 0,
    "broker_4": 0,
    "broker_5": 0
}

def generate_for_all_rebalance():
    topic_list = subprocess.check_output([
        "/u/kafka/server/bin/kafka-topics.sh",
        "--bootstrap-server",
        f"$(uname -n):{port}",
        "--command-config",
        "auth.properties.admin",
        "--describe"]).decode("utf-8")
    topics_raw = topic_list.split('\n')

    topics = []

    topics_with_partitions = [[f"{topic.split()[1]}:{topic.split()[3]}", topic.split()[7].split(sep=",")] for topic in topics_raw if "\tReplicas:" in topic]
    lt = len(topics_with_partitions)
    # Counting number of topics by bootstrap server
    for t in topics_with_partitions:
        for i in t[1]:
            brokers[f"broker_{i}"] += 1
    print(brokers)
    average = (int(brokers["broker_1"]) + int(brokers["broker_2"]) + int(brokers["broker_3"]) + int(brokers["broker_4"]) + int(brokers["broker_5"])) // 5

    c = 0
    for k, v in brokers.items():
        # Если число положительное, значит топиков слишком много, если отрицательное, слишком мало
        brokers_reasign[k] = int(v) - average
    print(brokers_reasign)
    flg = True
    c = 0
    for t in topics_with_partitions:
        for k2, v2 in brokers.items():
            if int(v2) > 0:
                for k, v in brokers_reasign.items():
                    if k == k2:
                        continue
                    if int(v) < 0:
                        # if len(t[1]) != 3:
                        #     continue
                        if k[-1] not in t[1]:
                            l = len(topics_with_partitions[c][1])
                            index = random.randrange(0, l)
                            topics_with_partitions[c][1][index] = k[-1]
                            brokers[k2] -= 1
                            brokers_reasign[k] += 1
                        
                        if brokers_reasign[k] == 0:
                            print("next step")
                            break
                        elif brokers[k2] < average:
                            print("stop")
                            flg = False
                            break
                        print("\r", c, end="")
            if not flg:
                break
        c += 1
    for t in topics_with_partitions:
        for i in t[1]:
            brokers_after_reasign[f"broker_{i}"] += 1
    print()
    print(brokers_after_reasign)

    # Generate json
    result_json = {
        "version":1,
        "partitions":[]
    }
    partitions = []

    notif = 1000
    for i in range(0, lt, notif):
        for topic_data in topics_with_partitions:
            topic = topic_data[0].split(sep=":")
            partitions.append(
                {
                    "topic": topic[0],
                    "partition": topic[1],
                    "replicas": topic_data[1]
                }
            )
        if len(partitions) > lt:
            strings = str({"version":1, "partitions": partitions[i:i+lt]})
        else:
            strings = str({"topics": partitions[i:], "version": 1})
        with open("file{i}_reasigned_new.json", "w") as f:
            f.write(
                strings
            )
        


# {"version":1,
#   "partitions":[{"topic":"foo1","partition":2,"replicas":[5,4,6]},
#                 {"topic":"foo1","partition":0,"replicas":[4,5,6]},
#                 {"topic":"foo2","partition":2,"replicas":[6,4,5]},
#                 {"topic":"foo2","partition":0,"replicas":[4,5,6]},
#                 {"topic":"foo1","partition":1,"replicas":[5,4,6]},
#                 {"topic":"foo2","partition":1,"replicas":[4,5,6]}]
#   }


def generate_json_for_reasign():
    broker_list = str(input("Введи список брокеров, НА которые хочешь перенести партиции: "))
    broker_remove = str(input("Введи номер брокера, с которого переезжаем: "))

    topic_list = subprocess.check_output([
        "/u/kafka/server/bin/kafka-topics.sh",
        "--bootstrap-server",
        f"$(uname -n):{port}",
        "--command-config",
        "auth.properties.admin",
        "--describe"]).decode("utf-8")
    topics_raw = topic_list.split('\n')

    topics_with_partitions = [topic.split() for topic in topics_raw if "\tReplicas:" in topic]
    topics_with_dublicates = [topic[1] for topic in topics_with_partitions if broker_remove in topic[5] or broker_remove in str(topic[7])]
    # print(topics_with_partitions)
    # set() - unique records filtering function from list
    topics = list(set(topics_with_dublicates))

    # print(topics)



    topics_lens = len(topics)
    dict_topics = []
    top = {}
    counter = 0
    number_of_topics = 400
    for i in range(0, topics_lens):
        top = {"topic": topics[i]}
        dict_topics.append(top)
    print(len(dict_topics))
    for i in range(0, topics_lens, number_of_topics):
        if len(dict_topics) > number_of_topics:
            strings = str({"topics": dict_topics[i:i+number_of_topics], "version": 1})
        else:
            strings = str({"topics": dict_topics[i:], "version": 1})

        new_str = strings.replace("'", "\"")
        with open(f"file{counter}.json", "a+") as f:
            f.write(new_str)
        counter += 1

    for i in range(0, counter):
        try:
            result = subprocess.check_output([
                "kafka-reassign-partitions.sh",
                "--bootstrap-server",
                f"$(uname -n):{port}",
                "--command-config",
                "auth.properties.admin",
                "--topics-to-move-json-file",
                f"file{i}.json",
                "--generate",
                "--broker-list",
                f"{broker_list}"], stderr=sys.stderr).decode("utf-8")
            pwd = os.getenv("PWD")
            number_of_files = [i for i in os.listdir(pwd) if "reasigned" in i]
            pr = f"Проход {i}. Сгенерировал файлов {len(number_of_files)}"
            print("\r" + pr, end='')

            with open(f"file{i}_reasigned.json", "w") as fr:
                fr.write(result)
            with open(f"file{i}_reasigned_new.json", "w") as frn:
                a = str(result).split('\n')[-2]
                frn.write(str(a))
            with open(f"file{i}_reasigned_return.json", "w") as frn:
                frn.write(str(result).split('\n')[1])

        except:
            print("Не смог сгенерить")
            break

def ret():
    current_dir = os.getenv("PWD")
    list_files = [file for file in os.listdir(current_dir) if "reasigned_return" in file]
    for i in list_files:
        print(f"Обрабатываю файл '{i}'.")
        subprocess.check_output([
            "kafka-reassign-partitions.sh",
            "--bootstrap-server",
            f"$(uname -n):{port}",
            "--command-config",
            "auth.properties.admin",
            "--reassignment-json-file",
            i,
            "--execute"], stderr=sys.stderr)
        bo = True
        
        while bo:
            result = subprocess.check_output([
            "kafka-reassign-partitions.sh",
            "--bootstrap-server",
            f"$(uname -n):{port}",
            "--command-config",
            "auth.properties.admin",
            "--reassignment-json-file",
            i,
            "--verify"], stderr=sys.stderr).decode("utf-8").split('\n')
            topic_count = len(result[1:-4])
            complete = 0
            error = 0
            error_topics = []
            for r in result:
                s = r.split()
                # if "progress" in r:
                #     print(f"Топик {s[3]} процессе")
                if "failed" in r:
                    print(f"Топик {s[3]} ОШИБКА!!")
                    error_topics.append(s[3])
                    error += 1
                elif "complete" in r:
                    # print(f"Топик {s[3]} готов")
                    complete += 1
            text = f"Всего готово топиков {complete} из {topic_count}."
            print ("\r" + text + "        ", end='')
            if topic_count <= complete and error == 0:
                print("Все топики перемещены))!")
                bo = False
            elif error != 0 and error + complete == topic_count:
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print(error_topics)
                print("Возникола ошибка")
                bo = False

def move():
    cuestions = input("\nВсе фалы сформированны. Начать перенос?[y/n]:")
    if cuestions == "y":
        files = [f for f in os.listdir(str(os.getenv("PWD"))) if "reasigned_new.json" in f]
        for i in files:
            # print(f"Обрабатываю файл 'file{i}_reasigned_new.json'.")
            subprocess.check_output([
                "kafka-reassign-partitions.sh",
                "--bootstrap-server",
                f"$(uname -n):{port}",
                "--command-config",
                "auth.properties.admin",
                "--reassignment-json-file",
                f"{i}",
                "--execute"], stderr=sys.stderr)
            bo = True

            while bo:
                result = subprocess.check_output([
                "kafka-reassign-partitions.sh",
                "--bootstrap-server",
                f"$(uname -n):{port}",
                "--command-config",
                "auth.properties.admin",
                "--reassignment-json-file",
                f"{i}",
                "--verify"], stderr=sys.stderr).decode("utf-8").split('\n')
                topic_count = len(result[1:-4])
                complete = 0
                error = 0
                error_topics = []
                for r in result:
                    s = r.split()
                    # if "progress" in r:
                        # print(f"Топик {s[3]} процессе")
                    if "failed" in r:
                        print(f"Топик {s[3]} ОШИБКА!!")
                        error_topics.append(s[3])
                        error += 1
                    elif "complete" in r:
                        # print(f"Топик {s[3]} готов")
                        complete += 1
                text = f"Всего готово топиков {complete} из {topic_count}.\nВ данный момент обрабатываю файл {i}"
                print ("\r" + text + "        ", end='')
                if topic_count <= complete and error == 0:
                    print("\r Все топики перемещены))!", end='')
                    bo = False
                elif error != 0 and error + complete == topic_count:
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    print(error_topics)
                    print("Возникола ошибка с переносом топиков")
                    bo = False
                time.sleep(1)

def all_rebalence():
    pass

if "move" in action:
    generate_json_for_reasign()
    move()

elif "return" in action:
    ret()

elif "rebalance" in action:
    generate_for_all_rebalance()
    move()

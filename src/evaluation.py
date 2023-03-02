import time

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
import queue
import random
import os

database_path = "./iotdb/"

dataset_setting = {
    "WC": {  # multi row
        "row_num": 10000000,
        "col_num": 8,
        "null_rate": 0.1958,
    },
    "CSSC": {  # multi col
        "row_num": 639770,
        "col_num": 48,
        "null_rate": 0.0622,
    },
    "WH": {
        "row_num": 1600000,
        "col_num": 8,
        "null_rate": 0.0443,
    },
    "Campus": {
        "row_num": 1000000,
        "col_num": 10,
        "null_rate": 0.1581,
    },
}

device_id = "root.sg_al_01.d1"
file_idx = 1


# check every time file_idx increment
def check_lag_queue(session, q):
    if q.empty():
        return

    global file_idx

    check_lag = q.get()
    while check_lag[0] <= file_idx:
        input_arr = check_lag[1]
        device_in = [device_id] * len(input_arr[0])
        # lag input
        session.insert_aligned_records(
            device_in, input_arr[0],
            input_arr[1], input_arr[2], input_arr[3]
        )
        session.execute_non_query_statement(
            "flush"
        )
        file_idx += 1

        if q.empty():
            return
        check_lag = q.get()
    q.put(check_lag)


def runDataset_aligned(session, dataset, row_num, col_num, update_rate, delay_rate, lag_dist_max, lag_num_max, seed):
    # setting
    global file_idx
    total_row = row_num if dataset == "" else dataset_setting[dataset]["row_num"]
    block_row = total_row // 150  # 150
    file_col = col_num if dataset == "" else dataset_setting[dataset]["col_num"]
    null_rate = 0.0 if dataset == "" else dataset_setting[dataset]["null_rate"]
    random.seed(seed)

    # create ts
    measurement = [str(i) + "1" for i in range(1, file_col + 1)]
    compression = [Compressor.UNCOMPRESSED] * len(measurement)
    data_type = [TSDataType.DOUBLE] * len(measurement)
    encoding = [TSEncoding.PLAIN] * len(measurement)
    session.create_aligned_time_series(
        device_id, measurement, data_type, encoding, compression
    )

    timestamps_list_ = [int(i * 1e4) + random.randint(0, int(1e4)) for i in range(total_row)]  # timestamps

    session.insert_aligned_record(  # insert index 0
        device_id, int(total_row * 1e4 * 10),
        [measurement[0]], [data_type[0]], [random.random() * 100]
    )
    session.execute_non_query_statement(
        "flush"
    )
    q = queue.PriorityQueue()

    for block_idx in range(total_row // block_row):
        if block_idx % 50 == 0:
            print(block_idx, "/", total_row // block_row)
        input_arr = [[], [], [], []]  # input this file

        # lag_num
        lag_num = random.randint(1, lag_num_max)
        segment_base = block_row // lag_num

        for segment_idx in range(lag_num):
            update_choice = random.randint(0, 1)  # 0: delay, 1: update

            lag_arr = [[], [], [], []]  # input latter

            # block range
            segment_min = segment_idx * segment_base
            segment_max = (segment_idx + 1) * segment_base

            if update_choice == 1:
                rate = update_rate * 2
            else:
                rate = delay_rate * 2

            selected_cols = random.randint(int(rate * float(file_col)) + 1, file_col + 1)
            if selected_cols > file_col:
                selected_cols = file_col
            selected_rows = int(float(file_col * segment_base) * rate / float(selected_cols))
            if selected_rows == 0:
                selected_rows = 1

            # lag range
            lag_row_min = random.randint(segment_min, segment_min + segment_base - selected_rows)
            lag_row_max = lag_row_min + selected_rows
            lag_col_min = random.randint(0, file_col - selected_cols)
            lag_col_max = lag_col_min + selected_cols

            for row in range(segment_min, segment_max):
                for col in range(file_col):
                    # null rate
                    if random.random() < null_rate:
                        continue
                    value = random.random() * 1000
                    if lag_row_min <= row <= lag_row_max and lag_col_min <= col <= lag_col_max:
                        lag_arr[0].append(timestamps_list_[block_idx * block_row + row])
                        lag_arr[1].append([measurement[col]])
                        lag_arr[2].append([data_type[col]])
                        lag_arr[3].append([value])
                        if update_choice == 1:  # update (remain in this input)
                            updated_value = random.random() * 1000  # updated value
                            input_arr[0].append(timestamps_list_[block_idx * block_row + row])
                            input_arr[1].append([measurement[col]])
                            input_arr[2].append([data_type[col]])
                            input_arr[3].append([updated_value])
                    else:
                        input_arr[0].append(timestamps_list_[block_idx * block_row + row])
                        input_arr[1].append([measurement[col]])
                        input_arr[2].append([data_type[col]])
                        input_arr[3].append([value])

            # lag distance
            lag_dist = random.randint(1, lag_dist_max)
            lag_file_num = lag_dist + file_idx
            q.put([lag_file_num, lag_arr])

        # this file input
        device_in = [device_id] * len(input_arr[0])
        session.insert_aligned_records(
            device_in, input_arr[0],
            input_arr[1], input_arr[2], input_arr[3]
        )
        session.execute_non_query_statement(
            "flush"
        )
        file_idx += 1

        check_lag_queue(session, q)

    # clear lag priority queue
    while not q.empty():
        # if q.qsize() % 5 == 0:
        #     print(q.qsize())
        input_arr = q.get()[1]
        device_in = [device_id] * len(input_arr[0])

        session.insert_aligned_records(
            device_in, input_arr[0],
            input_arr[1], input_arr[2], input_arr[3]
        )
        session.execute_non_query_statement(
            "flush"
        )


def main():
    # parameters
    SAVE_PATH = "./res/exp-6.2.6-delay-rate.txt"
    dataset = "WC"  # WC CSSC WH Campus
    row_num = 639770  # if dataset=""
    col_num = 8  # if dataset=""
    lag_dist_max = 30  # randint(1, lag_dist)
    lag_num_max = 5  # randint(1, lag_num) split evenly
    seed = 666
    update_rate = 0.25
    delay_rate = 0.25

    for dataset in ["WH", "Campus", "WC", "CSSC"]:
        row_num = dataset_setting[dataset]["row_num"]
        col_num = dataset_setting[dataset]["col_num"]
        f = open(SAVE_PATH, "a")
        f.write(dataset + "\n")
        f.close()
        print("dataset:", dataset)
        for method in ["MCC", "round", "oldest"]:
            f = open(SAVE_PATH, "a")
            f.write(method + "\n")
            f.close()
            print("method:", method)
            # 1-modify iotdb-engine.properties
            f = open(os.path.join(database_path, r"conf/iotdb-engine.properties"), "r")
            lines = f.readlines()
            for idx in range(len(lines)):
                lines[idx] = lines[idx].strip()
                if "compaction_select_file_method" in lines[idx]:
                    line_ls = lines[idx].split("=")
                    line_ls[-1] = method
                    lines[idx] = "=".join(line_ls)
            write_text = "\n".join(lines)
            f.close()

            f = open(os.path.join(database_path, r"conf/iotdb-engine.properties"), "w")
            f.write(write_text + "\n")
            f.close()

            # 2-start iotdb
            start_sh_path = os.path.join(database_path, "sbin/start-server.sh")
            os.system(f"nohup {start_sh_path} &")
            time.sleep(10)
            print("successfully start iotdb", method)

            ip = "127.0.0.1"
            port_ = "6667"
            username_ = "root"
            password_ = "root"
            session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
            session.open(False)

            # 3-input
            session.delete_storage_group("root.sg_al_01")
            time.sleep(5)

            runDataset_aligned(
                session=session,
                dataset=dataset,
                row_num=row_num,
                col_num=col_num,
                update_rate=update_rate,
                delay_rate=delay_rate,
                lag_dist_max=lag_dist_max,
                lag_num_max=lag_num_max,
                seed=seed
            )
            time.sleep(10)

            path = os.path.join(database_path, r"data/data/unsequence/root.sg_al_01/0/0/")
            record_size = 0.0
            for file in os.listdir(path):
                if ".resource" not in file:
                    record_size += os.path.getsize(os.path.join(path, file))

            # write
            f = open(SAVE_PATH, "a")
            f.write(str(record_size) + ", ")
            f.close()
            print("lag_dist_max:", lag_dist_max, "|  file_size:", record_size)

            f = open(SAVE_PATH, "a")
            f.write("\n\n")
            f.close()

            # 4-close iotdb
            os.system(os.path.join(database_path, r"sbin/stop-server.sh") + ">> NUL")
            time.sleep(5)
            print("successfully stop iotdb", method)


if __name__ == "__main__":
    main()

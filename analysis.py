import numpy as np
import sys
import os
import json

RESULTS_DIR_NAME = "results/"
RESULTS_SUBDIR_NAME = RESULTS_DIR_NAME + "sim_{}/"
THREAD_RUN_FORMAT = "{}_t{}"
CPU_FILE_NAME = "cpu_usage.csv"
TASK_FILE_NAME = "task_times.csv"
META_FILE_NAME = "meta.json"
STATS_FILE_NAME = "stats.json"
CSV_HEADER = "Run ID,Cores,Sim Duration,Average Task Duration,Load,CPU Load,Task Load,Work Steal Load," \
             "95% Tail Latency,99.9% Tail Latency,Median Latency,99% Tail Latency,Tasks Stolen,Average Number of Steals,Throughput," \
             "Real Load, Parks Per Second,Successful Work Steal Time,Unsuccessful Work Steal Time,Non Work Conserving Time,Allocation Time," \
             "Task Time,Distracted Time,Unpaired Time,Paired Time,Average Requeue Wait Time,Flag Task Time,Avg Time From Alloc to Task," \
             "Avg Steals Per Task,Flag Response Rate,Flag Rate,Tasks Flag Stolen,Average Steals Per Flag,Avg Core Flag Wait Time," \
             "Avg Task Flag Wait Time,Avg Queueing Time,Avg High Latency Task Flag Wait Time,Avg Flag Set Delay Time,Avg High Latency Flag Set Delay Time," \
             "Avg Flagged Task Service Time,Avg Flagged Task Time Left,Pct Flagged Queues Non-empty,Description"


def analyze_sim_run(run_name, output_file, print_results=False, time_dropped=0):
    cpu_file = open(RESULTS_SUBDIR_NAME.format(run_name) + CPU_FILE_NAME, "r")
    task_file = open(RESULTS_SUBDIR_NAME.format(run_name) + TASK_FILE_NAME, "r")
    meta_file = open(RESULTS_SUBDIR_NAME.format(run_name) + META_FILE_NAME, "r")
    stats_file = open(RESULTS_SUBDIR_NAME.format(run_name) + STATS_FILE_NAME, "r")

    meta_data = json.load(meta_file)
    stats = json.load(stats_file)

    meta_file.close()
    stats_file.close()

    # CPU Stats
    busy_time = 0
    task_time = 0
    work_steal_time = 0
    work_search_spin_time = 0
    enqueue_time = 0
    requeue_time = 0
    successful_ws_time = 0
    unsuccessful_ws_time = 0
    allocation_time = 0
    non_work_conserving_time = 0
    distracted_time = 0
    unpaired_time = 0
    paired_time = 0
    flag_task_time = 0
    flag_wait_time = 0

    next(cpu_file) # skip first line
    for line in cpu_file:
        data = line.strip().split(",")
        busy_time += int(data[1])
        task_time += int(data[2])
        work_steal_time += int(data[3])
        work_search_spin_time += int(data[4])
        enqueue_time += int(data[5])
        requeue_time += int(data[6])
        successful_ws_time += int(data[7])
        unsuccessful_ws_time += int(data[8])
        allocation_time += int(data[9])
        non_work_conserving_time += int(data[10])
        distracted_time += int(data[11])
        unpaired_time += int(data[12])
        paired_time += int(data[13])
        if len(data) > 14:
            flag_task_time += int(data[14])
            flag_wait_time += int(data[15])

    cpu_file.close()

    cores = meta_data["num_threads"]
    avg_load = (busy_time/(cores * stats["End Time"]))
    avg_ws_load = (work_steal_time/(cores * stats["End Time"]))
    avg_task_load = (task_time/(task_time + work_steal_time + work_search_spin_time + allocation_time + enqueue_time + requeue_time + flag_task_time))
    avg_core_flag_wait_time = flag_wait_time / (cores * stats["End Time"])

    # Task Stats
    task_latencies = []
    flagged_task_service_times = []
    flagged_task_time_left = []

    complete_tasks = stats["Completed Tasks"]
    total_tasks = 0
    tasks_stolen = 0
    total_steals = 0
    total_flag_steals = 0
    tasks_flag_stolen = 0
    total_flag_wait_time = 0
    total_flag_set_delay = 0
    total_queueing_time = 0
    total_requeue_wait_time = 0
    next(task_file) # skip first line
    for line in task_file:
        data = line.split(",")
        if int(data[0]) > time_dropped * stats["End Time"] and int(data[1]) >= 0:
            total_tasks += 1
            task_latencies.append(int(data[1]))
            total_queueing_time += (int(data[1])) - int(data[2])
            total_requeue_wait_time += int(data[9])
            steals = int(data[3])
            flag_steals = int(data[10]) if len(data) > 10 else 0
            if steals > 0:
                tasks_stolen += 1
                total_steals += steals
            if flag_steals > 0:
                tasks_flag_stolen += 1
                total_flag_steals += flag_steals
                if len(data) > 11:
                    total_flag_wait_time += int(data[11])
                if len(data) > 12:
                    total_flag_set_delay += int(data[12])
            if len(data) > 12 and data[13] == 1:
                flagged_task_service_times.append(int(data[2]))
                flagged_task_time_left.append(int(data[14]))

    percentiles = np.percentile(task_latencies, [95, 99.9, 50, 99])

    ## 99.9% Tail Flag Stats
    tasks_stolen_999 = 0
    tasks_999 = 0
    tasks_flag_stolen_999 = 0
    total_flag_wait_time_999 = 0
    total_flag_steals_999 = 0
    total_flag_set_delay_999 = 0
    task_file.seek(0,0)
    next(task_file)
    for line in task_file:
        data = line.split(",")
        if int(data[0]) > time_dropped * stats["End Time"] and int(data[1]) >= percentiles[1]:
            tasks_999 += 1
            if int(data[3]) > 0:
                tasks_stolen_999 += 1
    if len(data) > 10:
        for line in task_file:
            data = line.split(",")
            if int(data[0]) > time_dropped * stats["End Time"] and int(data[1]) >= percentiles[1] and int(data[10]) > 0:
                tasks_flag_stolen_999 += 1
                total_flag_steals_999 += int(data[10])
                if len(data) > 11:
                    total_flag_wait_time_999 += int(data[11])
                    total_flag_set_delay_999 += int(data[12])

    task_file.close()

    percent_stolen = tasks_stolen / total_tasks if total_tasks > 0 else 0
    percent_flag_stolen = tasks_flag_stolen / total_tasks if total_tasks > 0 else 0
    avg_steals = total_steals / tasks_stolen if tasks_stolen > 0 else 0
    avg_flag_steals_per_task = total_flag_steals / tasks_flag_stolen if tasks_flag_stolen > 0 else 0
    flag_steal_rate = (stats["Flag Steals"] / stats["End Time"]) * 10**9
    flag_rate = (stats["Flags Raised"] / stats["End Time"]) * 10 ** 9
    average_steals_per_flag = total_flag_steals / stats["Flag Steals"] * 0.9 if stats["Flag Steals"] > 0 else 0 #TODO: This needs fixed - drop 10% of denom or keep all of numerator

    throughput = (complete_tasks/stats["End Time"]) * 10**9
    real_load = (complete_tasks/stats["End Time"]) / ((cores/ meta_data["AVERAGE_SERVICE_TIME"]))

    avg_queueing_time = total_queueing_time / total_tasks if total_tasks > 0 else 0
    avg_requeue_wait_time = total_requeue_wait_time / tasks_stolen if tasks_stolen > 0 else 0
    avg_task_flag_wait_time = total_flag_wait_time / total_flag_steals if total_flag_steals > 0 else 0
    avg_core_flag_wait_time_99 = total_flag_wait_time_999 / total_flag_steals_999 if total_flag_steals_999 > 0 else 0
    avg_flag_set_delay_time = total_flag_set_delay / total_flag_steals if total_flag_steals > 0 else 0
    avg_flag_set_delay_time_99 = total_flag_set_delay_999 / total_flag_steals_999 if total_flag_steals_999 > 0 else 0

    avg_flagged_service_time = sum(flagged_task_service_times) / len(flagged_task_service_times) if len(flagged_task_service_times) > 0 else 0
    avg_flagged_time_left = sum(flagged_task_time_left) / len(flagged_task_time_left) if len(flagged_task_time_left) > 0 else 0
    pct_flagged_queues_empty = len(flagged_task_service_times) / (stats["Empty Queues Flagged"] + len(flagged_task_service_times)) if (stats["Empty Queues Flagged"] + len(flagged_task_service_times)) > 0 else 0

    avg_time_from_alloc_to_task = stats["Total Alloc to Task Time"] / stats["Number Allocations"] if stats["Number Allocations"] > 0 else 0

    data_string = "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(
        run_name[4:], meta_data["num_threads"], meta_data["sim_duration"], meta_data["AVERAGE_SERVICE_TIME"],
        meta_data["avg_system_load"], avg_load * 100, avg_task_load * 100, avg_ws_load * 100, percentiles[0],
        percentiles[1], percentiles[2], percentiles[3], percent_stolen * 100, avg_steals, throughput, real_load * 100,
        (stats["Global Park Count"]/stats["End Time"]) * 10**9, successful_ws_time, unsuccessful_ws_time, non_work_conserving_time,
        allocation_time, task_time, distracted_time, unpaired_time, paired_time, avg_requeue_wait_time, flag_task_time, avg_time_from_alloc_to_task,
        avg_flag_steals_per_task, flag_steal_rate, flag_rate, percent_flag_stolen * 100, average_steals_per_flag,
        avg_core_flag_wait_time * 100, avg_task_flag_wait_time, avg_queueing_time, avg_core_flag_wait_time_99,
        avg_flag_set_delay_time, avg_flag_set_delay_time_99, avg_flagged_service_time, avg_flagged_time_left, pct_flagged_queues_empty * 100,
        "\"{}\"".format(meta_data["description"]))
    output_file.write(data_string + "\n")


def main():
    # Arguments:
    # First arg is either a name of a file with a list of runs or the name of one run (or nothing to use entire results dir)
    # Second arg is output file
    # Third arg is how many of the first tasks to drop for task latency metrics

    if len(sys.argv) != 4:
        print("Invalid number of arguments.")
        exit(0)

    output_file_name = sys.argv[-2]
    output_file = open(output_file_name, "w")
    output_file.write(CSV_HEADER + "\n")

    sim_list = []
    name = sys.argv[1].strip()

    # File with list of sim names
    if os.path.isfile("./" + name):
        sim_list_file = open(name)
        sim_list = sim_list_file.readlines()
        sim_list_file.close()

    # Name of one run
    elif os.path.isdir(RESULTS_SUBDIR_NAME.format(name)):
        sim_list.append(name)

    # Name of multiple runs (different threads)
    elif os.path.isdir(RESULTS_SUBDIR_NAME.format(THREAD_RUN_FORMAT.format(name, 0))):
        i = 0
        while os.path.isdir(RESULTS_SUBDIR_NAME.format(THREAD_RUN_FORMAT.format(name, i))):
            sim_list.append(THREAD_RUN_FORMAT.format(name, i))
            i += 1
    else:
        print("File or directory not found")

    for sim_name in sim_list:
        analyze_sim_run(sim_name.strip(), output_file, time_dropped=int(sys.argv[-1])/100)
        print("Simulation {} analysis complete".format(sim_name))

    output_file.close()


if __name__ == "__main__":
    main()
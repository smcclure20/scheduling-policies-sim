from simulation import Simulation, RESULTS_DIR, META_LOG_FILE, CONFIG_LOG_DIR, SINGLE_THREAD_SIM_NAME_FORMAT, \
    MULTI_THREAD_SIM_NAME_FORMAT
from sim_config import SimConfig

import sys
import os
from datetime import datetime
import multiprocessing
import json
import pathlib


class SimProcess(multiprocessing.Process):
    def __init__(self, thread_id, name, configuration, sim_dir_path):
        multiprocessing.Process.__init__(self)
        self.thread_id = thread_id
        self.name = name
        self.config = configuration
        self.sim_path = sim_dir_path

    def run(self):
        print("Starting " + self.name)
        simulation = Simulation(self.config, self.sim_path)
        simulation.run()
        simulation.save_stats()
        print("Exiting " + self.name)


if __name__ == "__main__":
    time = datetime.now().strftime("%y-%m-%d_%H:%M:%S")

    loads = list(range(10, 110, 10))
    threads = []
    cores = None
    description = ""

    path_to_sim = os.path.relpath(pathlib.Path(__file__).resolve().parents[1], start=os.curdir)

    if os.path.isfile(sys.argv[1]):
        cfg_json_fp = open(sys.argv[1], "r")
        cfg_json = cfg_json_fp.read()
        cfg_json_fp.close()

    if "-varycores" in sys.argv:
        cores = [4, 8, 12, 16, 17, 18, 19, 20, 21, 22, 23, 24, 28, 32] #list(range(4, 36, 4))
        sys.argv.remove("-varycores")

    if len(sys.argv) > 2:
        name = SINGLE_THREAD_SIM_NAME_FORMAT.format(os.uname().nodename, time)
        if not os.path.isdir(RESULTS_DIR.format(path_to_sim)):
            os.makedirs(RESULTS_DIR.format(path_to_sim))
        meta_log = open(META_LOG_FILE.format(path_to_sim), "a")
        meta_log.write("{}: {}\n".format(name, sys.argv[2]))
        meta_log.close()
        description = sys.argv[2]


    if cores is not None:
        for i, core_num in enumerate(cores):
            name = MULTI_THREAD_SIM_NAME_FORMAT.format(os.uname().nodename, time, i)

            if os.path.isfile(sys.argv[1]):
                cfg = json.loads(cfg_json, object_hook=SimConfig.decode_object)
                if cfg.reallocation_replay:
                    name_parts = cfg.reallocation_record.split("_", 1)
                    cfg.reallocation_record = MULTI_THREAD_SIM_NAME_FORMAT.format(name_parts[0], name_parts[1], i)
                cfg.num_threads = core_num
                if cfg.num_queues != 1:
                    cfg.num_queues = core_num
                cfg.mapping = list(range(core_num))
                cfg.set_ws_permutation()
                cfg.name = name
                cfg.description = description
                cfg.progress_bar = (i == 0)

            else:
                print("Missing or invalid argument")
                exit(1)

            threads.append(SimProcess(i, name, cfg, path_to_sim))

    else:
        for i, load in enumerate(loads):
            name = MULTI_THREAD_SIM_NAME_FORMAT.format(os.uname().nodename, time, i)

            if os.path.isfile(sys.argv[1]):
                cfg = json.loads(cfg_json, object_hook=SimConfig.decode_object)
                if cfg.reallocation_replay:
                    name_parts = cfg.reallocation_record.split("_", 1)
                    cfg.reallocation_record = MULTI_THREAD_SIM_NAME_FORMAT.format(name_parts[0], name_parts[1], i)
                cfg.avg_system_load = load / 100
                cfg.name = name
                cfg.progress_bar = (i == 0)
                cfg.description = description

            else:
                print("Missing or invalid argument")
                exit(1)

            threads.append(SimProcess(i, name, cfg, path_to_sim))

    threads.reverse()
    for thread in threads:
        thread.start()

    if not(os.path.isdir(CONFIG_LOG_DIR.format(path_to_sim))):
        os.makedirs(CONFIG_LOG_DIR.format(path_to_sim))
    config_record = open(
        CONFIG_LOG_DIR.format(path_to_sim) + SINGLE_THREAD_SIM_NAME_FORMAT.format(os.uname().nodename, time) + ".json",
        "w")
    config_record.write(cfg_json)
    config_record.close()

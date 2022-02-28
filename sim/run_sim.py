import threading
import simulation as sim
import sys
import os
from datetime import datetime
import multiprocessing
import json

# TODO: Fix multithreaded printing progress bars

class SimProcess(multiprocessing.Process):
    def __init__(self, thread_id, name, configuration):
        multiprocessing.Process.__init__(self)
        self.thread_id = thread_id
        self.name = name
        self.config = configuration

    def run(self):
        print("Starting " + self.name)
        simulation = sim.Simulation(self.config)
        simulation.run()
        simulation.save_stats()
        print("Exiting " + self.name)


if __name__ == "__main__":
    time = datetime.now().strftime("%y-%m-%d_%H:%M:%S")

    loads = list(range(10, 110, 10))
    threads = []
    cores = None
    description = ""

    if os.path.isfile(sys.argv[1]):
        cfg_json_fp = open(sys.argv[1], "r")
        cfg_json = cfg_json_fp.read()
        cfg_json_fp.close()

    if "-varycores" in sys.argv:
        cores = [4, 8, 12, 16, 17, 18, 19, 20, 21, 22, 23, 24, 28, 32] #list(range(4, 36, 4))
        sys.argv.remove("-varycores")

    if len(sys.argv) > 2:
        name = sim.SINGLE_THREAD_SIM_NAME_FORMAT.format(os.uname().nodename, time)
        meta_log = open(sim.META_LOG_FILE, "a")
        meta_log.write("{}: {}\n".format(name, sys.argv[2]))
        meta_log.close()
        description = sys.argv[2]


    if cores is not None:
        for i, core_num in enumerate(cores):
            name = sim.MULTI_THREAD_SIM_NAME_FORMAT.format(os.uname().nodename, time, i)

            if os.path.isfile(sys.argv[1]):
                cfg = json.loads(cfg_json, object_hook=sim.SimConfig.decode_object)
                if cfg.reallocation_replay:
                    name_parts = cfg.reallocation_record.split("_", 1)
                    cfg.reallocation_record = sim.MULTI_THREAD_SIM_NAME_FORMAT.format(name_parts[0], name_parts[1], i)
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

            threads.append(SimProcess(i, name, cfg))

    else:
        for i, load in enumerate(loads):
            name = sim.MULTI_THREAD_SIM_NAME_FORMAT.format(os.uname().nodename, time, i)
            # if sys.argv[1] == "single_q": # TODO: we can probably dump this now that we have config files
            #     if len(sys.argv) > 4:
            #         name_parts = sys.argv[4].split("_", 1)
            #         playback_file = sim.MULTI_THREAD_SIM_NAME_FORMAT.format(name_parts[0], name_parts[1], i)
            #     else:
            #         playback_file = None
            #     cfg = sim.SimConfig.get_single_queue_config(name, int(sys.argv[2]), load, int(sys.argv[3]),
            #                                                 progress=(i == 0),
            #                                                 playback_file=playback_file)
            #     cfg_json = json.dumps(cfg)
            #
            # elif sys.argv[1] == "multi_q":
            #     arg_map = sim.SimConfig.parse_args(sys.argv[4:])
            #     cfg = sim.SimConfig.get_multiple_queue_config(name, int(sys.argv[2]), range(int(sys.argv[2])), load,
            #                                                   int(sys.argv[3]), arg_map["parking"],
            #                                                   record_allocs=arg_map["record_allocs"],
            #                                                   fast_forward=arg_map["ff"],
            #                                                   record_steals=arg_map["record_steals"],
            #                                                   buffer_cores=arg_map["buffer"],
            #                                                   ws_self_check=arg_map["ws_self_check"],
            #                                                   allocation_delay=arg_map["alloc_delay"],
            #                                                   two_choices=arg_map["two_choices"],
            #                                                   progress=(i == 0))

            if os.path.isfile(sys.argv[1]):
                cfg = json.loads(cfg_json, object_hook=sim.SimConfig.decode_object)
                if cfg.reallocation_replay:
                    name_parts = cfg.reallocation_record.split("_", 1)
                    cfg.reallocation_record = sim.MULTI_THREAD_SIM_NAME_FORMAT.format(name_parts[0], name_parts[1], i)
                cfg.avg_system_load = load / 100
                cfg.name = name
                cfg.progress_bar = (i == 0)
                cfg.description = description

            else:
                print("Missing or invalid argument")
                exit(1)

            threads.append(SimProcess(i, name, cfg))

    threads.reverse()
    for thread in threads:
        thread.start()

    config_record = open(
        sim.CONFIG_LOG_DIR + sim.SINGLE_THREAD_SIM_NAME_FORMAT.format(os.uname().nodename, time) + ".json", "w")
    config_record.write(cfg_json)
    config_record.close()

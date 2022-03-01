import sys
import os

RESULTS_DIR_NAME = "./results/"
CONFIG_RECORD_DIR = "./config_records/"
CONFIG_RECORD_NAME = "{}.json"
META_LOG_PATH = RESULTS_DIR_NAME + "meta_log"
run_name = sys.argv[1]

with open(META_LOG_PATH, "r") as f:
    lines = f.readlines()

try:
    config_record = CONFIG_RECORD_DIR + CONFIG_RECORD_NAME.format(run_name)
    os.remove(config_record)
    print("Deleted %s" % config_record)
except OSError as e:
    print("Error: %s : %s" % (config_record, e.strerror))
    exit(1)


with open(META_LOG_PATH, "w") as f:
    for line in lines:
        if not(run_name in line):
            f.write(line)

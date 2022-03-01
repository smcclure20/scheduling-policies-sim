import shutil
import sys
import os

RESULTS_DIR_NAME = "./results/"
RESULTS_SUBDIR_NAME = RESULTS_DIR_NAME + "{}/"
META_LOG_PATH = RESULTS_DIR_NAME + "meta_log"
CONFIG_RECORD_DIR = "./config_records/"
CONFIG_RECORD_NAME = "{}.json"

run_name = sys.argv[1]

if "-delconf" in sys.argv:
    config_record = CONFIG_RECORD_DIR + CONFIG_RECORD_NAME.format(run_name)
    os.remove(config_record)
    print("Deleted %s" % config_record)

with open(META_LOG_PATH, "r") as f:
    lines = f.readlines()

dir_nums = []
all_dirs = os.listdir(RESULTS_DIR_NAME)
for subdir in all_dirs:
    if run_name in subdir:
        shutil.rmtree(subdir)
        print("Deleted %s" % subdir)

with open(META_LOG_PATH, "w") as f:
    for line in lines:
        if not(run_name in line):
            f.write(line)

#!/usr/bin/env python
"""Configuration of simulation parameters."""

import random


class SimConfig:
    """Object to hold all configuration state of the simulation. Remains constant."""

    def __init__(self, name=None, num_queues=None, num_threads=None, mapping=[], avg_system_load=None,
                 initial_num_tasks=None, sim_duration=None, locking_enabled=True, ws_enabled=True, parking=True,
                 ff_enabled=False, pb_enabled=True, record_allocations=False, realloc_record=None, record_steals=False,
                 buffer_cores=False, ws_self_checks=False, allocation_delay=False, two_choices=False,
                 delay_range_enabled=False, oracle=False, delay_flagging=False, enqueue_choice=False, random_ws=False,
                 constant_service_time=False, regular_arrivals=False, load_thread_count=None, ws_sibling_first=True,
                 enqueue_by_st_sum=False, always_check_realloc=False, ideal_flag_steal=False, delay_range_by_service_time=False,
                 ideal_reallocation=False, fred_reallocation=False, spin_parking_enabled=False, utilization_range_enabled=False,
                 allow_naive_idle=False, work_steal_park_enabled=False, bimodal_service_time=False, join_bounded_shortest_queue=False,
                 record_queue_lens=False):
        # Basic configuration
        self.name = name
        self.description = ""
        self.num_queues = num_queues
        self.num_threads = num_threads
        self.mapping = list(mapping) # mapping is a list of queue numbers indexed by thread number
        self.avg_system_load = avg_system_load
        self.num_tasks = initial_num_tasks
        self.sim_duration = sim_duration
        self.load_thread_count = load_thread_count

        # Additional parameters
        self.locking_enabled = locking_enabled
        self.work_stealing_enabled = ws_enabled
        self.parking_enabled = parking
        self.fast_forward_enabled = ff_enabled
        self.progress_bar = pb_enabled
        self.record_allocations = record_allocations
        self.reallocation_record = realloc_record
        self.reallocation_replay = self.reallocation_record is not None and self.parking_enabled
        self.record_steals = record_steals
        self.buffer_cores_enabled = buffer_cores
        self.delay_range_enabled = delay_range_enabled
        self.ws_self_checks = ws_self_checks
        self.allocation_delay = allocation_delay
        self.two_choices = two_choices
        self.oracle_enabled = oracle
        self.delay_flagging_enabled = delay_flagging
        self.enqueue_choice = enqueue_choice
        self.random_work_steal_search = random_ws
        self.constant_service_time = constant_service_time
        self.regular_arrivals = regular_arrivals
        self.ws_sibling_first = ws_sibling_first
        self.enqueue_by_st_sum = enqueue_by_st_sum
        self.always_check_realloc = always_check_realloc
        self.ideal_flag_steal = ideal_flag_steal
        self.delay_range_by_service_time = delay_range_by_service_time
        self.ideal_reallocation_enabled = ideal_reallocation
        self.fred_reallocation = fred_reallocation
        self.spin_parking_enabled = spin_parking_enabled
        self.utilization_range_enabled = utilization_range_enabled
        self.allow_naive_idle = allow_naive_idle
        self.work_steal_park_enabled = work_steal_park_enabled
        self.bimodal_service_time = bimodal_service_time
        self.join_bounded_shortest_queue = join_bounded_shortest_queue
        self.record_queue_lens = record_queue_lens

        # Constants
        self.AVERAGE_SERVICE_TIME = 1000
        self.WORK_STEAL_CHECK_TIME = 120
        self.WORK_STEAL_TIME = 120
        self.MINIMUM_WORK_SEARCH_TIME = 3720
        self.LOCAL_QUEUE_CHECK_TIME = 0
        self.CORE_REALLOCATION_TIMER = 5000
        self.IDLE_PARK_TIME = 0
        self.LOCAL_QUEUE_CHECK_TIMER = 1000
        self.ALLOCATION_TIME = 5000
        self.BUFFER_CORE_COUNT_MIN = 2
        self.BUFFER_CORE_COUNT_MAX = 4
        self.BUFFER_CORE_PCT_MIN = None
        self.BUFFER_CORE_PCT_MAX = None
        self.REALLOCATION_THRESHOLD_MIN = 2500
        self.REALLOCATION_THRESHOLD_MAX = 10000
        self.DELAY_THRESHOLD = 15000
        self.FLAG_STEAL_DELAY = 120
        self.ENQUEUE_CHOICES = 2
        self.ENQUEUE_PENALTY = 120
        self.ALLOCATION_PAUSE = 5000
        self.ALLOCATION_THRESHOLD = 5000
        self.REQUEUE_PENALTY = 120
        self.UTILIZATION_MIN = 80
        self.UTILIZATION_MAX = 90
        self.FLAG_OPTIONS = 10
        self.WORK_STEAL_CHOICES = 10
        self.QUEUE_BOUND = 1

        # Set the work steal permutation
        if num_queues is not None:
            self.set_ws_permutation()
        else:
            self.WS_PERMUTATION = None

        # Record any additional file constants, ignoring recording ones
        self.constants = {}
        for key in globals().keys():
            if key.isupper() and "FORMAT" not in key and "FILE" not in key:
                self.constants[key] = globals()[key]

    def set_ws_permutation(self):
        """Shuffle queue list to decouple from allocation policy."""
        queues = list(range(self.num_queues))
        random.shuffle(queues)
        self.WS_PERMUTATION = queues

    def validate(self):
        """Validate configuration parameters."""
        # TODO: Update this for accuracy
        if self.num_queues == 0 or self.num_threads == 0:
            print("There must be nonzero queues and threads")
            return False

        if self.WORK_STEAL_CHECK_TIME == 0 and self.ws_self_checks:
            print("Cannot check queue within an infinitely fast work stealing cycle")
            return False

        # There must be some way for a core to spend time (even with parking the final core must do something)
        if self.LOCAL_QUEUE_CHECK_TIME == 0 and self.WORK_STEAL_CHECK_TIME == 0 and \
                (not self.parking_enabled and self.IDLE_PARK_TIME != 0):
            print("There must be some way for a core to spend time (even with parking the final core must do something)")
            return False

        if self.num_queues != len(set(self.mapping)):
            print("Number of queues does not match number in thread/queue mapping.")
            return False

        if sum([self.buffer_cores_enabled, self.delay_range_enabled, self.ideal_reallocation_enabled,
                self.fred_reallocation, self.utilization_range_enabled]) > 1:
            print("Only one allocation policy may be enabled.")
            return False

        if self.reallocation_replay and self.num_queues != 1:
            print("Reallocation replay assumes one queue.")
            return False

        if self.work_stealing_enabled and self.num_queues == 1:
            print("Cannot work steal with one queue.")
            return False

        if self.ws_sibling_first and self.random_work_steal_search:
            print("Random work steal walks assume siblings are not checked first.")
            return False

        if self.WORK_STEAL_CHECK_TIME == 0 and self.ws_self_checks:
            print("Cannot do checks on local queue while work stealing if it doesn't take any time.")
            return False

        if self.fred_reallocation and self.num_threads != self.num_queues:
            print("Fred assumes 1-1 mapping between threads and queues.")
            return False

        if self.fred_reallocation and not self.always_check_realloc:
            print("With Fred reallocations, always check reallocations needs to be on to make sure a core "
                  "can wake up if all are parked.")
            return False

        if self.num_queues % 2 != 0 and self.ws_sibling_first:
            print("Cannot have an odd number of queues and check siblings first.")
            return False

        if self.WORK_STEAL_CHOICES > 1 and not self.random_work_steal_search:
            print("To utilize extra choices, random work steal search must be enabled.")
            return False

        if self.bimodal_service_time and self.constant_service_time:
            print("Only one service time distribution can be specified.")
            return False

        # At least one way to decide when the simulation is over is needed
        if (self.num_tasks is None and self.sim_duration is None) or \
                (self.num_tasks is not None and self.num_tasks <= 0) or \
                (self.sim_duration is not None and self.sim_duration <= 0):
            print("There must be at least one way to decide when the simulation is over")
            return False

        return True

    def __str__(self):
        return str(self.__dict__)

    @staticmethod
    def decode_object(o):
        a = SimConfig()
        a.__dict__.update(o)
        a.set_ws_permutation()
        return a

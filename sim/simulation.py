#!/usr/bin/env python
"""Creates a runs a simulation."""

import logging
import random
import os
import json
import math
import sys
import datetime
import pathlib

from simulation_state import SimulationState
from sim_thread import Thread
from tasks import Task
import progress_bar as progress
from sim_config import SimConfig

SINGLE_THREAD_SIM_NAME_FORMAT = "{}_{}"
MULTI_THREAD_SIM_NAME_FORMAT = "{}_{}_t{}"
RESULTS_DIR = "{}/results/"
META_LOG_FILE = "{}/results/meta_log"
CONFIG_LOG_DIR = "{}/config_records/"


class Simulation:
    """Runs the simulation based on the simulation state."""

    def __init__(self, configuration, sim_dir_path):
        self.config = configuration
        self.state = SimulationState(configuration)
        self.sim_dir_path = sim_dir_path

    def run(self):
        """Run the simulation."""

        # Initialize data
        self.state.initialize_state(self.config)

        # A short duration may result in no tasks
        self.state.tasks_scheduled = len(self.state.tasks)
        if self.state.tasks_scheduled == 0:
            return

        # Start at first time stamp with an arrival
        task_number = 0
        self.state.timer.increment(self.state.tasks[0].arrival_time)

        allocation_number = 0
        reschedule_required = False

        if self.config.progress_bar:
            print("\nSimulation started")

        # Run for acceptable time or until all tasks are done
        while self.state.any_incomplete() and \
                (self.config.sim_duration is None or self.state.timer.get_time() < self.config.sim_duration):

            # If fast forwarding, find the time jump
            if self.config.fast_forward_enabled:
                next_arrival, next_alloc = self.find_next_arrival_and_alloc(task_number, allocation_number)
                time_jump, reschedule_required = self.find_time_jump(next_arrival, next_alloc,
                                                                     immediate_reschedule=reschedule_required)

            logging.debug("\n(jump: {}, rr: {})".format(time_jump, reschedule_required))

            # Put new task arrivals in queues
            while task_number < self.state.tasks_scheduled and \
                    self.state.tasks[task_number].arrival_time <= self.state.timer.get_time():

                if self.config.join_bounded_shortest_queue:
                    chosen_queue = self.state.main_queue
                    self.state.main_queue.enqueue(self.state.tasks[task_number], set_original=False)

                elif self.config.enqueue_choice:
                    chosen_queue = self.choose_enqueue(self.config.ENQUEUE_CHOICES)
                    working_cores = self.state.currently_working_cores()
                    if len(working_cores) == 0:
                        self.state.tasks[task_number].source_core = self.state.queues[chosen_queue].get_core()
                    else:
                        self.state.tasks[task_number].source_core = random.choice(self.state.currently_working_cores())
                    source_core = self.state.tasks[task_number].source_core
                    if source_core != chosen_queue:
                        self.state.threads[source_core].enqueue_penalty += 1
                        self.state.queues[chosen_queue].awaiting_enqueue = True
                        self.state.tasks[task_number].to_enqueue = chosen_queue
                    self.state.queues[source_core].enqueue(self.state.tasks[task_number], set_original=True)

                else:
                    chosen_queue = random.choice(self.state.available_queues)
                    self.state.queues[chosen_queue].enqueue(self.state.tasks[task_number], set_original=True)

                if self.config.fred_reallocation and \
                        self.state.threads[self.state.queues[chosen_queue].get_core()].is_busy():
                    self.state.threads[self.state.queues[chosen_queue].get_core()].fred_preempt = True

                logging.debug("[ARRIVAL]: {} onto queue {}".format(self.state.tasks[task_number], chosen_queue))
                task_number += 1

            # Reallocations
            # Continuously check for reallocations
            if self.config.parking_enabled and self.config.always_check_realloc and\
                    self.state.timer.get_time() - self.state.last_realloc_choice >= self.config.ALLOCATION_PAUSE:
                self.reallocate_threads()

            # Every x us, check for threads to park
            elif self.config.parking_enabled and not self.config.reallocation_replay and \
                    self.state.timer.get_time() % self.config.CORE_REALLOCATION_TIMER == 0:
                self.reallocate_threads()

            # Reallocation replay
            elif self.config.reallocation_replay:
                while allocation_number < self.state.reallocations and \
                        self.state.reallocation_schedule[allocation_number][0] <= self.state.timer.get_time():
                    if self.state.reallocation_schedule[allocation_number][1]:
                        self.state.deallocate_thread(self.find_deallocation())
                    else:
                        self.state.allocate_thread()
                    allocation_number += 1

            # No parking, but still record some stats at reallocation time
            elif not self.config.parking_enabled and self.config.record_allocations and \
                    self.state.timer.get_time() % self.config.CORE_REALLOCATION_TIMER == 0:
                self.state.add_realloc_time_check_in()

            # If recording queue lens but not parking, still do it on reallocs
            elif not self.config.parking_enabled and self.config.record_queue_lens and \
                    self.state.timer.get_time() % self.config.CORE_REALLOCATION_TIMER == 0:
                self.state.record_queue_lengths()

            # Schedule threads
            if self.config.fast_forward_enabled:
                self.fast_forward(time_jump)
            else:
                # Schedule threads
                for thread in self.state.threads:
                    thread.schedule()

                # Move forward in time
                self.state.timer.increment(1)

            # Log state (in debug mode)
            logging.debug("\nTime step: {}".format(self.state.timer))
            logging.debug("Thread status:")
            for thread in self.state.threads:
                logging.debug(str(thread) + " -- queue length of " + str(thread.queue.length()))

            # Print progress bar
            if self.config.progress_bar and self.state.timer.get_time() % 10000 == 0:
                progress.print_progress(self.state.timer.get_time(), self.config.sim_duration, length=50, decimals=3)

        # When the simulation is complete, record final stats
        self.state.add_final_stats()

    def choose_enqueue(self, num_choices):
        """Choose a queue to place a new task on by current queueing delay."""
        if num_choices > len(self.state.available_queues):
            num_choices = len(self.state.available_queues)
        choices = random.sample(self.state.available_queues, num_choices)

        delay = self.state.queues[choices[0]].length_by_service_time() if self.config.enqueue_by_st_sum \
            else self.state.queues[choices[0]].length(count_current=True)
        chosen_queue = choices[0]
        for choice in choices:
            if self.config.enqueue_by_st_sum:
                if self.state.queues[choice].length_by_service_time() < delay:
                    delay = self.state.queues[choice].length_by_service_time()
                    chosen_queue = choice
            elif self.state.queues[choice].length(count_current=True) < delay:
                delay = self.state.queues[choice].length(count_current=True)
                chosen_queue = choice

        return chosen_queue

    def reallocate_threads(self):
        """Reallocate threads according to policy defined in the configuration."""
        self.state.record_queue_lengths()
        if self.config.delay_range_enabled:
            self.reallocate_threads_delay_range()
        elif self.config.buffer_cores_enabled:
            self.reallocate_threads_buffer_cores()
        elif self.config.utilization_range_enabled:
            self.reallocate_threads_utilization()
        elif self.config.ideal_reallocation_enabled:
            self.reallocate_threads_ideal()
        elif self.config.fred_reallocation:
            # If all cores are parked, wake one up when a new task arrives (requires always checking reallocations)
            if len(self.state.parked_threads) == self.config.num_threads:
                self.state.allocate_thread()
        else:
            self.reallocate_threads_default()

    def reallocate_threads_default(self):
        """Reallocate threads. Grants a core if there is a queue whose head has been around for longer than the
        reallocation timer."""
        if self.state.any_queue_past_delay_threshold():
            self.state.allocate_thread()
        else:
            self.state.add_reallocation(None)

    def reallocate_threads_ideal(self):
        """Reallocate threads. Grants a core for every queued task as it is able."""

        # Determine how many queued items there are
        queued_tasks = self.state.total_queue_occupancy()

        # Determine how many threads are non-productive (weaker constraint than buffer cores)
        non_productive_cores = self.state.currently_non_productive_cores()

        # Add/remove the difference
        if queued_tasks < len(non_productive_cores):
            while queued_tasks < len(non_productive_cores):
                thread = min(non_productive_cores)
                self.state.deallocate_thread(thread)
                non_productive_cores = self.state.currently_non_productive_cores()
        elif queued_tasks > len(non_productive_cores):
            while queued_tasks > len(non_productive_cores):
                self.state.allocate_thread()
                non_productive_cores = self.state.currently_non_productive_cores()
        else:
            self.state.add_reallocation(None)

    def reallocate_threads_delay_range(self):
        """Reallocate cores according to delay range."""
        if self.config.delay_range_by_service_time:
            avg_delay = self.state.current_average_service_time_sum()
        else:
            avg_delay = self.state.current_average_queueing_delay()

        # If delay is high, add a core
        if avg_delay > self.config.REALLOCATION_THRESHOLD_MAX:
            self.state.allocate_thread()

        # If delay is low, remove a core
        elif avg_delay < self.config.REALLOCATION_THRESHOLD_MIN:
            # Only deallocate if there are buffer cores available
            if len(self.state.current_buffer_cores(check_work_available=True)) > 0:
                # Force clustering of parked cores by choosing min index
                thread = min(self.state.current_buffer_cores(check_work_available=True))
                self.state.deallocate_thread(thread)
        else:
            self.state.add_reallocation(None)

    def reallocate_threads_utilization(self):
        """Reallocate cores according to utilization range."""
        utilization = self.state.current_utilization()

        # If high utilization, add a thread
        if utilization > self.config.UTILIZATION_MAX:
            self.state.allocate_thread()

        # If low utilization, revoke a core
        elif utilization < self.config.UTILIZATION_MIN and \
                len(self.state.current_buffer_cores(check_work_available=True)) > 0:
            thread = min(self.state.current_buffer_cores(check_work_available=True))
            self.state.deallocate_thread(thread)

        # Log that an allocation decision was considered
        else:
            self.state.add_reallocation(None)

    def reallocate_threads_buffer_cores(self):
        """Reallocate cores according to buffer core ranges."""
        allowed_buffer_cores = self.state.allowed_buffer_cores()
        current_buffer_cores = self.state.current_buffer_cores()
        num_current_buffer_cores = len(current_buffer_cores)

        # If not enough buffer cores, allocate a core
        if num_current_buffer_cores < allowed_buffer_cores[0]:
            while len(current_buffer_cores) < allowed_buffer_cores[0]:
                allocated = self.state.allocate_thread()
                if allocated is None:
                    break
                current_buffer_cores = self.state.current_buffer_cores()

        # If too many buffer cores, deallocate a thread
        elif num_current_buffer_cores > allowed_buffer_cores[1]:
            while len(current_buffer_cores) > allowed_buffer_cores[1]:
                thread = random.choice(current_buffer_cores)
                self.state.deallocate_thread(thread)
                current_buffer_cores = self.state.current_buffer_cores()
        else:
            self.state.add_reallocation(None)

    def find_deallocation(self):
        """Find a core to deallocate from all non-parked cores. Preference is given to idle or soon-to-be-idle cores."""
        free_threads = set(range(self.config.num_threads)).difference(self.state.parked_threads)
        min_time_left = None

        choice = None

        # Prefer threads that are idle, then the one that will be idle soonest
        for thread_id in free_threads:
            if not self.state.threads[thread_id].is_productive():
                choice = thread_id
                break
            else:
                if min_time_left is None or self.state.threads[thread_id].current_task.time_left < min_time_left:
                    min_time_left = self.state.threads[thread_id].current_task.time_left
                    choice = thread_id

        return choice

    def find_next_arrival_and_alloc(self, task_number, allocation_number):
        """Determine the next task arrival and allocation decision.
        :param task_number: Current index into tasks that have arrived.
        :param allocation_number: Current allocation index into schedule if in replay.
        """
        next_arrival = self.state.tasks[task_number].arrival_time if task_number < self.state.tasks_scheduled else None
        next_alloc = None

        if self.config.reallocation_replay and allocation_number < self.state.reallocations:
            next_alloc = self.state.reallocation_schedule[allocation_number][0]

        elif self.config.always_check_realloc:
            if self.config.delay_range_enabled:
                # Check when max threshold - current avg delay will happen
                # (min threshold cannot be violated during phase of otherwise inaction)
                # If by service time, until next arrival (/completion), this value cannot change
                if self.config.delay_range_by_service_time:
                    time_until_threshold_passed = 0
                else:
                    time_until_threshold_passed = self.config.REALLOCATION_THRESHOLD_MAX - \
                                                  int(self.state.current_average_queueing_delay())

            elif self.config.buffer_cores_enabled:
                # Buffer cores cannot change between other actions
                time_until_threshold_passed = 0

            elif self.config.ideal_reallocation_enabled:
                # In ideal case, only arrivals change this
                time_until_threshold_passed = 0

            else:
                current_delays = [x.current_delay() for x in self.state.queues]
                time_until_threshold_passed = self.config.ALLOCATION_THRESHOLD - max(current_delays)
            next_alloc = self.state.timer.get_time() + time_until_threshold_passed \
                if time_until_threshold_passed > 0 else None

        # If recording queue lens, do it at the reallocation intervals
        elif self.config.parking_enabled or self.config.record_queue_lens:
            next_alloc = (math.floor(
                self.state.timer.get_time() / self.config.CORE_REALLOCATION_TIMER) + 1) \
                         * self.config.CORE_REALLOCATION_TIMER

        # When recording single queue reallocs
        elif self.config.record_allocations:
            next_alloc = (math.floor(
                self.state.timer.get_time() / self.config.CORE_REALLOCATION_TIMER) + 1) \
                         * self.config.CORE_REALLOCATION_TIMER

        return next_arrival, next_alloc

    def find_time_jump(self, next_arrival, next_allocation=None, set_clock=True, immediate_reschedule=False):
        """Find the time step to the next significant event that requires directly running the simulation.
        :param next_arrival: Next task arrival.
        :param next_allocation: Next core allocation event.
        :param set_clock:
        :param immediate_reschedule: True if last time step required a jump of 1 for the next step.
        (ie. completing a task)
        """
        # Find the next task completion time
        completion_times = []
        for thread in self.state.threads:
            if thread.current_task is not None and not thread.current_task.is_idle:
                completion_times.append(thread.current_task.expected_completion_time())

                # If a task completed now but immediate reschedule missed (ex. service time of 1), next jump must be 1
                if thread.last_complete == self.state.timer.get_time():
                    immediate_reschedule = True

        next_completion_time = min(completion_times) if len(completion_times) > 0 else None

        # Find the next event of any type
        upcoming_events = [next_arrival, next_completion_time, next_allocation]
        if not any(upcoming_events):
            next_event = self.state.timer.get_time() + 1
        else:
            next_event = min([event for event in upcoming_events if event])

        # Set the time jump
        jump = next_event - self.state.timer.get_time()

        if jump == 0: # this can happen with 0-duration tasks - TODO: look into this more
            jump = 1

        # If immediate reschedule required, jump is 1
        # Another immediate reschedule is necessary if the time jump would have been 1 regardless
        if immediate_reschedule:
            reschedule_required = jump == 1 and next_completion_time == next_event
            jump = 1
        else:
            # TODO: (below) Not if it is a work steal task that isn't actually done (but how to determine this?)
            reschedule_required = next_completion_time == next_event

        # Move the clock
        if set_clock:
            self.state.timer.increment(jump)
        # reschedule_required = False
        return jump, reschedule_required

    def fast_forward(self, jump):
        """Fast forward through uneventful timesteps."""
        for thread in self.state.threads:
            thread.schedule(time_increment=jump)
        # self.state.timer.increment(jump)
        # Record all paired/unpaired time
        self.determine_pairings(jump)

    def determine_pairings(self, jump):
        """Determine how to pair cores for accounting of how well they are spending their time."""
        increment = jump if jump != 0 else 1
        paired = self.state.num_paired_cores()
        for thread in self.state.threads:
            if thread.classified_time_step:
                thread.classified_time_step = False
                if thread.preempted_classification:
                    thread.preempted_classification = False
                    # If you preempt a work steal spin, you get to use this exact cycle on the new task
                    if paired > 0:
                        thread.add_paired_time(increment - 1)
                        paired -= 1
                    else:
                        thread.add_unpaired_time(increment - 1)
            elif paired > 0:
                thread.add_paired_time(increment)
                paired -= 1
            else:
                thread.add_unpaired_time(increment)

    def save_stats(self):
        """Save simulation date to file."""
        # Make files and directories
        new_dir_name = RESULTS_DIR.format(self.sim_dir_path) + "sim_{}/".format(self.config.name)
        os.makedirs(os.path.dirname(new_dir_name))
        cpu_file = open("{}cpu_usage.csv".format(new_dir_name, self.config.name), "w")
        task_file = open("{}task_times.csv".format(new_dir_name, self.config.name), "w")
        meta_file = open("{}meta.json".format(new_dir_name), "w")
        stats_file = open("{}stats.json".format(new_dir_name), "w")

        # Write CPU information
        cpu_file.write(','.join(Thread.get_stat_headers(self.config)) + "\n")
        for thread in self.state.threads:
            cpu_file.write(','.join(thread.get_stats()) + "\n")
        cpu_file.close()

        # Write task information
        task_file.write(','.join(Task.get_stat_headers(self.config)) + "\n")
        for task in self.state.tasks:
            task_file.write(','.join(task.get_stats()) + "\n")
        task_file.close()

        # Save the configuration
        json.dump(self.config.__dict__, meta_file, indent=0)
        meta_file.close()

        # Save global stats
        json.dump(self.state.results(), stats_file, indent=0)
        stats_file.close()

        # If recording work steal stats, save
        if self.config.record_steals:
            ws_file = open("{}work_steal_stats.csv".format(new_dir_name), "w")
            ws_file.write("Local Thread,Remote Thread,Time Since Last Check,Queue Length,Check Count,Successful\n")
            for check in self.state.ws_checks:
                ws_file.write("{},{},{},{},{},{}\n".format(check[0], check[1], check[2], check[3], check[4], check[5]))
            ws_file.close()

        # If recording allocations, save
        if self.config.record_allocations:
            realloc_sched_file = open("{}realloc_schedule".format(new_dir_name), "w")
            realloc_sched_file.write(str(self.state.reallocation_schedule))
            realloc_sched_file.close()

        # If recording queue lengths, save
        if self.config.record_queue_lens:
            qlen_file = open("{}queue_lens.csv".format(new_dir_name), "w")
            for lens in self.state.queue_lens:
                qlen_file.write(",".join([str(x) for x in lens]) + "\n")
            qlen_file.close()


if __name__ == "__main__":

    run_name = SINGLE_THREAD_SIM_NAME_FORMAT.format(os.uname().nodename,
                                                    datetime.datetime.now().strftime("%y-%m-%d_%H:%M:%S"))
    path_to_sim = os.path.relpath(pathlib.Path(__file__).resolve().parents[1], start=os.curdir)

    if os.path.isfile(sys.argv[1]):
        cfg_json = open(sys.argv[1], "r")
        cfg = json.load(cfg_json, object_hook=SimConfig.decode_object)
        cfg.name = run_name
        cfg_json.close()



        if "-d" in sys.argv:
            logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(message)s')
            sys.argv.remove("-d")

        if len(sys.argv) > 2:
            if not os.path.isdir(RESULTS_DIR.format(path_to_sim)):
                os.makedirs(RESULTS_DIR.format(path_to_sim))
            meta_log = open(META_LOG_FILE.format(path_to_sim), "a")
            meta_log.write("{}: {}\n".format(run_name, sys.argv[2]))
            meta_log.close()
            cfg.description = sys.argv[2]

    else:
        print("Config file not found.")
        exit(1)

    sim = Simulation(cfg, path_to_sim)
    sim.run()
    sim.save_stats()

    if not(os.path.isdir(CONFIG_LOG_DIR.format(path_to_sim))):
        os.makedirs(CONFIG_LOG_DIR.format(path_to_sim))
    config_record = open(CONFIG_LOG_DIR.format(path_to_sim) + run_name + ".json", "w")
    cfg_json = open(sys.argv[1], "r")
    config_record.write(cfg_json.read())
    cfg_json.close()
    config_record.close()

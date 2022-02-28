#!/usr/bin/env python
"""Thread running a tasks. Equivalent to a core in the simulation."""

import random
import logging
from work_search_state import WorkSearchState
from tasks import WorkSearchSpin, WorkStealTask, Task, EnqueuePenaltyTask, RequeueTask, ReallocationTask, FlagStealTask, QueueCheckTask, OracleWorkStealTask, IdleTask


class Thread:
    """Thread assigned to application to complete tasks."""

    def __init__(self, given_queue, identifier, config, state, given_sibling=None):
        self.queue = given_queue
        self.sibling = given_sibling
        self.current_task = None
        self.id = identifier
        self.work_search_state = WorkSearchState(config, state)
        self.scheduled_dealloc = False
        self.previous_task_type = None

        self.enqueue_penalty = 0
        self.enqueue_time = 0
        self.requeue_time = 0
        self.fred_preempt = False
        self.preempted_task = None
        self.classified_time_step = False
        self.classification = None
        self.distracted = False
        self.preempted_classification = False

        # For average utilization
        self.last_interval_task_time = 0
        self.last_interval_busy_time = 0

        # Flags
        self.work_steal_flag = None
        self.flag_sent = False

        # Flag accounting
        self.flag_time = None
        self.flag_wait_time = 0
        self.flag_set_delay = 0
        self.threshold_time = None
        self.flag_task_time = 0

        # General stats
        self.time_busy = 0
        self.work_stealing_time = 0
        self.task_time = 0
        self.work_steal_wait_time = 0
        self.successful_ws_time = 0
        self.unsuccessful_ws_time = 0
        self.allocation_time = 0
        self.unpaired_time = 0
        self.paired_time = 0
        self.distracted_time = 0

        # Other accounting
        self.last_complete = 0
        self.last_allocation = None
        self.non_work_conserving_time = 0

        self.config = config
        self.state = state

    def total_time(self):
        """Return total time spent on tasks, distracted, unpaired, and paired."""
        return self.distracted_time + self.task_time + self.unpaired_time + self.paired_time

    def is_busy(self, search_spin_idle=False):
        """Return true if the thread has any task."""
        if search_spin_idle:
            return self.current_task is not None and type(self.current_task) != WorkSearchSpin
        return self.current_task is not None

    def is_productive(self):
        """Return true if the thread is working on a productive task."""
        return self.current_task is not None and self.current_task.is_productive

    def is_distracted(self, evaluate=True):
        """Return true if the thread is working on overheads while there is a local task.
        Needs to be evaluated in each time step and not once it is over."""
        if evaluate:
            self.distracted = (self.current_task is not None and self.current_task.is_overhead) and \
                              self.queue.length() > 0 and self.queue.head().arrival_time < self.state.timer.get_time()
        return self.distracted

    def add_paired_time(self, amount):
        """Add time for accounting when the thread could have been working on an outstanding task but was not."""
        self.paired_time += amount

    def add_unpaired_time(self, amount):
        """Add time for accounting when the thread could not have been working on anything better."""
        self.unpaired_time += amount

    def set_flags(self):
        """Set flag on a remote core if the local queueing delay is long."""

        # If delay is long, attempt to raise a flag
        if self.queue.current_delay(second=True) > self.config.DELAY_THRESHOLD and not self.flag_sent and \
                self.queue.length() > 1:

            # Ideal flag stealing
            if self.config.ideal_flag_steal:
                helper = None
                helper_delay = self.queue.current_delay(second=True)
                for thread in self.state.threads:
                    if thread.queue.current_delay(second=True) < helper_delay and thread.work_steal_flag is None and \
                            thread.work_search_state.is_active():
                        helper = thread.id
                        helper_delay = thread.queue.current_delay(second=True)

            # Regular flag stealing
            else:
                options = [x.id for x in self.state.threads if x.work_steal_flag is None and
                           x.work_search_state.is_active() and x.id != self.id]

                if self.config.FLAG_OPTIONS > 1:
                    # Select flag_options-many options randomly
                    sample_len = self.config.FLAG_OPTIONS if len(options) >= self.config.FLAG_OPTIONS else len(options)
                    options = random.sample(options, sample_len)

                    # Choose the best one (by above metric)
                    helper = None
                    helper_delay = self.queue.current_delay(second=True)
                    for option in options:
                        if self.state.threads[option].queue.current_delay(second=True) < helper_delay:
                            helper = option
                            helper_delay = self.state.threads[option].queue.current_delay(second=True)
                else:
                    helper = random.choice(options) if len(options) > 0 else None

            # Send the flag
            if helper is not None:
                self.state.flag_raise_count += 1
                self.state.threads[helper].work_steal_flag = self.id
                self.flag_sent = True
                self.flag_time = self.state.timer.get_time()

                # Logging
                logging.debug("Thread {} flagged thread {} (with a delay of {})"
                              .format(self.id, helper, self.flag_time - self.threshold_time))
                if self.state.threads[helper].current_task is not None:
                    self.state.threads[helper].current_task.flagged = True
                    self.state.threads[helper].current_task.flagged_time_left = \
                        self.state.threads[helper].current_task.time_left
                else:
                    self.state.empty_flags += 1

        if self.queue.current_delay(second=True) > self.config.DELAY_THRESHOLD and not self.flag_sent:
            logging.debug("Thread {} failed to flag".format(self.id))

    def set_threshold_time(self):
        """Set the time when the queueing delay passed the threshold."""

        # This is the original queue or the task passed the threshold since being stolen
        if self.queue.second().requeue_time is None or \
                self.queue.second().requeue_time < self.queue.second().arrival_time + self.config.DELAY_THRESHOLD:
            self.threshold_time = self.queue.second().arrival_time + self.config.DELAY_THRESHOLD

        # Otherwise, this task was old before coming to this queue
        else:
            self.threshold_time = self.queue.second().requeue_time
        logging.debug("Thread {}'s threshold time set to {} based on task {}".format(self.id, self.threshold_time,
                                                                                     self.queue.second()))

    def delay_flagging(self):
        """Set work steal flags as necessary."""

        # If the current delay is above the threshold and this is the first time, set the threshold time
        if self.queue.current_delay(second=True) > self.config.DELAY_THRESHOLD and self.threshold_time is None:
            self.set_threshold_time()
            logging.debug("Thread {} crossed the threshold".format(self.id))

        # If the delay is below the threshold but the threshold time exists, empty it
        elif self.queue.current_delay(second=True) < self.config.DELAY_THRESHOLD and self.threshold_time is not None \
                and not self.flag_sent:
            self.threshold_time = None

        # Set flags if needed
        self.set_flags()

    def process_task(self, time_increment=1):
        """Process the current task for the given amount of time."""
        initial_task = self.current_task
        distracted = self.is_distracted()

        # Process the task as specified by its type
        self.current_task.process(time_increment=time_increment)

        # If completed, empty current task
        if self.current_task.complete:
            if self.current_task.is_productive:
                self.last_complete = self.state.timer.get_time()

            self.previous_task_type = type(initial_task)

            self.current_task = None

            # Park if deallocation is scheduled
            if self.scheduled_dealloc:
                self.work_search_state.park()

        # If the task just completed took no time, schedule again
        if initial_task.is_zero_duration():
            self.schedule(time_increment=time_increment)

        # Otherwise, account for the time spent
        else:
            if initial_task.preempted:
                time_increment -= 1

            if not initial_task.is_idle:
                self.time_busy += time_increment
                if type(initial_task) == WorkStealTask or \
                        type(initial_task) == WorkSearchSpin or type(initial_task) == Task:
                    self.last_interval_busy_time += time_increment

            if distracted:
                self.distracted_time += time_increment
                self.classified_time_step = True
                self.classification = "Distracted"
            if initial_task.is_productive:
                self.task_time += time_increment
                self.classified_time_step = True
                self.classification = "Task"
                self.last_interval_task_time += time_increment

        # If the task was preempted, schedule again
        if initial_task.preempted:
            self.preempted_classification = True
            self.schedule()

    def schedule(self, time_increment=1):
        """Determine how to spend the thread's time."""

        # Work on current task if there is one
        if self.is_busy():
            # Only non-new tasks should use the time_increment (new ones did not exist before this cycle)
            self.process_task(time_increment=time_increment)

            if self.enqueue_penalty > 0 and type(self.current_task) == Task:
                self.preempted_task = self.current_task
                self.current_task = EnqueuePenaltyTask(self, self.config, self.state, preempted=True)
            if self.fred_preempt and type(self.current_task) == Task:
                self.preempted_task = self.current_task
                self.current_task = RequeueTask(self, self.config, self.state)

        # If the thread must pay an enqueue penalty, do that before anything new
        elif self.enqueue_penalty > 0:
            self.current_task = EnqueuePenaltyTask(self, self.config, self.state)
            self.process_task()

        # If thread is allocating, start allocation task
        elif self.work_search_state == WorkSearchState.ALLOCATING:
            self.current_task = ReallocationTask(self, self.config, self.state)
            self.process_task()

        # Try own queue first
        elif self.work_search_state == WorkSearchState.LOCAL_QUEUE_FIRST_CHECK:

            if self.config.delay_flagging_enabled:
                self.delay_flagging()
                if self.work_steal_flag is not None:
                    self.current_task = FlagStealTask(self, self.config, self.state)

            if self.current_task is None:
                self.current_task = QueueCheckTask(self, self.config, self.state)
                self.work_search_state.set_start_time()

            self.process_task()

        # Then try stealing
        elif self.work_search_state == WorkSearchState.WORK_STEAL_CHECK:
            # If fred reallocation, check allocation status before allowing a work steal
            if self.config.fred_reallocation and \
                    self.state.total_queue_occupancy() <= len(self.state.currently_non_productive_cores()) \
                    and self.current_task != self.state.tasks[0]:
                self.state.deallocate_thread(self.id)
                return
            elif self.config.oracle_enabled:
                self.current_task = OracleWorkStealTask(self, self.config, self.state)
            else:
                self.current_task = WorkStealTask(self, self.config, self.state)
            self.process_task()

        # Check own queue one last time before parking
        elif self.work_search_state == WorkSearchState.LOCAL_QUEUE_FINAL_CHECK:
            if self.config.delay_flagging_enabled:
                self.delay_flagging()
                if self.work_steal_flag is not None:
                    self.current_task = FlagStealTask(self, self.config, self.state)

            if self.current_task is None:
                self.current_task = QueueCheckTask(self, self.config, self.state)
            self.process_task()

        # Park
        elif self.work_search_state == WorkSearchState.PARKING:
            # Make sure allocation requirements will still be met if the core is parked
            if self.config.parking_enabled and not(self.config.work_stealing_enabled and
                                                   not self.config.work_steal_park_enabled) and \
                    self.state.can_remove_buffer_core() and self.state.can_increase_delay() and \
                    not self.queue.awaiting_enqueue:
                self.state.deallocate_thread(self.id)

            # Otherwise, spend some time idle before searching again
            else:
                if self.config.work_stealing_enabled and \
                        (self.config.WORK_STEAL_CHECK_TIME != 0 or self.config.WORK_STEAL_TIME != 0):
                    self.work_search_state.reset()
                    self.schedule(time_increment=time_increment)

                # Occupy the whole time increment if needed
                else:
                    idle_time = self.config.IDLE_PARK_TIME \
                        if time_increment <= self.config.IDLE_PARK_TIME else time_increment
                    self.current_task = IdleTask(idle_time, self.config, self.state)
                    self.queue.unlock(self.id)
                    self.work_search_state.reset()
                    self.process_task()

    def get_stats(self):
        stats = [self.id, self.time_busy, self.task_time, self.work_stealing_time, self.work_steal_wait_time,
                 self.enqueue_time, self.requeue_time,
                 self.successful_ws_time, self.unsuccessful_ws_time, self.allocation_time, self.non_work_conserving_time,
                 self.distracted_time, self.unpaired_time, self.paired_time]

        if self.config.delay_flagging_enabled:
            stats += [self.flag_task_time, self.flag_wait_time]
        stats = [str(x) for x in stats]
        return stats

    @staticmethod
    def get_stat_headers(config):
        headers = ["Thread ID", "Busy Time", "Task Time", "Work Stealing Time", "Work Steal Spin Time",
                   "Enqueue Time", "Requeue Time", "Successful Work Steal Time", "Unsuccessful Work Steal Time",
                   "Allocation Time", "Non Work Conserving Time", "Distracted Time", "Unpaired Time", "Paired Time"]
        if config.delay_flagging_enabled:
            headers += ["Flag Task Time", "Flag Wait Time"]
        return headers

    def __str__(self):
        if self.work_search_state == WorkSearchState.PARKED:
            return "Thread {} (queue {}): parked".format(self.id, self.queue.id)
        elif self.is_busy():
            return "Thread {} (queue {}): busy on {}".format(self.id, self.queue.id, self.current_task)
        else:
            return "Thread {} (queue {}): idle".format(self.id, self.queue.id)

    def __repr__(self):
        return str(self)
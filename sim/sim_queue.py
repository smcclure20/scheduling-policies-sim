#!/usr/bin/env python
"""Queue object for incoming tasks."""

from work_search_state import WorkSearchState


class Queue:
    """Queue with locking capabilities."""

    # Initial lock for beginning of simulation
    # Only threads mapped to the queue can take the lock from the default
    DEFAULT_LOCK_ID = -1
    MAIN_QUEUE_ID = -1

    def __init__(self, identifier, config, state):
        self.queue = []
        self.locked = True
        self.lock_owners = [Queue.DEFAULT_LOCK_ID]
        self.id = identifier
        self.thread_ids = []
        self.locking_enabled = config.locking_enabled
        self.last_ws_check = 0
        self.awaiting_enqueue = False
        self.config = config
        self.state = state

    def set_thread(self, thread_id):
        """Add a thread to the set of threads corresponding to this queue."""
        self.thread_ids.append(thread_id)

    def swap_thread(self, current_thread, new_thread, get_lock=False):
        """Swap out a thread currently mapping to the queue for another.
        :param current_thread: Thread currently mapped to the queue to swap
        :param new_thread: New thread to swap in to mapping
        :param get_lock: If true, transfer any lock held by current_thread to the new_thread
        """
        held_lock = False
        self.thread_ids.remove(current_thread)
        if current_thread in self.lock_owners:
            self.unlock(current_thread)
            held_lock = True
        self.thread_ids.append(new_thread)

        if get_lock:
            self.try_get_lock(new_thread)

        return held_lock

    def head(self):
        """Return the first element of the queue."""
        return self.queue[0] if len(self.queue) > 0 else None

    def tail(self):
        """Return the last element of the queue."""
        return self.queue[-1] if len(self.queue) > 0 else None

    def work_available(self):
        """Return whether there are tasks currently in the queue."""
        if self.config.join_bounded_shortest_queue and self.id != self.MAIN_QUEUE_ID:
            return self.state.main_queue.length() > 0 or len(self.queue) > 0
        return len(self.queue) > 0

    def try_get_lock(self, thread_id, get_lock=True):
        """Try to acquire a lock on the queue.
        :param thread_id: ID of thread attempting to aquire the lock
        :param get_lock: If false, just determines if the thread could get the lock
        :return: True if the lock can be/is acquired, else false
        """
        # No locking
        if not self.locking_enabled:
            return True
        # Queue is not locked
        elif not self.locked:
            if get_lock:
                self.lock_owners.append(thread_id)
                self.locked = True
            return True
        # Thread already has the lock
        elif self.locked and thread_id in self.lock_owners:
            return True
        # Thread is mapped to the queue and the owner is the default lock ID
        elif self.locked and self.lock_owners[0] == Queue.DEFAULT_LOCK_ID and thread_id in self.thread_ids:
            if get_lock:
                self.lock_owners = [thread_id]
            return True
        # Thread is mapped to the queue and no non-mapped threads currently have a lock
        elif self.locked and thread_id in self.thread_ids and all(x in self.thread_ids for x in self.lock_owners):
            if get_lock:
                self.lock_owners.append(thread_id)
            return True
        # Cannot acquire the lock
        else:
            return False

    def unlock(self, thread_id):
        """Release lock held by thread.
        :param thread_id: ID of the thread releasing the lock.
        """
        if thread_id in self.lock_owners:
            self.lock_owners.remove(thread_id)
            if len(self.lock_owners) == 0:
                self.locked = False

    def enqueue(self, task, set_original=False, requeued=False, stolen=False, flag_steal=False,
                flag_time=None, threshold_time=None):
        """Enqueue a task onto the queue.
        :param task: Task to enqueue
        :param set_original: If true, set task's original queue to this queue
        :param requeued: If true, set task's requeue time
        :param stolen: If true, mark task as stolen and insert at the beginning of the queue
        :param flag_steal: If true, mark as flag stolen and set flag timers
        :param flag_time: Time that the flag requesting a steal was raised
        :param threshold_time: Time that the flag steal threshold was crossed by the original queue
        """
        if self.id != self.MAIN_QUEUE_ID:
            task.front_task_time = self.state.threads[self.get_core()].current_task.time_left \
                if self.state.threads[self.get_core()].current_task is not None else 0

        if set_original:
            task.original_queue = self.id
            task.queued_ahead = self.length() - 1 if self.length() > 0 else 0
            task.total_queue = self.state.total_queue_occupancy()

        if requeued:
            task.requeue_time = self.state.timer.get_time()

        if stolen:
            self.queue.insert(0, task)
            task.steal_count += 1
            if flag_steal:
                task.flag_steal_count += 1
                task.flag_wait_time += (self.state.timer.get_time() - flag_time)
                task.flag_set_delay += (flag_time - threshold_time)
            task.requeue_time = self.state.timer.get_time()
        else:
            self.queue.append(task)

    def dequeue(self):
        """Remove and return turn task from the front of the queue."""
        if self.id != -1 and self.config.join_bounded_shortest_queue and len(self.queue) <= self.config.QUEUE_BOUND:
            while len(self.queue) <= self.config.QUEUE_BOUND and self.state.main_queue.length() >= 1:
                self.enqueue(self.state.main_queue.dequeue(), set_original=True)
            task = self.queue.pop(0)
            return task
        return self.queue.pop(0)

    def length(self, count_current=False):
        """Return the length of the queue."""
        if count_current:
            return len(self.queue) + int(self.state.threads[self.get_core()].is_busy(search_spin_idle=True))
        return len(self.queue)

    def length_by_service_time(self):
        """Return the length of the queue as the sum of the service times present."""
        sum = 0
        for item in self.queue:
            sum += item.service_time
        return sum

    def get_threads_by_status(self, is_parked):
        """Return a list of threads mapped to the queue that match the given parking status.
        :param is_parked: Return threads that are parked if true, otherwise active
        """
        matching = []
        for thread in self.thread_ids:
            if (is_parked and self.state.threads[thread].work_search_state == WorkSearchState.PARKED) or \
                    (not is_parked and self.state.threads[thread].work_search_state != WorkSearchState.PARKED):
                matching.append(thread)
        return matching

    def update_check_counts(self):
        """Update the queue check counter for each task in the queue to track work steal checks."""
        for task in self.queue:
            task.queue_checks += 1

    def current_delay(self, second=False):
        """Return the current queueing delay (defined as time since the reference item in the queue arrived).
        :param second: If true, reference item is second task. Otherwise, its the head.
        """
        reference = self.second() if second else self.head()
        if reference is None:
            return 0
        return self.state.timer.get_time() - reference.arrival_time

    def sort_by_arrival(self):
        """Sort the queue by the arrival time of tasks."""
        self.queue.sort(key=lambda x: x.arrival_time)

    def second(self):
        """Return the second item in the queue if it exists."""
        return self.queue[1] if len(self.queue) >= 2 else None

    def get_core(self, all=False):
        """Return the first core assigned to this queue or all if specified.
        :param """
        if all:
            return self.thread_ids
        return self.thread_ids[0]

    def first_with_to_enqueue(self):
        """Return the first task in the queue with a to_enqueue flag."""
        for task in self.queue:
            if task.to_enqueue is not None and task.to_enqueue != self.id:
                self.queue.remove(task)
                return task
        return None

    def __str__(self):
        if self.locked:
            return "Queue {} [locked] (len: {})".format(self.id, len(self.queue))
        else:
            return "Queue {} [unlocked] (len: {})".format(self.id, len(self.queue))

    def __repr__(self):
        return str(self)
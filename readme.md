# Microsecond-Scale Task Simulator

## Naming and Organization
Simulation code resides in`/sim/`. Raw results are stored in the `results` directory in a directory named after the simulation name.

Simulations are named with the following convention: `<hostname>_<date_time>` or `<hostname>_<date_time>_t<thread_number>` for parallelized runs. `sim_` is prepended to the name for results subdirectories.

## To Run
You should be in the `/sim/` directory

#### To run a single simulation:
`python3 simulation.py <config_file> <description (optional)>`

Flags: `-d` for debug output to standard out.

#### To run multiple simulations in parallel:

`python3 run_sim.py <config_file_path> <options: -varycores, description>`

##### Optional arguments:

`-varycores`: Vary the number of cores in each simulation rather than varying the load.

`description`: String to describe the simulation group. This will be written in `results/meta_log`

Example: `python3 run_sim.py config.json -varycores "work stealing static allocations, 400ns overhead`

## Configuration

#### Configuration Parameter Dictionary

##### System Parameters
* `avg_system_load`: (float) Offered load
* `num_queues`: (int) Number of queues
* `num_threads`: (int) Number of threads
* `mapping`: (list[int]) Mapping from threads to queues. List of queue numbers indexed by thread number (i.e., mapping[thread #] = queue_number)
* `load_thread_count`: (int) Number of cores to base the offered load on (allows actual cores and load to differ)
* `num_tasks`: (int) Number of tasks in the simulation. Ends after this number of tasks completed or duration expires.
* `sim_duration`: (int) Total simulated time in nanoseconds. Simulation ends after this many nanoseconds of simulated time or when the number of tasks runs out.
* `locking_enabled`: (bool) Whether queue locks must be acquired or not
* `work_stealing_enabled`: (bool) Enables work stealing
* `parking_enabled`: (bool) Whether cores are allowed to park or not
* `fast_forward_enabled`: (bool) Whether the simulation can skip time increments in which nothing will happen (not clear why this should ever be false)
* `record_alocations`: (bool) Record time of allocations/allocation decisions and write to `reallocation_record`
* `reallocation_record`: (string) File path to write recorded allocation schedule
* `record_steals`: (bool) Record time of steals
* `buffer_cores_enabled`: (bool) Enables reallocation policy in which a buffer of extra cores is maintained. Only one reallocation policy may be enabled.
* `delay_range_enabled`: (bool) Enables reallocation policy in which cores are added/removed to maintain the range of average queueing delay. Only one reallocation policy may be enabled.
* `ws_self_checks`: (bool) Whether cores check their own cores during the work stealing process.
* `allocation_delay`: (bool) Whether there is a delay to allocating cores.
* `two_choices`: (bool) Enables two choices when finding cores to work steal from.
* `oracle_enabled`: (bool) Enables work stealing oracle which can find the best core to steal from immediately.
* `delay_flagging_enabled`: (bool) Enables load balancing policy in which loaded cores flag other cores for assistance.
* `enqueue_choice`: (bool) Enables load balancing policy in which arriving tasks are placed onto the least loaded core of available options.
* `random_work_steal_search`: (bool) If enabled, work steal checks occur in random order.
* `regular_arrivals`: (bool) If enabled, tasks arrive at regular intervals rather than as a Poisson process.
* `constant_service_time`: (bool) If enabled, tasks have a constant service time rather than exponentially distributed service times.
* `ws_sibling_first`: (bool) If enabled, cores check their sibling core first when work stealing.
* `enqueue_by_st_sum`: (bool) If enabled, tasks are enqueued onto the queue with the lowest sum of service times.
* `always_check_realloc`: (bool) If enabled, the system checks if it should reallocate cores constantly rather than at fixed intervals.
* `ideal_flag_steal`: (bool) If enabled, flagging cores are able to flag the least-loaded core and stolen tasks are sorted by arrival time.
* `delay_range_by_service_time`: (bool) If enabled, delay range is enforced by the sum of service times rather than queueing delay.
* `ideal_reallocation_enabled`: (bool) Enables the reallocation policy in which the number of cores is set to match the number of tasks in the system. Only one reallocation policy may be enabled.
* `fred_reallocation`: (bool) Enables the reallocation policy in which cores are allocated per-task. Requires `always_check_realloc` to be enabled. Only one reallocation policy may be enabled.
* `spin_parking_enabled`: (bool) If enabled, cores may park after spinning to fill out the required work stealing time.
* `utilization_range_enabled`: (bool) Enables the reallocation policy in which a range of CPU utilization is maintained. Only one reallocation policy may be enabled.
* `bimodal_service_time`: (bool) If enabled, tasks are generated according to a bimodal distribution rather than an exponential distribution.
* `join_bounded_shortest_queue`: (bool) Enables the load balancing policy in which tasks join a central queue and individual queues pull to maintain a certain length.
* `record_queue_lens`: (bool) If enabled, record queue lengths at each reallocation decision.

##### Constants
* `AVERAGE_SERVICE_TIME`: (int) Average service time of tasks in ns.
* `WORK_STEAL_CHECK_TIME`: (int) Time to check if a core can be stolen from in ns.
* `WORK_STEAL_TIME`: (int) Time to steal from another core in ns.
* `MINIMUM_WORK_SEARCH_TIME`: (int) Minimum time that a core must work steal before parking in ns.
* `LOCAL_QUEUE_CHECK_TIME`: (int) Time to check local queue for tasks in ns.
* `ALLOCATION_TIME`: (int) Time required to allocate a core in ns.
* `BUFFER_CORE_COUNT_MIN`: (int) Minimum number of buffer cores allowed. If both count and percent are defined, takes the higher value.
* `BUFFER_CORE_COUNT_MAX`: (int) Maximum number of buffer cores allowed. If both count and percent are defined, takes the count value.
* `BUFFER_CORE_PCT_MIN`: (int) Minimum percent of cores that should be buffer cores. If both count and percent are defined, takes the higher value.
* `BUFFER_CORE_PCT_MAX`: (int) Maximum percent of cores that should be buffer cores. If both count and percent are defined, takes the count value.
* `REALLOCATION_THRESHOLD_MIN`: (int) Minimum allowed queueing delay for delay range in ns.
* `REALLOCATION_THRESHOLD_MAX`: (int) Maximum allowed queueing delay for delay range in ns.
* `DELAY_THRESHOLD`: (int) Queueing delay threshold to trigger flagging in ns.
* `FLAG_STEAL_DELAY`: (int) Overhead for flag stealing in ns.
* `ENQUEUE_CHOICES`: (int) Number of enqueue choices.
* `ENQUEUE_PENALTY`: (int) Overhead for determining enqueue location in ns.
* `ALLOCATION_PAUSE`: (int) Time before another allocation decision can be made if a core is added in ns.
* `ALLOCATION_THRESHOLD`: (int) Threshold for queueing delay which causes a core to be added in default core allocation policy in ns.
* `REQUEUE_PENALTY`: (int) Overhead for moving a task between queues in ns. Applies to per-task movement of tasks.
* `UTILIZATION_MIN`: (int) Minimum allowable CPU utilization in utilization range.
* `UTILIZATION_MAX`: (int) Maximum allowable CPU utilization in utilization range.
* `FLAG_OPTIONS`: (int) Number of options when flagging.
* `WORK_STEAL_CHOICES`: (int) Number of choices when work stealing.
* `QUEUE_BOUND`: (int) Bounded size of queue for JBSQ.


## To Analyze Results
`python3 analysis.py <simulations> <output_file> <ignored_time>`

Arguments:
* simulations: Specifies simulations to analyze. Can be (1) a file with a list of simulation names, (2) the name of a single run, or (3)
 the name of a run with multiple parallel threads.
* output_file: Output file for results
* ignored_time: Percentage of simulation time (as an int) to drop from the beginning of the data.

## To Delete Old Results
Removes output files associated with the simulation and removes the line in the meta log.
`python3 del_old_results.py <run_name>`

Flags:
* `-delconf`: Delete the saved version of the configuration file as well.

Deletes configuration record for the given simulation and deletes the associated line in the meta log.
`python3 del_config_record.py <run_name>`






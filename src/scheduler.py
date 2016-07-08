#!/usr/bin/env python

import logging
import multiprocessing
import Queue as threadqueue
import threading
import time
import src.const as const

class Message(object):
    """
    Base class for message task. Takes dictionary of values to return, note that values must be
    pickle-able, even better if it is just single string text, or a list.
    """
    def __init__(self, status, **kwargs):
        self.status = status
        self.arguments = kwargs
        self.pretty_name = u"%s[%s]%s" % (type(self).__name__, self.status, self.arguments)

    def __str__(self):
        return self.pretty_name

    def __unicode__(self):
        return self.pretty_name

    def __repr__(self):
        return self.pretty_name

# Set of messages for executors
MESSAGE_SHUTDOWN = "SHUTDOWN"
# Message to cancel task
MESSAGE_TASK_CANCEL = "TASK_CANCEL"
# Message received from executors with task status
MESSAGE_TASK_STARTED = "TASK_STARTED"
MESSAGE_TASK_FINISHED = "TASK_FINISHED"
MESSAGE_TASK_CANCELLED = "TASK_CANCELLED"

class TerminationException(Exception):
    """
    Custom exception for executor termination or any other valid termination.
    """
    def __init__(self, message=None):
        msg = message if message else "Requested termination"
        super(TerminationException, self).__init__(msg)

class Task(object):
    """
    Task is a unit of execution in scheduler, all backends should implement this interface for
    their execution requests, so they can be launched on executor. Note that some methods might
    require availability status of backend. Main task API consists of several methods:
    - status()
    - async_launch()
    - cancel()

    Also each task must overwrite property for a unique identifier, and exit_code to return status
    of finished task.
    """

    # == Task statuses ==
    # Task is blocked, e.g. by backend availability, will not be scheduled until resolved
    BLOCKED = "BLOCKED"
    # Task is pending, next after BLOCKED status, meaning good to launch
    PENDING = "PENDING"
    # Task is running on backend
    RUNNING = "RUNNING"
    # Task is finished, either failed or succeeded, which is determined by exit code
    FINISHED = "FINISHED"

    @property
    def uid(self):
        """
        Return unique identifier for this task, it is recommended to be globally unique.

        :return: unique identifier
        """
        raise NotImplementedError()

    @property
    def exit_code(self):
        """
        Return exit code (status) of the task. If task has not been launched or running, should
        return None, and return valid status in case it was finished or terminated.

        :return: exit code
        """
        raise NotImplementedError()

    def status(self):
        """
        Return current status of the task. Should take into account backend availability, state of
        the resources allocated, or background process running.

        :return: enum status of the task (see above)
        """
        raise NotImplementedError()

    def async_launch(self):
        """
        Asynchronously launch task, because executor does not support launching tasks in a separate
        thread yet. For example, spawning shell process in the background, etc.
        """
        raise NotImplementedError()

    def cancel(self):
        """
        Cancel task, this should ask termination of all background processes and close resources
        associated with task. Can block thread to execute code, must set exit code for the task.
        """
        raise NotImplementedError()

class Executor(multiprocessing.Process):
    """
    Executor process to run tasks and receive messages from scheduler. It represents long running
    process with polling interval, all communications are done through pipe. Executor guarantees
    termination of task with termination of subprocess, assuming that task implements interface
    correctly. Takes dictionary of task queues that are mapped to priorities, higher priority is
    checked first.
    """
    def __init__(self, name, conn, task_queue_map, timeout=0.5, logger=None):
        """
        Create instance of Executor.

        :param name: friendly executor name, e.g. "executor-1"
        :param conn: Connection instance to receive and send messages
        :param task_queue_map: task queue as a dict [priority: queue]
        :param timeout: polling interval
        :param logger: provided logger, if None then default logger is used
        """
        self.name = name
        self.conn = conn
        self.task_queue_map = task_queue_map
        self.timeout = timeout
        # if no logger defined create new logger and add null handler
        if logger:
            self.logger = logger
        else:
            log_name = "%s[%s]" % (type(self).__name__, self.name)
            self.logger = self._get_default_logger(log_name)
        # we also keep reference to active task, this will be reassigned for every iteration
        self.active_task = None
        # list of task ids to cancel, we add new task_id when specific message arrives and remove
        # task_id that has been removed
        self.cancel_task_ids = set()
        # flag to indicate if executor is terminated
        self._terminated = False
        super(Executor, self).__init__(name=name)

    def _get_default_logger(self, name):
        """
        Internal method to set default logger. Should be moved into utility functions or unified
        package instead. Creates logger for name provided.

        :param name: logger name
        :return: default logger
        """
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        form = logging.Formatter("LOG :: %(asctime)s :: %(name)s :: %(levelname)s :: %(message)s")
        stderr = logging.StreamHandler()
        stderr.setFormatter(form)
        logger.addHandler(stderr)
        return logger

    def iteration(self):
        """
        Run single iteration, entire logic of executor should be specified in this method, unless
        there is an additional logic between iterations. Iteration is cancelled, if executor is
        terminated.

        :return: boolean flag, True - run next iteration, False - terminate
        """
        # we process special case of terminated executor in case someone would launch it again.
        if self._terminated:
            self.logger.warning("Executor has been terminated, iteration is cancelled")
            return False
        self.logger.debug("Run iteration for %s, timeout=%s", self.name, self.timeout)
        try:
            # check if there are any messages in connection
            if self.conn.poll():
                self._process_message(self.conn.recv())
            # check if there is any outstanding task to run, otherwise poll data for current task
            self._process_task()
        except TerminationException:
            self.logger.info("Requested termination of executor %s", self.name)
            self._terminated = True
            # If we encounter termination of executor, we should cancel current running task and
            # set block launch of any other tasks
            self._cancel_task()
            return False
        # pylint: disable=W0703,broad-except
        except Exception as e:
            self.logger.exception("Unrecoverable error %s, shutting down executor %s", e, self.name)
            return False
        # pylint: enable=W0703,broad-except
        else:
            return True

    def _process_message(self, msg):
        """
        Process message and take action, e.g. terminate process, execute callback, etc. Message
        types are defined above in the package. Note that this can take actions on tasks, e.g.
        when task is cancelled, so the subsequent processing of task, will work with updated state.

        :param msg: message to process
        """
        self.logger.debug("Received message %s", msg)
        if isinstance(msg, Message):
            if msg.status == MESSAGE_SHUTDOWN: # pragma: no branch
                raise TerminationException()
            elif msg.status == MESSAGE_TASK_CANCEL: # pragma: no branch
                # update set of tasks to cancel
                if "task_id" in msg.arguments:
                    self.cancel_task_ids.add(msg.arguments["task_id"])
            else:
                # valid but unrecognized message, no-op
                pass
        else:
            self.logger.info("Invalid message %s is ignored", msg)

    def _get_new_task(self):
        """
        Extract new task from priority list of queues. If no tasks found for priority or priority
        does not exist in dictionary, next priority is checked. If task is found, it is returned,
        otherwise None.

        :return: new available task across priorities
        """
        for priority in const.PRIORITIES:
            self.logger.debug("Searching task in queue for priority %s" % priority)
            try:
                return self.task_queue_map[priority].get(block=False)
            except threadqueue.Empty:
                self.logger.debug("No tasks available in queue for priority %s" % priority)
            except KeyError:
                self.logger.debug("Non-existent priority %s skipped" % priority)
        return None

    def _process_task(self):
        """
        Process individual task, returns exit code for each task following available API. One of
        the checks is performed to test current task_id against cancelled list, and discard task,
        if it has been marked as cancelled, or terminate running task.

        :return: task exit code (see Task API), or None, if exit code is not resolved
        """
        if not self.active_task:
            self.active_task = self._get_new_task()
        # At this point we check if we can actually launch task
        exit_code = None
        if self.active_task:
            task_id = self.active_task.uid
            status = self.active_task.status()
            # before checking statuses and proceed execution, we check if current task was
            # requested to be cancelled, if yes, we remove it from set of ids.
            if task_id in self.cancel_task_ids:
                self._cancel_task()
                self.cancel_task_ids.discard(task_id)
            elif status == Task.PENDING:
                # now we need to launch it and log action, note that launch should be asynchronous
                self.active_task.async_launch()
                self.conn.send(Message(MESSAGE_TASK_STARTED, task_id=task_id))
                self.logger.info("Started task %s", task_id)
                exit_code = None
            elif status == Task.RUNNING:
                # task is running, nothing we can do, but wait
                exit_code = None
            elif status == Task.FINISHED:
                exit_code = self.active_task.exit_code
                self.conn.send(Message(MESSAGE_TASK_FINISHED, task_id=task_id, exit_code=exit_code))
                self.logger.info("Finished task %s, exit_code=%s", task_id, exit_code)
                self.active_task = None
            else:
                self.logger.info("Blocked task %s, ping under-system implementation", task_id)
        # Return exit code obtained from if-else branches, note that if executor does not have any
        # tasks to run, we return None similar to when task is running.
        return exit_code

    def _cancel_task(self):
        """
        Cancel current running task, if available.
        """
        if self.active_task:
            task_id = self.active_task.uid
            self.active_task.cancel()
            self.conn.send(Message(MESSAGE_TASK_CANCELLED, task_id=task_id))
            self.logger.info("Cancelled task %s", task_id)
            self.active_task = None
        else:
            self.logger.info("No active task to terminate")

    def run(self):
        """
        Method to run tasks on executor, this runs in iterations with each timeout interval. Each
        iteration polls new messages from connection and checks running task. If iteration fails we
        immediately return status False.
        """
        self.logger.info("Start executor %s, time=%s", self.name, time.time())
        proceed = True
        while proceed: # pragma: no branch
            proceed = self.iteration()
            if not proceed:
                return False
            time.sleep(self.timeout)

class Scheduler(object):
    """
    Scheduler class prepares and launches executors and provides means to pass and process tasks
    and messages from executors. Should be one instance per application.
    """
    def __init__(self, num_executors, timeout=0.5, logger=None):
        """
        Create new instance of Scheduler.

        :param num_executors: number of executors to initialize
        :param timeout: executor's timeout
        :param logger: executor's logger
        """
        self.num_executors = int(num_executors)
        self.timeout = timeout
        self.logger = logger
        # pipe connections to send and receive messages to/from executors
        self.pipe = {}
        # list of executors that are initialized
        self.executors = []
        # initialize priority queues, the lower number means higher priority
        self.task_queue_map = {
            const.PRIORITY_0: multiprocessing.Queue(),
            const.PRIORITY_1: multiprocessing.Queue(),
            const.PRIORITY_2: multiprocessing.Queue()
        }

    def _prepare_executor(self, name):
        """
        Prepare single executor, this creates connection for executor and launches it as daemon
        process, and appends to executors list.

        :param name: executor's name
        :return: created executor (it is already added to the list of executors)
        """
        main_conn, exec_conn = multiprocessing.Pipe()
        exc = Executor(name, exec_conn, self.task_queue_map, timeout=self.timeout,
                       logger=self.logger)
        exc.daemon = True
        self.executors.append(exc)
        self.pipe[exc.name] = main_conn
        return exc

    def _prepare_polling_thread(self, name, target=None):
        """
        Prepare maintenance thread for polling messages from Pipe. This returns None, when no
        target consumer is provided.

        :param name: name of the polling thread
        :param target: target function that should accept argument as list of messages, can be None
        :return: created daemon thread or None, if target is not specified
        """
        # Do not create any thread, if there is no target provided, because of saving resources
        # on polling messages without consumer.
        if not target:
            return None
        # Thread is considered to be long-lived, and is terminated when scheduler is stopped.
        def poll_messages():
            while True:
                msg_list = []
                for conn in self.pipe.values():
                    while conn.poll():
                        msg_list.append(conn.recv())
                target(msg_list)
                time.sleep(self.timeout)
        thread = threading.Thread(name=name, target=poll_messages)
        thread.daemon = True
        return thread

    def start(self):
        """
        Start scheduler, launches executors asynchronously.
        """
        # Launch executors and save pipes per each
        for i in range(self.num_executors):
            exc = self._prepare_executor("Executor-%s" % i)
            exc.start()

    def start_maintenance(self, polling_target=None):
        """
        Start all maintenance threads and processes.
        """
        # Launch polling thread for messages
        thread = self._prepare_polling_thread("Polling-1", target=polling_target)
        if thread:
            thread.start()

    def put(self, priority, task):
        """
        Add task for priority.

        :param priority: priority of task, must be one of the const.PRIORITY_x
        :param task: task to add, must be instance of Task
        """
        if priority not in self.task_queue_map:
            raise KeyError("No priority %s found in queue map" % priority)
        if not isinstance(task, Task):
            raise TypeError("%s != Task" % type(task))
        self.task_queue_map[priority].put(task, block=False)

    def cancel(self, task_id):
        """
        Cancel task by provided task_id, this includes either termination of currently running task,
        or removal of future scheduled tasks, note that this will be no-op if task that has been
        already processed.

        :param task_id: task id to cancel, no-op if task_id is None
        """
        if task_id:
            for conn in self.pipe.values():
                conn.send(Message(MESSAGE_TASK_CANCEL, task_id=task_id))

    def stop(self):
        """
        Stop scheduler, terminates executors, and all tasks that were running at the time.
        """
        for conn in self.pipe.values():
            conn.send(Message(MESSAGE_SHUTDOWN))
        # timeout to terminate processes and process remaining messages in Pipe by polling thread
        time.sleep(5)
        for exc in self.executors:
            if exc.is_alive():
                exc.terminate()
            exc.join()
        self.pipe = None
        self.executors = None
        self.task_queue_map = None

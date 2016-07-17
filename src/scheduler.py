#!/usr/bin/env python

import inspect
import multiprocessing
import Queue as threadqueue
import threading
import time
import src.const as const
import src.util as util

# == Task statuses ==
# Task is pending, ready to execute block
TASK_PENDING = "PENDING"
# Task is started and running on backend
TASK_STARTED = "STARTED"
# Task is cancelled
TASK_CANCELLED = "CANCELLED"
# Task is succeeded
TASK_SUCCEEDED = "SUCCEEDED"
# Task is failed to execute
TASK_FAILED = "FAILED"

# == Message statuses ==
# Action requests for executor
EXECUTOR_IS_ALIVE = "EXECUTOR_IS_ALIVE"
EXECUTOR_SHUTDOWN = "EXECUTOR_SHUTDOWN"
EXECUTOR_CANCEL_TASK = "EXECUTOR_CANCEL_TASK"
# Statuses of actions taken by executor (events of task execution)
EXECUTOR_TASK_STARTED = "EXECUTOR_TASK_STARTED"
EXECUTOR_TASK_SUCCEEDED = "EXECUTOR_TASK_SUCCEEDED"
EXECUTOR_TASK_FAILED = "EXECUTOR_TASK_FAILED"
EXECUTOR_TASK_CANCELLED = "EXECUTOR_TASK_CANCELLED"

class Task(object):
    """
    Task class is a public API for creating executable code within executor TaskThread. Must be
    serializable, and must implement methods:
    - uid
    - priority
    - run()
    - cancel()
    Task defines the running code for unit of execution, note that it is assumed to be blocking,
    though should handle asynchronous calls of `cancel()` method correctly.
    """
    @property
    def uid(self):
        """
        Unique identifier for task.

        :return: globally unique task id
        """
        raise NotImplementedError("Not implemented")

    @property
    def priority(self):
        """
        Priority of the task. Must be one of the PRIORITY_0, PRIORITY_1, PRIORITY_2.

        :return: task priority
        """
        raise NotImplementedError("Not implemented")

    def run(self):
        """
        Main method within Block code executes. This method should block and wait for it to
        complete. Example might be a polling status of the running process. Any failures can result
        in exception, it is recommended to rather fail instead of silencing exception, because
        Task will capture failure and return correct status for code block.
        """
        raise NotImplementedError("Not implemented")

    def cancel(self):
        """
        Cancel currently running block. This should shut down main process in `run()`, though this
        operation will run after task is cancelled, so it is not going to block task process, since
        task will exit before shutdown. Note that it is important that Block provides this method,
        otherwise task will be cancelled without terminating actual process.
        """
        raise NotImplementedError("Not implemented")

class InterruptedException(Exception):
    """
    Base class for interrupted exceptions, these errors are only raised when either task or
    executor are asked to terminate, in case of task it will be cancellation. Note that this only
    covers expected or scheduled failures, any other exceptions are thrown without wrapping into
    InterruptedException.
    """
    pass

class TaskInterruptedException(InterruptedException):
    """
    Interrupted exception for task, thrown when task is cancelled.
    """
    pass

class ExecutorInterruptedException(InterruptedException):
    """
    Interrupted exception for executor, thrown when executor is requested to shut down.
    """
    pass

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

class WorkerThread(threading.Thread):
    """
    Non-daemon thread to execute Task instance. Any exception is captured and sent back to a
    task thread. Communication with the task thread goes through message queue, which is created
    before launching thread.
    """
    def __init__(self, task, msg_queue):
        """
        Create new instance of WorkerThread.

        :param task: Task instance to run
        :param msg_queue: message queue, mainly to store error messages
        """
        super(WorkerThread, self).__init__()
        self._task = task
        self._msg_queue = msg_queue

    def run(self):
        # pylint: disable=W0703,broad-except
        try:
            self._task.run()
        except Exception as e:
            self._msg_queue.put_nowait(e)
        # pylint: enable=W0703,broad-except

    def cancel(self):
        self._task.cancel()

class TaskThread(threading.Thread):
    """
    TaskThread is a container for task as a unit of execution. It is essentially a daemon thread
    that spawns another worker thread to execute task block. Life cycle of the task is reflected in
    set of statuses:
    TASK_PENDING -> TASK_STARTED -> TASK_FAILED/TASK_SUCCEEDED
                                 -> TASK_CANCELLED
    When task is created it is assigned TASK_PENDING status, TASK_STARTED is assigned when task is
    launched, we also start collecting metrics, TASK_FAILED/TASK_SUCCEEDED is returned when worker
    thread is finished; if there is any message in msg_queue as an exception, task is considered
    failed, otherwise succeeded. Task can also be cancelled during execution. Note that task does
    not guarantee that worker thread will exit correctly, this depends on actual implementation.
    """
    def __init__(self, task, logger=None):
        """
        Create new instance of TaskThread. Note that task block must be serializable with pickle.

        :param task: code to execute for this task thread
        :param logger: available logger function, uses default if None
        """
        super(TaskThread, self).__init__()
        # task is by definition a daemon thread
        self.daemon = True
        # refresh timeout for worker thread
        self.refresh_timeout = 0.5
        if not isinstance(task, Task):
            raise AttributeError("Invalid task provided: %s" % task)
        self.__uid = task.uid
        self.__task = task
        self.__metrics = {}
        self.__status = TASK_PENDING
        # setting up logger
        self.name = "Task[%s]" % self.__uid
        self.logger = logger(self.name) if logger else util.get_default_logger(self.name)
        # different work statuses
        self.__cancel = threading.Event()
        # callbacks
        # .. note:: DeveloperApi
        self.on_task_started = None
        self.on_task_cancelled = None
        self.on_task_succeeded = None
        self.on_task_failed = None

    @property
    def uid(self):
        """
        Get unique identifier.

        :return: unique identifier for this task
        """
        return self.__uid

    @property
    def status(self):
        """
        Get current task status.

        :return: status for this task
        """
        return self.__status

    def _set_metric(self, name, value):
        """
        Set metric value for name, this overwrites previous value.

        :param name: name for metric
        :param value: new value for metric
        """
        self.__metrics[name] = value

    def _get_metric(self, name):
        """
        Get metric value for name, or None if name is not found.

        :return: metric value or None in case of absent name
        """
        return self.__metrics[name] if name in self.__metrics else None

    def cancel(self):
        """
        Cancel current thread and potentially running task.
        """
        self.logger.debug("Requested cancellation of task")
        self.__cancel.set()

    @property
    def is_cancelled(self):
        """
        Return True, if thread is either cancelled, or has been requested to stop.

        :return: True if cancel condition is triggered, False otherwise
        """
        return self.__cancel.is_set()

    def _safe_exec(self, func, **kwargs):
        """
        Safely execute function with a list of arguments. Function is assumed not to return any
        result.

        :param func: function to execute
        :param kwargs: dictionary of method parameters
        """
        # pylint: disable=W0703,broad-except
        try:
            if func:
                func(**kwargs)
        except Exception as e:
            self.logger.debug("Failed to execute '%s(%s)', reason=%s", func, kwargs, e)
        # pylint: enable=W0703,broad-except

    def run(self):
        # update task metrics and set status
        self._set_metric("starttime", time.time())
        self._set_metric("duration", 0)
        self.__status = TASK_STARTED
        # try launching listener callback, note that failure should not affect execution of task
        self._safe_exec(self.on_task_started, uid=self.__uid)
        self.logger.debug("Started, time=%s", self._get_metric("starttime"))
        try:
            msg_queue = threadqueue.Queue()
            wprocess = WorkerThread(self.__task, msg_queue)
            wprocess.start()
            next_iteration = True
            while next_iteration:
                time.sleep(self.refresh_timeout)
                if self.is_cancelled:
                    wprocess.cancel()
                    raise TaskInterruptedException()
                if not wprocess.is_alive():
                    next_iteration = False
            wprocess.join()
            if not msg_queue.empty():
                # we only care about the first exception occuried
                error = msg_queue.get_nowait()
                raise error
        except TaskInterruptedException:
            # task has been cancelled or requested termination
            self.__status = TASK_CANCELLED
            self._safe_exec(self.on_task_cancelled, uid=self.__uid)
        # pylint: disable=W0703,broad-except
        except Exception as e:
            # any other exception is considered a failure
            self._set_metric("reason", "%s" % e)
            self.logger.debug("Failure reason=%s", self._get_metric("reason"))
            self.__status = TASK_FAILED
            self._safe_exec(self.on_task_failed, uid=self.__uid, reason=self._get_metric("reason"))
        # pylint: enable=W0703,broad-except
        else:
            self.__status = TASK_SUCCEEDED
            self._safe_exec(self.on_task_succeeded, uid=self.__uid)
        finally:
            # set post-execution metrics for task
            self._set_metric("endtime", time.time())
            duration = self._get_metric("endtime") - self._get_metric("starttime")
            self._set_metric("duration", duration)
        self.logger.debug("Finished, status=%s, time=%s, duration=%s", self.__status,
                          self._get_metric("endtime"), self._get_metric("duration"))

class Executor(multiprocessing.Process):
    """
    Executor process to run tasks and receive messages from scheduler. It represents long running
    daemon process with polling interval, all communications are done through pipe. Executor
    guarantees termination of task with termination of subprocess, assuming that task implements
    interface correctly. Takes dictionary of task queues that are mapped to priorities, higher
    priority is checked first.
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
        super(Executor, self).__init__()
        self.name = "%s[%s]" % (type(self).__name__, name)
        self.daemon = True
        self.conn = conn
        self.task_queue_map = task_queue_map
        self.timeout = timeout
        # if no logger defined create new logger and add null handler
        self.log_func = logger
        self.logger = logger(self.name) if logger else util.get_default_logger(self.name)
        # we also keep reference to active task, this will be reassigned for every iteration
        self._active_task = None
        # list of task ids to cancel, we add new task_id when specific message arrives and remove
        # task_id that has been removed
        self._cancel_task_ids = set()
        # flag to indicate if executor is terminated
        self._terminated = False

    def _process_message(self, msg):
        """
        Process message and take action, e.g. terminate process, execute callback, etc. Message
        types are defined above in the package. Note that this can take actions on tasks, e.g.
        when task is cancelled, so the subsequent processing of task, will work with updated state.
        :param msg: message to process
        """
        self.logger.debug("Received message %s", msg)
        if isinstance(msg, Message):
            if msg.status == EXECUTOR_SHUTDOWN: # pragma: no branch
                raise ExecutorInterruptedException("Executor shutdown")
            elif msg.status == EXECUTOR_CANCEL_TASK: # pragma: no branch
                # update set of tasks to cancel
                if "task_id" in msg.arguments: # pragma: no branch
                    task_id = msg.arguments["task_id"]
                    self._cancel_task_ids.add(task_id)
                    self.logger.debug("Registered cancelled task %s", task_id)
            else:
                # valid but unrecognized message, no-op
                pass
        else:
            self.logger.info("Invalid message %s is ignored", msg)

    def _respond_is_alive(self):
        """
        Send "is alive" response to the scheduler with current timestamp.
        """
        if self.conn: # pragma: no branch
            self.conn.send(Message(EXECUTOR_IS_ALIVE, datetime=util.utcnow(), name=self.name))

    def _get_new_task(self):
        """
        Extract new task from priority list of queues. If no tasks found for priority or priority
        does not exist in dictionary, next priority is checked. If task is found, it is returned,
        otherwise None. For each task TaskThread is created to provide status and metrics updates.

        :return: new available task across priorities
        """
        task = None
        for priority in const.PRIORITIES:
            self.logger.debug("Searching task in queue for priority %s" % priority)
            try:
                task = self.task_queue_map[priority].get(block=False)
            except threadqueue.Empty:
                self.logger.debug("No tasks available in queue for priority %s" % priority)
            except KeyError:
                self.logger.debug("Non-existent priority %s skipped" % priority)
            else:
                if task: # pragma: no branch
                    break
        # create thread for task
        task_thread = TaskThread(task, self.log_func) if task else None
        return task_thread

    def _process_task(self):
        """
        Process individual task, returns exit code for each task following available API. One of
        the checks is performed to test current task_id against cancelled list, and discard task,
        if it has been marked as cancelled, or terminate running task.
        """
        if not self._active_task:
            self._active_task = self._get_new_task()
            self.logger.warning("New task registered")
        # before checking statuses and proceed execution, we check if current task was
        # requested to be cancelled, if yes, we remove it from set of ids.
        if self._active_task and self._active_task.uid in self._cancel_task_ids:
            self._cancel_task_ids.discard(self._active_task.uid)
            self._cancel_active_task()
        # check general task processing
        if self._active_task:
            task_id = self._active_task.uid
            task_status = self._active_task.status
            # perform action based on active task status
            if task_status is TASK_PENDING:
                # check if external system is available to run task (Developer API)
                if self.external_system_available():
                    self._active_task.start()
                    self.conn.send(Message(EXECUTOR_TASK_STARTED, task_id=task_id))
                    self.logger.info("Started task %s", task_id)
                else:
                    self.logger.info("External system is not available, will try again later")
            elif task_status is TASK_STARTED:
                # task has started and running
                if self._active_task.is_alive(): # pragma: no branch
                    self.logger.debug("Ping task %s is alive", task_id)
            elif task_status is TASK_SUCCEEDED:
                # task finished successfully
                self.conn.send(Message(EXECUTOR_TASK_SUCCEEDED, task_id=task_id))
                self.logger.info("Finished task %s, status %s", task_id, task_status)
                self._active_task = None
            elif task_status is TASK_FAILED:
                # task failed
                self.conn.send(Message(EXECUTOR_TASK_FAILED, task_id=task_id))
                self.logger.info("Finished task %s, status %s", task_id, task_status)
                self._active_task = None
            elif task_status is TASK_CANCELLED:
                # task has been cancelled
                if self._active_task: # pragma: no branch
                    self._active_task = None
            else:
                self.logger.warning("Unknown status %s for task %s", task_status, task_id)
        else:
            self.logger.debug("No active task registered")

    def _cancel_active_task(self):
        """
        Cancel current running task, if available.
        """
        if self._active_task:
            task_id = self._active_task.uid
            self._active_task.cancel()
            self.conn.send(Message(EXECUTOR_TASK_CANCELLED, task_id=task_id))
            self.logger.info("Cancelled task %s", task_id)
            self._active_task = None
        else:
            self.logger.info("No active task to cancel")

    def external_system_available(self):
        """
        .. note:: DeveloperApi

        Can be overriden to check if external system is available to run task. This can include
        system status, e.g. running, or system load, e.g. how many tasks are already queued up.

        :return: True if system can run task, False otherwise
        """
        return True

    def iteration(self):
        """
        Run single iteration, entire logic of executor should be specified in this method, unless
        there is an additional logic between iterations. Iteration is cancelled, if executor is
        terminated.

        :return: boolean flag, True - run next iteration, False - terminate
        """
        # we process special case of terminated executor in case someone would launch it again.
        if self._terminated:
            self.logger.warning("Executor %s has been terminated", self.name)
            return False
        self.logger.debug("Run iteration for %s, timeout=%s", self.name, self.timeout)
        try:
            # send reponse to the scheduler that this executor is up and processing tasks
            self._respond_is_alive()
            # check if there are any messages in connection, process one message per iteration
            if self.conn.poll():
                self._process_message(self.conn.recv())
            # check if there is any outstanding task to run, otherwise poll data for current task
            self._process_task()
        except ExecutorInterruptedException:
            self.logger.info("Requested termination of executor %s", self.name)
            self._terminated = True
            # cancel task that is currently running and clean up state
            self._cancel_active_task()
            return False
        # pylint: disable=W0703,broad-except
        except Exception as e:
            self.logger.exception("Unrecoverable error %s, terminating executor %s", e, self.name)
            self._terminated = True
            return False
        # pylint: enable=W0703,broad-except
        else:
            return True

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
        self.name = "%s" % type(self).__name__
        self.num_executors = int(num_executors)
        self.timeout = timeout
        self.log_func = logger
        self.logger = logger(self.name) if logger else util.get_default_logger(self.name)

        # pipe connections to send and receive messages to/from executors
        self.pipe = {}
        # list of executors that are initialized
        self.executors = []
        # list of is_alive statuses for executors
        self.is_alive_statuses = {}
        # initialize priority queues, the lower number means higher priority
        self.task_queue_map = {
            const.PRIORITY_0: multiprocessing.Queue(),
            const.PRIORITY_1: multiprocessing.Queue(),
            const.PRIORITY_2: multiprocessing.Queue()
        }
        # scheduler metrics
        self.__metrics = {}
        # callbacks
        """
        .. note:: DeveloperApi

        Invoked when task is started on executor.

        :param messages: list of messages EXECUTOR_TASK_STARTED
        """
        self.on_task_started = None
        """
        .. note:: DeveloperApi

        Invoked when task is cancelled on executor.

        :param messages: list of messages EXECUTOR_TASK_CANCELLED
        """
        self.on_task_cancelled = None
        """
        .. note:: DeveloperApi

        Invoked when task is finished successfully on executor.

        :param messages: list of messages EXECUTOR_TASK_SUCCEEDED
        """
        self.on_task_succeeded = None
        """
        .. note:: DeveloperApi

        Invoked when task is finished with failure on executor.

        :param messages: list of messages EXECUTOR_TASK_FAILED
        """
        self.on_task_failed = None

        """
        .. note:: DeveloperApi

        Invoked when executor sends 'is alive' reponse.

        :param messages: list of messages EXECUTOR_IS_ALIVE
        """
        self.on_is_alive = self._update_is_alive

    def _get_metric(self, name):
        """
        Get metric for name.

        :return: metric value
        """
        return self.__metrics[name] if name in self.__metrics else None

    def _set_metric(self, name, value):
        """
        Set metric value for name. Will update previously registered value.

        :param name: metric name
        :param value: metric value
        """
        self.__metrics[name] = value

    def _increment_metric(self, name):
        """
        Increment metric assuming that metric is integer value. If error occurs defaults to None.

        :param name: metric name
        """
        updated = 0
        try:
            updated = int(self._get_metric(name)) + 1
        except ValueError:
            updated = 0
        except TypeError:
            updated = None
        self._set_metric(name, updated)

    def get_metrics(self):
        """
        Return copy of the scheduler metrics.

        :return: scheduler metrics copy
        """
        return self.__metrics.copy()

    def _prepare_executor(self, name):
        """
        Prepare single executor, this creates connection for executor and launches it as daemon
        process, and appends to executors list.

        :param name: executor's name (original, not final executor name)
        :return: created executor (it is already added to the list of executors)
        """
        main_conn, exc_conn = multiprocessing.Pipe()
        clazz = self.executor_class()
        if not inspect.isclass(clazz) or not issubclass(clazz, Executor):
            raise TypeError("Type %s !<: Executor" % clazz)
        exc = clazz(name, exc_conn, self.task_queue_map, timeout=self.timeout, logger=self.log_func)
        self.executors.append(exc)
        self.pipe[exc.name] = main_conn
        self.is_alive_statuses[exc.name] = util.utcnow()
        return exc

    def start(self):
        """
        Start scheduler, launches executors asynchronously.
        """
        self.logger.info("Starting %s '%s' executors", self.num_executors, self.executor_class())
        # Launch executors and save pipes per each
        for i in range(self.num_executors):
            exc = self._prepare_executor("#%s" % i)
            exc.start()

    def stop(self):
        """
        Stop scheduler, terminates executors, and all tasks that were running at the time.
        """
        for conn in self.pipe.values():
            conn.send(Message(EXECUTOR_SHUTDOWN))
        # timeout to terminate processes and process remaining messages in Pipe by polling thread
        self.logger.info("Waiting for termination...")
        time.sleep(5)
        for exc in self.executors:
            if exc.is_alive():
                exc.terminate()
            exc.join()
        self.logger.info("Terminated executors, cleaning up internal data")
        self.pipe = None
        self.executors = None
        self.task_queue_map = None

    def submit(self, task):
        """
        Add task for priority provided with task.

        :param task: task to add, must be instance of Task
        :return: task uid
        """
        if not isinstance(task, Task):
            raise TypeError("%s != Task" % type(task))
        if task.priority not in self.task_queue_map:
            raise KeyError("No priority %s found in queue map" % task.priority)
        self.task_queue_map[task.priority].put_nowait(task)
        self._increment_metric("submitted-tasks")
        return task.uid

    def cancel(self, task_id):
        """
        Cancel task by provided task_id, this includes either termination of currently running task,
        or removal of future scheduled tasks, note that this will be no-op if task that has been
        already processed.

        :param task_id: task id to cancel, no-op if task_id is None
        """
        if task_id:
            for conn in self.pipe.values():
                conn.send(Message(EXECUTOR_CANCEL_TASK, task_id=task_id))

    # Thread is considered to be long-lived, and is terminated when scheduler is stopped.
    # Method always provides list of messages to callback or empty list, it is guaranteed to provide
    # list of valid messages.
    def _process_callback(self):
        msg_list = {}
        # sometimes thread can report that pipe is None, which might require lock before
        # processing, currently we just skip iteration, if it is None.
        if self.pipe is not None:
            for conn in self.pipe.values():
                while conn.poll():
                    message = conn.recv()
                    # ignore non-valid messages
                    if not isinstance(message, Message):
                        continue
                    if message.status in msg_list:
                        msg_list[message.status].append(message)
                    else:
                        msg_list[message.status] = [message]
        if self.on_task_started and EXECUTOR_TASK_STARTED in msg_list:
            self.on_task_started.__call__(msg_list[EXECUTOR_TASK_STARTED])
        if self.on_task_succeeded and EXECUTOR_TASK_SUCCEEDED in msg_list:
            self.on_task_succeeded.__call__(msg_list[EXECUTOR_TASK_SUCCEEDED])
        if self.on_task_failed and EXECUTOR_TASK_FAILED in msg_list:
            self.on_task_failed.__call__(msg_list[EXECUTOR_TASK_FAILED])
        if self.on_task_cancelled and EXECUTOR_TASK_CANCELLED in msg_list:
            self.on_task_cancelled.__call__(msg_list[EXECUTOR_TASK_CANCELLED])
        if self.on_is_alive and EXECUTOR_IS_ALIVE in msg_list:
            self.on_is_alive.__call__(msg_list[EXECUTOR_IS_ALIVE])

    def _update_is_alive(self, messages):
        """
        Update 'is alive' status for executors. Currently just updates datetime of message.

        :param messages: list of Message instances with EXECUTOR_IS_ALIVE status
        """
        for msg in messages:
            if "name" in msg.arguments:
                exc_name = msg.arguments["name"]
                self.is_alive_statuses[exc_name] = util.utcnow()
                self.logger.debug("Updated 'is alive' status for executor %s", exc_name)

    def get_is_alive_statuses(self):
        """
        Return copy of 'is alive' statuses.

        :return: dictionary of 'executor -> datetime of update in UTC'
        """
        return self.is_alive_statuses.copy()

    def _prepare_polling_thread(self, name):
        """
        Prepare maintenance thread for polling messages from Pipe. This returns None, when no
        target consumer is provided.

        :param name: name of the polling thread
        :return: created daemon thread or None, if target is not specified
        """
        def poll_messages(): # pragma: no cover
            while True:
                self._process_callback()
                time.sleep(self.timeout)
        thread = threading.Thread(name=name, target=poll_messages)
        thread.daemon = True
        return thread

    def start_maintenance(self):
        """
        Start all maintenance threads and processes.
        """
        # Launch polling thread for messages
        thread = self._prepare_polling_thread("Polling-1")
        if thread:
            thread.start()

    def executor_class(self):
        """
        .. note:: DeveloperApi

        Return executor class to launch. By default returns generic Executor implementation.

        :return: scheduler.Executor subclass
        """
        return Executor

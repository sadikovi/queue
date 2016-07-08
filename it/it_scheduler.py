#!/usr/bin/env python

import time
import types
import it.abstract as abstract
import src.const as const
import src.scheduler as scheduler

class SimpleTask(scheduler.Task):
    """
    Simple task that waits for blocking and running.
    """
    def __init__(self, uid, priority, blocktime, runtime):
        self._uid = uid
        self._priority = priority
        self.blocktime = blocktime
        self.runtime = runtime
        self._exit_code = None
        self._status = self.BLOCKED

    @property
    def priority(self):
        return self._priority

    @property
    def uid(self):
        return self._uid

    @property
    def exit_code(self):
        return self._exit_code

    def status(self):
        if self._status == self.BLOCKED:
            if self.blocktime <= 0:
                self._status = self.PENDING
            else:
                self.blocktime -= 1
        elif self._status == self.RUNNING:
            if self.runtime <= 0:
                self._status = self.FINISHED
                self._exit_code = 0
            else:
                self.runtime -= 1
        return self._status

    def async_launch(self):
        self._status = self.RUNNING

    def cancel(self):
        self._status = self.FINISHED
        self._exit_code = -1

class Scenario(object):
    def __init__(self, name, args):
        self._name = name
        self._args = args

    def execute(self, sched):
        print "- Scenario = %s" % self.name
        for arg in self.args:
            if isinstance(arg, types.StringType):
                call = arg.split(":")
                if call[0] == "cancel":
                    sched.cancel(call[1])
            if isinstance(arg, types.IntType):
                time.sleep(arg)
            elif isinstance(arg, SimpleTask):
                sched.put(arg.priority, arg)

    @property
    def name(self):
        return self._name

    @property
    def args(self):
        return self._args

class Scheduler1IntegrationTest(abstract.IntegrationTest):
    def setUp(self):
        self._msg = []
        self._sched = scheduler.Scheduler(1, timeout=1, logger=None)
        self.scenario = Scenario("simple", [1, SimpleTask("1", const.PRIORITY_1, 2, 2), 7])

    def get_msg(self, msg):
        self._msg.append(msg)

    def runTest(self):
        self._sched.start_maintenance(polling_target=self.get_msg)
        self._sched.start()
        self.scenario.execute(self._sched)
        # check messages
        print self._msg
        messages = [x for arr in self._msg for x in arr]
        assert len(messages) == 2
        assert messages[0].status == scheduler.MESSAGE_TASK_STARTED
        assert messages[0].arguments["task_id"] == "1"
        assert messages[1].status == scheduler.MESSAGE_TASK_FINISHED
        assert messages[1].arguments["task_id"] == "1"

class Scheduler2IntegrationTest(abstract.IntegrationTest):
    def setUp(self):
        self._msg = []
        self._sched = scheduler.Scheduler(2, timeout=1, logger=None)
        self.scenario = Scenario("2-executors", [
            2,
            SimpleTask("1", const.PRIORITY_2, 2, 4),
            3
        ])

    def get_msg(self, msg):
        self._msg.append(msg)

    def runTest(self):
        self._sched.start_maintenance(polling_target=self.get_msg)
        self._sched.start()
        self.scenario.execute(self._sched)
        self._sched.stop()
        # check messages
        print self._msg
        messages = [x for arr in self._msg for x in arr]
        assert len(messages) == 2
        assert messages[0].status == scheduler.MESSAGE_TASK_STARTED
        assert messages[1].status == scheduler.MESSAGE_TASK_CANCELLED

class Scheduler3IntegrationTest(abstract.IntegrationTest):
    def setUp(self):
        self._msg = []
        self._sched = scheduler.Scheduler(2, timeout=1, logger=None)
        self.scenario = Scenario("2-executors", [
            2,
            SimpleTask("1", const.PRIORITY_2, 2, 4),
            SimpleTask("2", const.PRIORITY_1, 4, 2),
            6,
            "cancel:2",
            5
        ])

    def get_msg(self, msg):
        self._msg.append(msg)

    def runTest(self):
        self._sched.start_maintenance(polling_target=self.get_msg)
        self._sched.start()
        self.scenario.execute(self._sched)
        self._sched.stop()
        # check messages
        print self._msg
        messages = [x for arr in self._msg for x in arr if len(arr) == 1]
        assert len(messages) == 4
        assert messages[0].status == scheduler.MESSAGE_TASK_STARTED
        assert messages[1].status == scheduler.MESSAGE_TASK_STARTED
        assert messages[2].status == scheduler.MESSAGE_TASK_CANCELLED
        assert messages[2].arguments["task_id"] == "2"
        assert messages[3].status == scheduler.MESSAGE_TASK_FINISHED
        assert messages[3].arguments["task_id"] == "1"

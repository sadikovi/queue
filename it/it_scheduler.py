#!/usr/bin/env python
# -*- coding: UTF-8 -*-

#
# Copyright 2016 sadikovi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import time
import types
import it.abstract as abstract
import src.const as const
import src.scheduler as scheduler

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
            elif isinstance(arg, scheduler.Task):
                sched.submit(arg)

    @property
    def name(self):
        return self._name

    @property
    def args(self):
        return self._args

class OkTask(scheduler.Task):
    def __init__(self, uid, priority, seconds):
        self._uid = uid
        self._priority = priority
        self._runtime = seconds
        self._cancelled = False

    @property
    def uid(self):
        return self._uid

    @property
    def priority(self):
        return self._priority

    def run(self):
        while self._runtime > 0 and not self._cancelled:
            time.sleep(1)
            self._runtime -= 1

    def cancel(self):
        self._cancelled = True

class FailTask(OkTask):
    def __init__(self, uid, priority, seconds):
        super(FailTask, self).__init__(uid, priority, seconds)

    def run(self):
        while self._runtime > 0 and not self._cancelled:
            time.sleep(1)
            self._runtime -= 1
        raise StandardError("Failed task")

class SchedulerSimpleSuite(abstract.IntegrationTest):
    """
    Simple test, we launch single task and expect it to report start and finish before scheduler is
    stopped.
    """
    def setUp(self):
        self.msg = []
        self.sched = scheduler.Scheduler(1, timeout=1)
        self.scenario = Scenario("simple", [1, OkTask("1", const.PRIORITY_1, 2), 4])

    def get_msg(self, msg):
        self.msg += msg

    def runTest(self):
        # register callback
        self.sched.on_task_started = self.get_msg
        self.sched.on_task_failed = self.get_msg
        self.sched.on_task_succeeded = self.get_msg
        self.sched.on_task_cancelled = self.get_msg
        # run test
        self.sched.start_maintenance()
        self.sched.start()
        self.scenario.execute(self.sched)
        self.sched.stop()
        print self.msg
        assert len(self.msg) == 2
        assert self.msg[0].status == scheduler.EXECUTOR_TASK_STARTED
        assert self.msg[0].arguments["task_id"] == "1"
        assert self.msg[1].status == scheduler.EXECUTOR_TASK_SUCCEEDED
        assert self.msg[1].arguments["task_id"] == "1"

class SchedulerCancelSuite(abstract.IntegrationTest):
    """
    Test of 2 executors with one task, that is cancelled by terminated executor. We expect task to
    report start and cancel.
    """
    def setUp(self):
        self.msg = []
        self.sched = scheduler.Scheduler(2, timeout=1)
        self.scenario = Scenario("cancel", [2, OkTask("1", const.PRIORITY_2, 5), 3])

    def get_msg(self, msg):
        self.msg += msg

    def runTest(self):
        # register callback
        self.sched.on_task_started = self.get_msg
        self.sched.on_task_failed = self.get_msg
        self.sched.on_task_succeeded = self.get_msg
        self.sched.on_task_cancelled = self.get_msg
        # run test
        self.sched.start_maintenance()
        self.sched.start()
        self.scenario.execute(self.sched)
        self.sched.stop()
        print self.msg
        assert len(self.msg) == 2
        assert self.msg[0].status == scheduler.EXECUTOR_TASK_STARTED
        assert self.msg[0].arguments["task_id"] == "1"
        assert self.msg[1].status == scheduler.EXECUTOR_TASK_CANCELLED
        assert self.msg[1].arguments["task_id"] == "1"

class SchedulerFinishCancelSuite(abstract.IntegrationTest):
    """
    Test of 2 executors launching 2 tasks in parallel, and cancel second task before scheduler is
    stopped. We expect tasks to report start, first task to report finish, and second task to
    report cancel.
    """
    def setUp(self):
        self.msg = []
        self.sched = scheduler.Scheduler(2, timeout=1)
        self.scenario = Scenario(
            "finish-cancel",
            [2, OkTask("1", const.PRIORITY_2, 3), 1, OkTask("2", const.PRIORITY_1, 7), 5]
        )

    def get_msg(self, msg):
        self.msg += msg

    def runTest(self):
        # register callback
        self.sched.on_task_started = self.get_msg
        self.sched.on_task_failed = self.get_msg
        self.sched.on_task_succeeded = self.get_msg
        self.sched.on_task_cancelled = self.get_msg
        # run test
        self.sched.start_maintenance()
        self.sched.start()
        self.scenario.execute(self.sched)
        self.sched.stop()
        print self.msg
        assert len(self.msg) == 4
        assert self.msg[0].status == scheduler.EXECUTOR_TASK_STARTED
        assert self.msg[0].arguments["task_id"] == "1"
        assert self.msg[1].status == scheduler.EXECUTOR_TASK_STARTED
        assert self.msg[1].arguments["task_id"] == "2"
        assert self.msg[2].status == scheduler.EXECUTOR_TASK_SUCCEEDED
        assert self.msg[2].arguments["task_id"] == "1"
        assert self.msg[3].status == scheduler.EXECUTOR_TASK_CANCELLED
        assert self.msg[3].arguments["task_id"] == "2"

class SchedulerCancelAheadSuite(abstract.IntegrationTest):
    """
    Test of 2 executors, launching 2 tasks, but the second task should never get launched, because
    we cancel it ahead of scheduling. We expect messages of start and finish for the first task.
    """
    def setUp(self):
        self.msg = []
        self.sched = scheduler.Scheduler(2, timeout=1)
        self.scenario = Scenario("cancel-ahead", [
            1,
            "cancel:2",
            2,
            OkTask("1", const.PRIORITY_2, 3),
            1,
            OkTask("2", const.PRIORITY_1, 7),
            5
        ])

    def get_msg(self, msg):
        self.msg += msg

    def runTest(self):
        # register callback
        self.sched.on_task_started = self.get_msg
        self.sched.on_task_failed = self.get_msg
        self.sched.on_task_succeeded = self.get_msg
        self.sched.on_task_cancelled = self.get_msg
        # run test
        self.sched.start_maintenance()
        self.sched.start()
        self.scenario.execute(self.sched)
        self.sched.stop()
        print self.msg
        assert len(self.msg) == 3
        assert self.msg[0].status == scheduler.EXECUTOR_TASK_STARTED
        assert self.msg[0].arguments["task_id"] == "1"
        assert self.msg[1].status == scheduler.EXECUTOR_TASK_CANCELLED
        assert self.msg[1].arguments["task_id"] == "2"
        assert self.msg[2].status == scheduler.EXECUTOR_TASK_SUCCEEDED
        assert self.msg[2].arguments["task_id"] == "1"

class SchedulerFailedTasksSuite(abstract.IntegrationTest):
    """
    Test of 2 executors, launching 2 failed tasks. Both tasks will raise errors, and should be
    reported as failed, but scheduler should continue running.
    """
    def setUp(self):
        self.msg = []
        self.sched = scheduler.Scheduler(2, timeout=1)
        self.scenario = Scenario("failed-tasks", [
            2,
            FailTask("1", const.PRIORITY_2, 2),
            1,
            FailTask("2", const.PRIORITY_1, 3),
            5
        ])

    def get_msg(self, msg):
        self.msg += msg

    def runTest(self):
        # register callback
        self.sched.on_task_started = self.get_msg
        self.sched.on_task_failed = self.get_msg
        self.sched.on_task_succeeded = self.get_msg
        self.sched.on_task_cancelled = self.get_msg
        # run test
        self.sched.start_maintenance()
        self.sched.start()
        self.scenario.execute(self.sched)
        self.sched.stop()
        print self.msg
        assert len(self.msg) == 4
        assert self.msg[0].status == scheduler.EXECUTOR_TASK_STARTED
        assert self.msg[0].arguments["task_id"] == "1"
        assert self.msg[1].status == scheduler.EXECUTOR_TASK_STARTED
        assert self.msg[1].arguments["task_id"] == "2"
        assert self.msg[2].status == scheduler.EXECUTOR_TASK_FAILED
        assert self.msg[2].arguments["task_id"] == "1"
        assert self.msg[3].status == scheduler.EXECUTOR_TASK_FAILED
        assert self.msg[3].arguments["task_id"] == "2"

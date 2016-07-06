#!/usr/bin/env python

import unittest
import src.const as const

class ConstSuite(unittest.TestCase):
    def test_status(self):
        status = const.Status("name", "desc")
        self.assertEqual(status.name, "name")
        self.assertEqual(status.desc, "desc")

    def test_status_none(self):
        status = const.Status(None, None)
        self.assertEqual(status.name, None)
        self.assertEqual(status.desc, None)

    # Tests for system statuses
    def test_system_available(self):
        self.assertEqual(const.SYSTEM_AVAILABLE.name, "AVAILABLE")
        self.assertEqual(const.SYSTEM_AVAILABLE.desc, "Available")

    def test_system_busy(self):
        self.assertEqual(const.SYSTEM_BUSY.name, "BUSY")
        self.assertEqual(const.SYSTEM_BUSY.desc, "Busy")

    def test_system_unavailable(self):
        self.assertEqual(const.SYSTEM_UNAVAILABLE.name, "UNAVAILABLE")
        self.assertEqual(const.SYSTEM_UNAVAILABLE.desc, "Unavailable")

    # Tests for submission statuses
    def test_submission_pending(self):
        self.assertEqual(const.SUBMISSION_PENDING.name, "PENDING")
        self.assertEqual(const.SUBMISSION_PENDING.desc, "Pending")

    def test_submission_waiting(self):
        self.assertEqual(const.SUBMISSION_WAITING.name, "WAITING")
        self.assertEqual(const.SUBMISSION_WAITING.desc, "Waiting")

    def test_submission_running(self):
        self.assertEqual(const.SUBMISSION_RUNNING.name, "RUNNING")
        self.assertEqual(const.SUBMISSION_RUNNING.desc, "Running")

    def test_submission_success(self):
        self.assertEqual(const.SUBMISSION_SUCCESS.name, "SUCCESS")
        self.assertEqual(const.SUBMISSION_SUCCESS.desc, "Success")

    def test_submission_failure(self):
        self.assertEqual(const.SUBMISSION_FAILURE.name, "FAILURE")
        self.assertEqual(const.SUBMISSION_FAILURE.desc, "Failure")

    def test_submission_discard(self):
        self.assertEqual(const.SUBMISSION_DISCARD.name, "DISCARD")
        self.assertEqual(const.SUBMISSION_DISCARD.desc, "Discard")

    def test_submission_unknown(self):
        self.assertEqual(const.SUBMISSION_UNKNOWN.name, "UNKNOWN")
        self.assertEqual(const.SUBMISSION_UNKNOWN.desc, "Unknown")

    # Tests for priority levels
    def test_priority(self):
        self.assertEqual(const.PRIORITY_0, 0)
        self.assertEqual(const.PRIORITY_1, 1)
        self.assertEqual(const.PRIORITY_2, 2)
        self.assertEqual(const.PRIORITY_3, 3)
        self.assertEqual(const.PRIORITY_4, 4)
        self.assertEqual(const.PRIORITY_5, 5)
        self.assertEqual(const.PRIORITY_6, 6)
        self.assertEqual(const.PRIORITY_7, 7)
        self.assertEqual(const.PRIORITY_8, 8)
        self.assertEqual(const.PRIORITY_9, 9)

# Load test suites
def suites():
    return [
        ConstSuite
    ]
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

import os
import unittest
import mock
import src.util as util

class UtilSuite(unittest.TestCase):
    @mock.patch("src.util.os")
    def test_resolve_path_fail_1(self, mock_os):
        mock_os.path.abspath.side_effect = OSError("Test")
        with self.assertRaises(OSError):
            # pylint: disable=W0212,protected-access
            util._resolve_path(".", True, [os.R_OK])
            # pylint: enable=W0212,protected-access

    @mock.patch("src.util.os")
    def test_resolve_path_fail_2(self, mock_os):
        mock_os.path.abspath.return_value = "/tmp"
        mock_os.path.realpath.side_effect = OSError("Test")
        with self.assertRaises(OSError):
            # pylint: disable=W0212,protected-access
            util._resolve_path(".", True, [os.R_OK])
            # pylint: enable=W0212,protected-access

    @mock.patch("src.util.os")
    def test_resolve_path_fail_3(self, mock_os):
        mock_os.path.abspath.return_value = "/tmp"
        mock_os.path.realpath.return_value = "/tmp"
        mock_os.path.isdir.return_value = False
        with self.assertRaises(OSError):
            # pylint: disable=W0212,protected-access
            util._resolve_path(".", dir_expected=True, permissions=[])
            # pylint: enable=W0212,protected-access

    @mock.patch("src.util.os")
    def test_resolve_path_fail_4(self, mock_os):
        mock_os.path.abspath.return_value = "/tmp"
        mock_os.path.realpath.return_value = "/tmp"
        mock_os.path.isfile.return_value = False
        with self.assertRaises(OSError):
            # pylint: disable=W0212,protected-access
            util._resolve_path(".", dir_expected=False, permissions=[])
            # pylint: enable=W0212,protected-access

    @mock.patch("src.util.os")
    def test_resolve_path_fail_5(self, mock_os):
        mock_os.path.abspath.return_value = "/tmp"
        mock_os.path.realpath.return_value = "/tmp"
        mock_os.path.isdir.return_value = True
        mock_os.access.return_value = False
        with self.assertRaises(OSError):
            # pylint: disable=W0212,protected-access
            util._resolve_path(".", dir_expected=True, permissions=[os.R_OK])
            # pylint: enable=W0212,protected-access

    def test_resolve_path_fail_6(self):
        # should fail when empty string or None are passed
        with self.assertRaises(ValueError):
            # pylint: disable=W0212,protected-access
            util._resolve_path(None, dir_expected=False, permissions=[])
            # pylint: enable=W0212,protected-access
        with self.assertRaises(ValueError):
            # pylint: disable=W0212,protected-access
            util._resolve_path("", dir_expected=False, permissions=[])
            # pylint: enable=W0212,protected-access

    @mock.patch("src.util.os")
    def test_resolve_path_empty_permissions(self, mock_os):
        mock_os.path.abspath.return_value = "/tmp/a"
        mock_os.path.realpath.return_value = "/tmp/a/b"
        mock_os.path.isdir.return_value = True
        # pylint: disable=W0212,protected-access
        path = util._resolve_path(".", dir_expected=True, permissions=[])
        # pylint: enable=W0212,protected-access
        self.assertEqual(path, "/tmp/a/b")

    @mock.patch("src.util.os")
    def test_resolve_path_permissions(self, mock_os):
        mock_os.path.abspath.return_value = "/tmp/a"
        mock_os.path.realpath.return_value = "/tmp/a/b"
        mock_os.path.isdir.return_value = True
        mock_os.path.access.return_value = True
        # pylint: disable=W0212,protected-access
        path = util._resolve_path(".", dir_expected=True, permissions=[os.R_OK, os.W_OK])
        # pylint: enable=W0212,protected-access
        self.assertEqual(path, "/tmp/a/b")

    @mock.patch("src.util._resolve_path")
    def test_readwriteDirectory(self, mock_resolve_path):
        mock_resolve_path.return_value = "/tmp"
        self.assertEqual(util.readwriteDirectory("."), "/tmp")
        # pylint: disable=W0212,protected-access
        mock_resolve_path.assert_called_with(".", True, [os.R_OK, os.W_OK])
        # pylint: enable=W0212,protected-access

    @mock.patch("src.util._resolve_path")
    def test_readonlyDirectory(self, mock_resolve_path):
        mock_resolve_path.return_value = "/tmp"
        self.assertEqual(util.readonlyDirectory("."), "/tmp")
        # pylint: disable=W0212,protected-access
        mock_resolve_path.assert_called_with(".", True, [os.R_OK])
        # pylint: enable=W0212,protected-access

    @mock.patch("src.util._resolve_path")
    def test_readonlyFile(self, mock_resolve_path):
        mock_resolve_path.return_value = "/tmp"
        self.assertEqual(util.readonlyFile("."), "/tmp")
        # pylint: disable=W0212,protected-access
        mock_resolve_path.assert_called_with(".", False, [os.R_OK])
        # pylint: enable=W0212,protected-access

    def test_concat(self):
        self.assertEqual(util.concat("test"), "test")
        self.assertEqual(util.concat("test", "1"), "test/1")
        self.assertEqual(util.concat("test", "1", "2"), "test/1/2")

    @mock.patch("src.util.os")
    def test_mkdir_fail_1(self, mock_os):
        mock_os.mkdir.side_effect = OSError("Test")
        with self.assertRaises(OSError):
            util.mkdir("path", 0777)
        mock_os.mkdir.assert_called_with("path", 0777)

    @mock.patch("src.util.os")
    def test_mkdir_fail_2(self, mock_os):
        mock_os.mkdir.return_value = None
        with self.assertRaises(ValueError):
            util.mkdir(None, 0777)
        with self.assertRaises(ValueError):
            util.mkdir("path", None)

    @mock.patch("src.util.os")
    def test_mkdir_success(self, mock_os):
        mock_os.mkdir.return_value = None
        util.mkdir("path", 0777)
        mock_os.mkdir.assert_called_with("path", 0777)

    def test_safe_dict(self):
        self.assertEqual(util.safe_dict(None), None)
        self.assertEqual(util.safe_dict([]), {}) # weird conversion rule for list
        self.assertEqual(util.safe_dict([1, 2]), None)
        self.assertEqual(util.safe_dict({"a": True}), {"a": True})
        # testing of the failed state
        with self.assertRaises(TypeError):
            util.safe_dict(None, fail=True)

    def test_safe_int(self):
        self.assertEqual(util.safe_int(None), None)
        self.assertEqual(util.safe_int({"a": 1}), None)
        self.assertEqual(util.safe_int("abc"), None)
        self.assertEqual(util.safe_int(1), 1)
        self.assertEqual(util.safe_int(0), 0)
        # testing of the failed state
        with self.assertRaises(TypeError):
            util.safe_int(None, fail=True)
        with self.assertRaises(ValueError):
            util.safe_int("abc", fail=True)

class URISuite(unittest.TestCase):
    def setUp(self):
        self.uri1 = util.URI("http://localhost:8080", "Spark UI")
        self.uri2 = util.URI("spark://sandbox:7077")

    def test_init(self):
        uri = util.URI("http://localhost:8080")
        self.assertNotEqual(uri, None)

    def test_invalid_scheme(self):
        with self.assertRaises(StandardError):
            util.URI("localhost:8080")

    def test_invalid_host(self):
        with self.assertRaises(StandardError):
            util.URI("http://:8080")

    def test_invalid_port(self):
        with self.assertRaises(StandardError):
            util.URI("http://localhost")
        with self.assertRaises(ValueError):
            util.URI("http://localhost:ABC")

    def test_host(self):
        self.assertEqual(self.uri1.host, "localhost")
        self.assertEqual(self.uri2.host, "sandbox")

    def test_port(self):
        self.assertEqual(self.uri1.port, 8080)
        self.assertEqual(self.uri2.port, 7077)

    def test_scheme(self):
        self.assertEqual(self.uri1.scheme, "http")
        self.assertEqual(self.uri2.scheme, "spark")

    def test_netloc(self):
        self.assertEqual(self.uri1.netloc, "localhost:8080")
        self.assertEqual(self.uri2.netloc, "sandbox:7077")

    def test_fragment(self):
        self.assertEqual(self.uri1.fragment, "")
        self.assertEqual(self.uri2.fragment, "")

    def test_url(self):
        self.assertEqual(self.uri1.url, "http://localhost:8080")
        self.assertEqual(self.uri2.url, "spark://sandbox:7077")

    def test_alias(self):
        self.assertEqual(self.uri1.alias, "Spark UI")
        self.assertEqual(self.uri2.alias, "spark://sandbox:7077")

class QueueConfSuite(unittest.TestCase):
    def test_set_conf(self):
        conf = util.QueueConf()
        with self.assertRaises(AttributeError):
            conf.setConf(None, None)
        with self.assertRaises(AttributeError):
            conf.setConf("", None)
        # non-empty key and None value should succeed
        conf.setConf("key", None)
        self.assertEqual(conf.getConf("key"), None)

    def test_set_all_conf(self):
        conf = util.QueueConf()
        with self.assertRaises(TypeError):
            conf.setAllConf(None)
        with self.assertRaises(TypeError):
            conf.setAllConf([])
        conf.setAllConf({"a": 1, "b": True})
        self.assertEqual(conf.getConf("a"), 1)
        self.assertEqual(conf.getConf("b"), True)

    def test_get_conf(self):
        conf = util.QueueConf()
        conf.setConf("a", 1)
        conf.setConf("b", None)
        self.assertEqual(conf.getConf("a"), 1)
        self.assertEqual(conf.getConf("b"), None)
        self.assertEqual(conf.getConf("c"), None)

    def test_get_conf_string(self):
        conf = util.QueueConf()
        conf.setConf("a", 1)
        conf.setConf("b", None)
        self.assertEqual(conf.getConfString("a"), "1")
        self.assertEqual(conf.getConfString("b"), "None")
        self.assertEqual(conf.getConfString("c"), "None")

    def test_get_conf_boolean(self):
        conf = util.QueueConf()
        conf.setConf("a", 1)
        conf.setConf("b", None)
        conf.setConf("c", False)
        self.assertEqual(conf.getConfBoolean("a"), True)
        self.assertEqual(conf.getConfBoolean("b"), False)
        self.assertEqual(conf.getConfBoolean("c"), False)

    def test_get_conf_int(self):
        conf = util.QueueConf()
        conf.setConf("a", 1)
        conf.setConf("b", "2")
        conf.setConf("c", None)
        conf.setConf("d", False)
        self.assertEqual(conf.getConfInt("a"), 1)
        self.assertEqual(conf.getConfInt("b"), 2)
        with self.assertRaises(TypeError):
            conf.getConfInt("c")
        self.assertEqual(conf.getConfInt("d"), 0)

    def test_get_conf_float(self):
        conf = util.QueueConf()
        conf.setConf("a", 1)
        conf.setConf("b", "2.0")
        conf.setConf("c", None)
        conf.setConf("d", False)
        self.assertEqual(conf.getConfFloat("a"), 1.0)
        self.assertEqual(conf.getConfFloat("b"), 2.0)
        with self.assertRaises(TypeError):
            conf.getConfFloat("c")
        self.assertEqual(conf.getConfFloat("d"), 0.0)

    def test_contains(self):
        conf = util.QueueConf()
        conf.setConf("a", 1)
        conf.setConf("b", None)
        self.assertEqual(conf.contains("a"), True)
        self.assertEqual(conf.contains("b"), True)
        self.assertEqual(conf.contains("c"), False)

    def test_parse(self):
        self.assertEqual(util.QueueConf.parse(None), {})
        self.assertEqual(util.QueueConf.parse(""), {})
        self.assertEqual(util.QueueConf.parse("a=1 b=2 c='3 4'"), {"a": "1", "b": "2", "c": "3 4"})
        self.assertEqual(util.QueueConf.parse("a=1 b= c"), {"a": "1", "b": ""})
        self.assertEqual(util.QueueConf.parse("a=1 b=2=3"), {"a": "1", "b": "2=3"})

    def test_copy(self):
        conf = util.QueueConf()
        opts = {"a": 1, "b": True, "c": 1.0}
        conf.setAllConf(opts)
        self.assertEqual(conf.copy(), opts)

# Load test suites
def suites():
    return [
        UtilSuite,
        URISuite,
        QueueConfSuite
    ]

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

import datetime
import os
import urlparse

# == OS related methods and classes ==
def _resolve_path(unresolved_path, dir_expected, permissions):
    """
    Resolve absolute path for unresolved path, check that it exists and it is either directory or
    file depending on a flag `dir_expected`, and also check that path is accessible based on list
    of provided permissions (os.R_OK, os.W_OK, os.X_OK).

    :param unresolved_path: unresolved path
    :param dir_expected: True if path should be directory, False if path should be file
    :param permissions: list of expected permissions, e.g. [os.R_OK, os.W_OK]
    :return: fully resolved absolute path with checked permissions
    """
    if not unresolved_path:
        raise ValueError("Expected path, got %s" % unresolved_path)
    normpath = os.path.realpath(os.path.abspath(unresolved_path))
    # check if path is a valid directory or valid file
    if dir_expected and not os.path.isdir(normpath):
        raise OSError("Path %s is not a directory" % normpath)
    if not dir_expected and not os.path.isfile(normpath):
        raise OSError("Path %s is not a file" % normpath)
    # check list of permissions on normalized path, valid permissions: os.R_OK, os.W_OK, os.X_OK
    # see: https://docs.python.org/2/library/os.html#os.access
    for permission in permissions:
        if not os.access(normpath, permission):
            raise OSError("Insufficient permissions for %s, expected %s" % (normpath, permission))
    return normpath

def readwriteDirectory(unresolved_directory):
    """
    Resolve absolute path for the unresolved directory, check that it exists and valid, and
    also check read-write access to the directory.

    :param unresolved_directory: unresolved directory
    :return: fully resolved absolute path as directory with read-write access
    """
    return _resolve_path(unresolved_directory, True, [os.R_OK, os.W_OK])

def readonlyDirectory(unresolved_directory):
    """
    Resolve absolute path for the unresolved directory, check that it exists and valid, and we have
    read access to it.

    :param unresolved_directory: unresolved directory
    :return: fully resolved absolute path as directory with read access
    """
    return _resolve_path(unresolved_directory, True, [os.R_OK])

def readonlyFile(unresolved_filepath):
    """
    Resolve absolute path for the unresolved filepath, check that path exists and valid, and read
    access is granted.

    :param unresolved_filepath: unresolved path to the file
    :return: fully resolved absolute path to the file with read access
    """
    return _resolve_path(unresolved_filepath, False, [os.R_OK])

def concat(root, *paths):
    """
    Shortcut for `os.path.join()` method.

    :param root: main part of path
    :param *paths: suffices to append to the root
    :return: concatenated path
    """
    return os.path.join(root, *paths)

def mkdir(path, mode):
    """
    Shortcut for `os.mkdir()` method.

    :param path: path to create
    :param mode: permissions octal value
    """
    if not path:
        raise ValueError("Invalid path %s is provided" % path)
    if not mode:
        raise ValueError("Invalid mode %s is provided" % mode)
    os.mkdir(path, mode)

def exists(path):
    """
    Shortcut for `os.path.exists()` method.

    :param path: path to check
    :return: True if path exists, False otherwise
    """
    return os.path.exists(path)

def open(path, mode):
    """
    Shortcut for `open` Python built-in. Read docs on `open` for more information.

    :param path: file path
    :param mode: mode to use when opening file
    :return: File object based on path and mode
    """
    # pylint: disable=W0622,redefined-builtin
    return open(path, mode) # pragma: no cover
    # pylint: enable=W0622,redefined-builtin

# == REST API and URI related methods and classes ===
class URI(object):
    """
    URI class to keep information about URL and components, parses and validates components
    """
    def __init__(self, rawURI, alias=None):
        uri = urlparse.urlsplit(rawURI)
        # we expect scheme, hostname and port to be set, otherwise uri is considered invalid
        if not uri.port or not uri.hostname or not uri.scheme:
            raise StandardError("Invalid URI - expected host, port and scheme from '%s'" % rawURI)
        self._host = uri.hostname
        self._port = int(uri.port)
        self._scheme = uri.scheme
        self._netloc = uri.netloc
        self._fragment = uri.fragment
        self._url = uri.geturl()
        # Alias for URL if it is too long, if None provided url is used
        self._alias = alias
        self._user = uri.username
        self._password = uri.password

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def scheme(self):
        return self._scheme

    @property
    def netloc(self):
        return self._netloc

    @property
    def fragment(self):
        return self._fragment

    @property
    def url(self):
        return self._url

    @property
    def alias(self):
        return self._alias if self._alias else self._url

    @property
    def user(self):
        return self._user

    @property
    def password(self):
        return self._password

def _safe_conversion(value, func, fail, msg):
    """
    Safe conversion using 'func' as conversion function. If conversion fails, 'None' is returned.
    If 'fail' is set to True, then instead of returning 'None', raises error with extended message.

    :param value: value to convert
    :param func: conversion function
    :param fail: raise error in case of failed conversion if True, otherwise return None
    :param msg: custom message in case of failure
    :return: converted value or None if no fail
    """
    result = None
    try:
        result = func(value)
    except TypeError as type_error:
        if fail:
            raise TypeError(msg % (value, type_error))
    except ValueError as value_error:
        if fail:
            raise ValueError(msg % (value, value_error))
    return result

def safe_int(value, fail=False):
    """
    Safe conversion to int.
    """
    return _safe_conversion(value, int, fail, "Failed to convert '%s' into 'int', reason: %s")

def safe_dict(value, fail=False):
    """
    Safe conversion to dict.
    """
    return _safe_conversion(value, dict, fail, "Failed to convert '%s' into 'dict', reason: %s")

# == Datetime related methods and classes ==
def utcnow(delay=0):
    """
    Simple wrapper on datetime.utcnow(), returns datetime object as UTC. Applies offset in seconds.

    :param delay: offset in seconds
    :return: datetime object in UTC
    """
    delta = datetime.timedelta(seconds=delay)
    return datetime.datetime.utcnow() + delta

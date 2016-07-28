#!/usr/bin/env python

import datetime
import logging
import os
import shlex
import types
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

# == Logging related methods and classes ==
def get_default_logger(name):
    """
    Internal method to set default logger. Should be moved into utility functions or unified
    package instead. Creates logger for name provided.

    :param name: logger name
    :return: default logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if not logger.handlers: # pragma: no cover
        form = logging.Formatter("LOG :: %(asctime)s :: %(name)s :: %(levelname)s :: %(message)s")
        stderr = logging.StreamHandler()
        stderr.setFormatter(form)
        logger.addHandler(stderr)
    return logger

# == Datetime related methods and classes ==
def utcnow(delay=0):
    """
    Simple wrapper on datetime.utcnow(), returns datetime object as UTC. Applies offset in seconds.

    :param delay: offset in seconds
    :return: datetime object in UTC
    """
    delta = datetime.timedelta(seconds=delay)
    return datetime.datetime.utcnow() + delta

# == Configuration related methods and classes ==
class QueueConf(object):
    """
    Global Queue configuration similar to org.apache.spark.sql.SQLContext. Provides basic interface
    to add and extract options for keys.
    """
    def __init__(self):
        # private configuration map
        self.__configuration = {}

    def setConf(self, key, value):
        """
        Set configuration 'key -> value', if key is invalid, e.g. None, raises AttributeError.

        :param key: option key
        :param value: option value
        """
        if not key:
            raise AttributeError("Invalid key provided for configuration '%s -> %s'" % (key, value))
        self.__configuration[key] = value

    def setAllConf(self, options):
        """
        Set configuration using map of options, each pair is 'key -> value', similar rules apply to
        each option. Raises AttributeError, if options map is not of DictType.

        :param options: map of options
        """
        if not isinstance(options, types.DictType):
            raise TypeError("Expected dict object, got %s" % options)
        for key, value in options.items():
            self.setConf(key, value)

    def getConf(self, key):
        """
        Extract raw configuration value.

        :param key: option key
        :return: option value or None if key does not exist
        """
        return self.__configuration[key] if key in self.__configuration else None

    def getConfString(self, key):
        """
        Get option value as string. None will be converted into string.

        :param key: option key
        :return: option value as string
        """
        return str(self.getConf(key))

    def getConfBoolean(self, key):
        """
        Get option value as boolean. None will be converted into boolean.

        :param key: option key
        :return: option value as boolean
        """
        return bool(self.getConf(key))

    def getConfInt(self, key):
        """
        Get option value as int. None will be converted into int.

        :param key: option key
        :return: option value as int
        """
        return int(self.getConf(key))

    def getConfFloat(self, key):
        """
        Get option value as float. None will be converted into float.

        :param key: option key
        :return: option value as float
        """
        return float(self.getConf(key))

    def contains(self, key):
        """
        Return True if key is in configuration, False otherwise.

        :param key: option key
        :return: True, if key exists, False otherwise
        """
        return key in self.__configuration

    def copy(self):
        """
        Return dict copy of all options in configuration.

        :return: copy of configuration
        """
        return self.__configuration.copy()

    @staticmethod
    def parse(raw_string):
        """
        Parse raw string of configuration options into map of 'key -> value' pairs.

        :param raw_string: raw configuration string
        :return: options map of key -> value pairs
        """
        if not raw_string:
            return {}
        arr = shlex.split(raw_string)
        # each element in array is a key-value pair, key should not contain '=' is part of name
        def convert_tuple(elem):
            lst = elem.split("=", 1)
            return (lst[0], lst[1]) if len(lst) == 2 else None
        conv = [convert_tuple(x) for x in arr]
        return dict([pair for pair in conv if pair])

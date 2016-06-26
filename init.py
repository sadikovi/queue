#!/usr/bin/env python

import os

# This is an initial configuration, e.g. settings global paths, resolving dependencies directory
# Root directory of the project
ROOT_PATH = os.path.dirname(os.path.realpath(__file__))
# Dependencies directory
LIB_PATH = os.path.join(ROOT_PATH, "lib")
# Web source directory
WEB_PATH = os.path.join(ROOT_PATH, "web")
# Directory for serving static files
STATIC_PATH = os.path.join(WEB_PATH, "static")

# Application version
VERSION = "0.0.1"

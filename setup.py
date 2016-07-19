#!/usr/bin/env python

import sys
from setuptools import setup, Command
from init import LIB_PATH, VERSION

# Currently Python 2.7 is supported
PYTHON_VERSION_MAJOR = 2
PYTHON_VERSION_MINOR = 7
if sys.version_info.major != PYTHON_VERSION_MAJOR or sys.version_info.minor != PYTHON_VERSION_MINOR:
    print "[ERROR] Only Python %s.%s is supported" % (PYTHON_VERSION_MAJOR, PYTHON_VERSION_MINOR)
    sys.exit(1)

# Run only on OS X and Linux
if not (sys.platform.startswith("darwin") or sys.platform.startswith("linux")):
    print "[ERROR] Only OS X and Linux are supported"
    sys.exit(1)

# Add dependencies to the path
sys.path.insert(1, LIB_PATH)

class IntegrationTestCommand(Command):
    description = "Run integration tests"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import it
        sys.exit(it.main())

setup(
    name="queue",
    version=VERSION,
    description="Improved job scheduler for Apache Spark",
    long_description="Improved job scheduler for Apache Spark",
    author="Ivan Sadikov",
    author_email="ivan.sadikov@github.com",
    url="https://github.com/sadikovi/queue",
    platforms=["OS X", "Linux"],
    license="Apache License 2.0",
    test_suite="test",
    cmdclass={
        "run_it": IntegrationTestCommand
    }
)

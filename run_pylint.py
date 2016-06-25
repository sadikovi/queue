#!/usr/bin/env python

import sys
import init

if __name__ == '__main__':
    sys.path.insert(1, init.LIB_PATH)
    import pylint
    pylint.run_pylint()

#!/usr/bin/env python
import sys
import init

sys.path.insert(1, init.LIB_PATH)

if __name__ == '__main__':
    import pylint
    pylint.run_pylint()

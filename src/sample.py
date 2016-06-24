#!/usr/bin/env python

import os, unittest

class A(object):
    def __init__(self, name):
        self.name = name

    def show(self):
        return self.name

    def compute(self):
        return self.name + "..."

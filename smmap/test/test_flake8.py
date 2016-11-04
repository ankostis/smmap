"""Run PEP8 on all Python files in this directory and subdirectories as part of the tests."""
from __future__ import print_function

import unittest
import subprocess

import os.path as osp
import sys
try:
    from unittest import skipIf
except ImportError:
    from unittest2 import skipIf  # @UnusedImport

mydir = osp.dirname(__file__)
projdir = osp.normpath(osp.abspath(osp.join(mydir, '..')))


@skipIf(sys.version_info[:2] < (3, 4), "Latest style accepted` ")
class Test(unittest.TestCase):
    """Run PEP8 on all files in this directory and subdirectories."""

    def test_flake8(self):
        ret = subprocess.check_call(['flake8', projdir])
        assert not ret, ret

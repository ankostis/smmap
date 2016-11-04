"""Run PEP8 on all Python files in this directory and subdirectories as part of the tests."""
from __future__ import print_function

import os
import sys
import unittest

import pep8

import os.path as osp


__author__ = 'Christopher Swenson'
__email__ = 'chris@caswenson.com'
__license__ = 'CC0 http://creativecommons.org/publicdomain/zero/1.0/'
__url__ = 'https://gist.github.com/swenson/8142788'


# ignore stuff in virtualenvs or version control directories
ignore_patterns = ('.svn', 'bin', 'lib' + os.sep + 'python')

mydir = osp.dirname(__file__)
projdir = osp.normpath(osp.abspath(osp.join(mydir, '..')))


def is_dir_ignored(dirname):
    """Should the directory be ignored?"""
    for pattern in ignore_patterns:
        if pattern in dirname:
            return True
    return False


def collect_python_files(projdir):
    python_files = []
    for root, _, files in os.walk(projdir):
        if is_dir_ignored(root):
            continue

        python_files += [osp.join(root, f) for f in files if f.endswith('.py')]
    return python_files


class TestPep8(unittest.TestCase):
    """Run PEP8 on all files in this directory and subdirectories."""

    def test_pep8(self):
        style = pep8.StyleGuide(quiet=True)
        style.init_report(pep8.StandardReport)
        style.options.ignore += (
            'E265',  # comment blocks like @{ section, which it can't handle
            'E266',  # too many leading '#' for block comment
            'E731',  # do not assign a lambda expression, use a def
            'W293',  # Blank line contains whitespace
        )
        style.options.max_line_length = 120  # because it isn't 1928 anymore

        python_files = collect_python_files(projdir)
        report = style.check_files(python_files)

        if report.total_errors:
            report.print_statistics()
            raise AssertionError('PEP8 style errors: %d' % report.total_errors)

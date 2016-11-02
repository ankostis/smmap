import doctest
import sys

import os.path as osp


try:
    from unittest import skipIf
    import unittest
except ImportError:
    from unittest2 import skipIf  # @UnusedImport
    import unittest2 as unittest

mydir = osp.dirname(__file__)
proj_path = osp.normpath(osp.join(mydir, '..', '..'))
readme_path = osp.join(proj_path, 'README.md')
tutorial_path = osp.join(proj_path, 'doc', 'source', 'tutorial.rst')


@skipIf(sys.version_info[:2] != (3, 5), "Doctests are made for py >= 3.5")
class Doctest(unittest.TestCase):

    # def test_doctest_README(self):
    #     failure_count, test_count = doctest.testfile(
    #         readme_path, module_relative=False,
    #         optionflags=doctest.NORMALIZE_WHITESPACE)
    #     self.assertGreater(test_count, 0, (failure_count, test_count))
    #     self.assertEquals(failure_count, 0, (failure_count, test_count))

    def test_doctest_tutorial(self):
        failure_count, test_count = doctest.testfile(
            tutorial_path, module_relative=False,
            optionflags=(doctest.NORMALIZE_WHITESPACE |
                         doctest.ELLIPSIS))
        self.assertGreater(test_count, 0, (failure_count, test_count))
        self.assertEquals(failure_count, 0, (failure_count, test_count))

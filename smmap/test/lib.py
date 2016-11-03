"""Provide base classes for the test system"""
import logging
import os
import tempfile
from unittest import TestCase

from smmap.util import finalize


__all__ = ['TestBase', 'FileCreator']

logging.basicConfig(level=0)

#{ Utilities


class FileCreator(object):

    """A instance which creates a temporary file with a prefix and a given size
    and provides this info to the user.
    Once it gets deleted, it will remove the temporary file as well."""
    __slots__ = ("_size", "_path", "_finalize", "__weakref__")

    def __init__(self, size, prefix='', final_byte=b'1'):
        assert size, "Require size to be larger 0"

        self._path = tempfile.mktemp(prefix=prefix)
        self._size = size
        self._finalize = finalize(self, self.remove)

        with open(self._path, "wb") as fp:
            fp.write(os.urandom(size - 1))
            fp.write(final_byte)

        assert os.path.getsize(self.path) == size

    def remove(self):
        try:
            os.remove(self.path)
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.remove()

    @property
    def path(self):
        return self._path

    @property
    def size(self):
        return self._size

#} END utilities


class TestBase(TestCase):

    """Foundation used by all tests"""

    #{ Configuration
    k_window_test_size = 1000 * 1000 * 8 + 5195
    #} END configuration

    #{ Overrides
    @classmethod
    def setUpAll(cls):
        # nothing for now
        pass

    # END overrides

    #{ Interface

    #} END interface

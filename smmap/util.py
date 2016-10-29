"""Generic and compatibility utilities."""
import logging
import sys


__all__ = ["PY3", "is_64_bit", "buffer", 'suppress']

log = logging.getLogger(__name__)

#{ Utilities

try:
    # Python 2
    buffer = buffer  # @UndefinedVariable
except NameError:
    # Python 3 has no `buffer`; only `memoryview`
    def buffer(obj, offset, size):
        # Actually, for gitpython this is fastest ... but `memoryviews` LEAK!
        #return memoryview(obj)[offset:offset + size]
        return obj[offset:offset + size]


#:True if the system is 64 bit. Otherwise it can be assumed to be 32 bit
is_64_bit = sys.maxsize > (1 << 32) - 1
PY3 = sys.version_info[0] >= 3


def string_types():
    if PY3:
        return str
    else:
        return basestring  # @UndefinedVariable

#}END utilities


#{ Utility Classes

## Copied from python std-lib.
class suppress:
    """Context manager to suppress specified exceptions

    After the exception is suppressed, execution proceeds with the next
    statement following the with statement.

         with suppress(FileNotFoundError):
             os.remove(somefile)
         # Execution still resumes here if the file was already removed
    """

    def __init__(self, *exceptions):
        self._exceptions = exceptions

    def __enter__(self):
        pass

    def __exit__(self, exctype, excinst, exctb):
        # Unlike isinstance and issubclass, CPython exception handling
        # currently only looks at the concrete type hierarchy (ignoring
        # the instance and subclass checking hooks). While Guido considers
        # that a bug rather than a feature, it's a fairly hard one to fix
        # due to various internal implementation details. suppress provides
        # the simpler issubclass based semantics, rather than trying to
        # exactly reproduce the limitations of the CPython interpreter.
        #
        # See http://bugs.python.org/issue12029 for more details
        supp = exctype is not None and issubclass(exctype, self._exceptions)
        if supp:
            log.debug("Suppressed exception: %s(%s)", exctype, excinst, exc_info=1)
        return supp

#} END utility classes

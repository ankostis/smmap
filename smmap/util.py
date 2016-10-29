"""Generic and compatibility utilities."""
import logging
import sys


__all__ = ["PY3", "is_64_bit", "buffer", 'suppress', 'string_types']

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


if PY3:
    string_types = str
else:
    string_types = basestring  # @UndefinedVariable

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


class Relations(dict):
    """Maintains the integrity of a "1-to-1" or "N-to-1" mappings.

    The "1-1" flavor is inversible through `.inv` property.

    Assertions supported:

    - insertion/removal: null key-values
    - insertion: pre-existing key
    - insertion: non-unique values ("1-1" mappings only)
    - removal: non-existing key
    - removal: null popped value
    - removal: popped value mismcatch popped key (1-1 only)

    """
    __slots__ = ('name',
                 'null_keys',
                 'null_values',
                 'inv',             #: For "1-1" mapping, this is a `dict()` populated with {values->key}.
                 'on_put_error',    #: A `callable(registry, k, v)` to fix state on errors.
                 'on_take_error',   #: A `callable(registry, k, (null)v)` to fix state on errors.
                 'kname',           #: label printed in messages
                 'vname',           #: label printed in messages
                 )

    def __init__(self, name='', one2one=False,
                 null_keys=False, null_values=False,
                 kname='KEY', vname='VALUE',
                 on_put_error=None, on_take_error=None,
                 ):
        if one2one:
            self.inv = type(self)(name, False,
                                  null_values, null_keys,
                                  vname, kname,
                                  on_put_error, on_take_error,
                                  )
            self.inv.inv = self
        else:
            self.inv = None
        self.name = name
        self.null_keys = null_keys
        self.null_values = null_values
        self.on_put_error = on_put_error
        self.on_take_error = on_take_error
        self.kname = kname
        self.vname = vname

    def put(self, k, v):
        kname = self.kname
        vname = self.vname
        inverse = self.inv

        ok = False
        try:
            assert self.null_keys or k, self._err_msg("Null", kname, k, v)
            assert self.null_values or v, self._err_msg("Null", vname, k, v)
            assert k not in self, self._err_msg("Already", kname, k, v)

            if inverse is not None:
                assert v not in inverse, self._err_msg("Already", vname, k, v)
                inverse[v] = k
            self[k] = v

            ok = True
        finally:
            if not ok and self.on_put_error:
                with suppress(Exception):
                    self.on_put_error(self, k, v)

    def take(self, k):
        kname = self.kname
        vname = self.vname
        inverse = self.inv

        ok = False
        try:
            v = self.get(k)

            assert self.null_keys or k, self._err_msg("Null", kname, k, v)
            assert k in self, self._err_msg("Missing", kname, k, v)

            if inverse:
                assert v in inverse, self._err_msg("Missing", vname, k, v)
                kk = inverse.get(v)
                assert k is kk, self._err_msg("Missmatch", vname, k, v, kk)
                del inverse[v]
            del self[k]

            assert self.null_values or bool(v), self._err_msg("Empty", vname, k)

            ok = True
        finally:
            if not ok and self.on_take_error:
                with suppress(Exception):
                    self.on_take_error(self, k, v)

        return (k, v)

    def _err_msg(self, msg, item, k, v):
        link = '-->' if self.inv is None else '<->'
        return '%s(%s %s %s): %s %s(k=%s, v=%s)\n  %s' % (
            self.name, self.kname, link, self.vname, msg, item, k, v, self)

#} END utility classes

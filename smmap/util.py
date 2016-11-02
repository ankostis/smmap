"""Generic and compatibility utilities."""
from collections import MutableMapping
import itertools
import logging
import sys
from weakref import ref

try:
    from contextlib import ExitStack
except ImportError:
    from contextlib2 import ExitStack  # @UnusedImport

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict  # @UnusedImport PY26-

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


class Relation(MutableMapping):
    """A single-threaded, integrity checked, transactional, "N-to-1" or "1-to-1"(invertible) mapping.

    The "1-1" flavor is invertible through the :attr:`inv` property.

    Any integrity errors are reported as :class:`KeyError` and *do not modify the mapping*.
    If you still need to revert changes AFTER they have been committed, use "Transactions".

    Integrity checks supported:

    - insertion/removal: null key-values
    - insertion: pre-existing key
    - insertion: non-unique values ("1-1" mappings only)
    - removal: non-existing key
    - removal: popped value mismatch popped key (1-1 only, with `is` comparison)

    **Transactions**

    Use it as a context-manager like this::


        >>> rg = Relation(one2one=True)
        >>> rg.put(1, 11)
        >>> d = rg.copy()  # just to assert results

        >>> try:
        ...     with rg:
        ...         rg.put(2, 22)
        ...
        ...         raise Exception()
        ... except:
        ...     pass
        >>> assert rg == d

    If you need to modify multiple relations and revert them if anyone fails (most probable),
    use an :class:`ExitStack` like this::

        >>> rg1, rg2 = Relation(one2one=True), Relation()
        >>> rg1.put(0,0)
        >>> d1, d2 = rg1.copy(), rg2.copy()  # just to assert results

        >>> try:
        >>>     with ExitStack() as exs:
        ...         exs.enter_context(rg1).put(1, 11)
        ...         exs.enter_context(rg2).put(1, 12)
        ...
        ...         raise Exception()
        ... except:
        ...     pass

        >>> assert rg2 == d1
        >>> assert rg2 == d2

    """
    __slots__ = ('name',
                 'rel',
                 'inv',             #: For "1-1" mapping, this is a `dict()` populated with {values->key}.
                 'null_keys',       #: Whether to accept null-keys on insert.
                 'null_values',     #: Whether to accept null-values on insert.
                 'on_put_error',    #: A `callable(registry, k, v)` to fix state on errors.
                 'on_take_error',   #: A `callable(registry, k, (null)v)` to fix state on errors.
                 'kname',           #: label printed in messages
                 'vname',           #: label printed in messages
                 '_rollback_copy',
                 )

    class _Missing():
        def __repr__(self):
            return "<MISSING>"

    MISSING = _Missing()

    def __init__(self, name='', one2one=False,
                 null_keys=False, null_values=False,
                 kname='KEY', vname='VALUE',
                 on_put_error=None, on_take_error=None,
                 dictfact=OrderedDict):
        self.rel = dictfact()
        if one2one:
            self.inv = type(self)(name, False,
                                  null_values, null_keys,
                                  vname, kname,
                                  on_put_error, on_take_error,
                                  dictfact)
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

    def __enter__(self):
        self._rollback_copy = (self.rel.copy(), None if self.inv is None else self.inv.copy())
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.rel, self.inv = self._rollback_copy
        self._rollback_copy = None

    def __getitem__(self, key):
        return self.rel[key]

    def __delitem__(self, key):
        del self.rel[key]

    def __setitem__(self, key, value):
        self.rel[key] = value

    def __iter__(self):
        return iter(self.rel)

    def __len__(self):
        return len(self.rel)

    def copy(self):
        return self.rel.copy()

    def clear(self):
        return self.rel.clear()

    def update(self, *args, **kwds):
        return self.rel.update(*args, **kwds)

    def items(self):
        return self.rel.items()

    def keys(self):
        return self.rel.keys()

    def values(self):
        return self.rel.values()

    def put(self, k, v):
        action = 'PUT'
        kname = self.kname
        vname = self.vname
        rel = self.rel
        inverse = self.inv

        ok = False
        try:
            if not self.null_keys and k is None:
                raise KeyError(self._err_msg(action, "Null %s" % kname, k, v))
            if not self.null_values and v is None:
                raise KeyError(self._err_msg(action, "Null %s" % vname, k, v))
            vv = rel.get(k, Relation.MISSING)
            if vv is not Relation.MISSING:
                raise KeyError(self._err_msg(action, "%s already mapped to %s" % (kname, vv), k, v))

            if inverse is not None:
                kk = inverse.get(v, Relation.MISSING)
                if kk is not Relation.MISSING:
                    raise KeyError(self._err_msg(action, "%s already invert-mapped to %s" % (vname, kk), k, v))
                inverse[v] = k
            rel[k] = v

            ok = True
        finally:
            if not ok and self.on_put_error:
                with suppress(Exception):
                    self.on_put_error(self, k, v)

    def take(self, k):
        action = 'TAKE'
        kname = self.kname
        vname = self.vname
        rel = self.rel
        inverse = self.inv

        ok = False
        try:
            v = rel.get(k, Relation.MISSING)
            if v is Relation.MISSING:
                raise KeyError(self._err_msg(action, "Missing %s" % kname, k, v))

            if inverse:
                kk = inverse.get(v, Relation.MISSING)
                if kk is Relation.MISSING:
                    raise KeyError(self._err_msg(action, "Missing invert-%s" % vname, k, v))
                if k is not kk:
                    raise KeyError(self._err_msg(action, "Mismatch %s with inverted: %r <> %r" %
                                                 (kname, k, kk), k, v))
                del inverse[v]
            del rel[k]

            ok = True
        finally:
            if not ok and self.on_take_error:
                with suppress(Exception):
                    self.on_take_error(self, k, v)

        return v

    def _err_msg(self, action, msg, k, v):
        link = '-->' if self.inv is None else '<->'
        return '%s %s{%s%s%s}: %s (key: %s, value: %s)\n  %s' % (
            action, self.name, self.kname, link, self.vname, msg, k, v, self)

    def hit(self, k):
        """Maintain LRU, by moving key to the end."""
        rel = self.rel
        v = rel.get(k, Relation.MISSING)
        if v is Relation.MISSING:
            raise KeyError(self._err_msg('HIT', "Missing %s" % self.kname, k, v))
        try:
            rel.move_to_end(k)
        except AttributeError:
            rel[k] = rel.pop(k)

        ## TODO: Hit also inverse...

try:
    from weakref import finalize  # @UnusedImport
except ImportError:
    ## Copied from PY3 sources
    class finalize:
        """Class for finalization of weakrefable objects

        finalize(obj, func, *args, **kwargs) returns a callable finalizer
        object which will be called when obj is garbage collected. The
        first time the finalizer is called it evaluates func(*arg, **kwargs)
        and returns the result. After this the finalizer is dead, and
        calling it just returns None.

        When the program exits any remaining finalizers for which the
        atexit attribute is true will be run in reverse order of creation.
        By default atexit is true.
        """

        # Finalizer objects don't have any state of their own.  They are
        # just used as keys to lookup _Info objects in the registry.  This
        # ensures that they cannot be part of a ref-cycle.

        __slots__ = ()
        _registry = {}
        _shutdown = False
        _index_iter = itertools.count()
        _dirty = False
        _registered_with_atexit = False

        class _Info:
            __slots__ = ("weakref", "func", "args", "kwargs", "atexit", "index")

        def __init__(self, obj, func, *args, **kwargs):
            if not self._registered_with_atexit:
                # We may register the exit function more than once because
                # of a thread race, but that is harmless
                import atexit
                atexit.register(self._exitfunc)
                finalize._registered_with_atexit = True
            info = self._Info()
            info.weakref = ref(obj, self)
            info.func = func
            info.args = args
            info.kwargs = kwargs or None
            info.atexit = True
            info.index = next(self._index_iter)
            self._registry[self] = info
            finalize._dirty = True

        def __call__(self, _=None):
            """If alive then mark as dead and return func(*args, **kwargs);
            otherwise return None"""
            info = self._registry.pop(self, None)
            if info and not self._shutdown:
                return info.func(*info.args, **(info.kwargs or {}))

        def detach(self):
            """If alive then mark as dead and return (obj, func, args, kwargs);
            otherwise return None"""
            info = self._registry.get(self)
            obj = info and info.weakref()
            if obj is not None and self._registry.pop(self, None):
                return (obj, info.func, info.args, info.kwargs or {})

        def peek(self):
            """If alive then return (obj, func, args, kwargs);
            otherwise return None"""
            info = self._registry.get(self)
            obj = info and info.weakref()
            if obj is not None:
                return (obj, info.func, info.args, info.kwargs or {})

        @property
        def alive(self):
            """Whether finalizer is alive"""
            return self in self._registry

        @property
        def atexit(self):
            """Whether finalizer should be called at exit"""
            info = self._registry.get(self)
            return bool(info) and info.atexit

        @atexit.setter
        def atexit(self, value):
            info = self._registry.get(self)
            if info:
                info.atexit = bool(value)

        def __repr__(self):
            info = self._registry.get(self)
            obj = info and info.weakref()
            if obj is None:
                return '<%s object at %#x; dead>' % (type(self).__name__, id(self))
            else:
                return '<%s object at %#x; for %r at %#x>' % \
                    (type(self).__name__, id(self), type(obj).__name__, id(obj))

        @classmethod
        def _select_for_exit(cls):
            # Return live finalizers marked for exit, oldest first
            L = [(f, i) for (f, i) in cls._registry.items() if i.atexit]
            L.sort(key=lambda item: item[1].index)
            return [f for (f, i) in L]

        @classmethod
        def _exitfunc(cls):
            # At shutdown invoke finalizers for which atexit is true.
            # This is called once all other non-daemonic threads have been
            # joined.
            reenable_gc = False
            try:
                if cls._registry:
                    import gc
                    if gc.isenabled():
                        reenable_gc = True
                        gc.disable()
                    pending = None
                    while True:
                        if pending is None or finalize._dirty:
                            pending = cls._select_for_exit()
                            finalize._dirty = False
                        if not pending:
                            break
                        f = pending.pop()
                        try:
                            # gc is disabled, so (assuming no daemonic
                            # threads) the following is the only line in
                            # this function which might trigger creation
                            # of a new finalizer
                            f()
                        except Exception:
                            sys.excepthook(*sys.exc_info())
                        assert f not in cls._registry
            finally:
                # prevent any more finalizers from executing during shutdown
                finalize._shutdown = True
                if reenable_gc:
                    gc.enable()

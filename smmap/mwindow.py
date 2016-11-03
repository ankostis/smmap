"""
Memory-managers provide static or sliding windows on memory mapped files.

Cursors/Regiond Differences
=============================
- lifecycle: regions are long-lived managed by mman, cursors are only allowed as context-managers.
- offsets: cursors are have exactly placement of offsets, regions are page aligned

"""
import logging
import sys

from .util import buffer, finalize, suppress


__all__ = ["FixedWindowCursor", "SlidingWindowCursor", "MemmapRegion"]

log = logging.getLogger(__name__)


class WindowHandle(object):
    """
    Abstract non-re-entrant no-reusable context-manager for a mman-managed memory window into a file.

    :ivar mman:
        the manger keeping all windows regions
    :ivar finfo:
        the file to map, or the opened file descriptor
    :ivar finfo:
        the file we are acting upon
    :ivar ofs:
        the absolute offset from the actually mapped area to our start area
    :ivar size:
        maximum size we should provide

    @property
    def self.closed():
        (abstract) return True if already closed

    def release():
        (abstract) must clean up any resources or fail on any irregularity

    """

    __slots__ = (
        'mman',         # the manger keeping all file regions
        'finfo',        # the file we are acting upon
        'ofs',          # the absolute offset from the actually mapped area to our start area
        'size',         # maximum size we should provide
        '_finalize',    # To replace __del_
        '__weakref__',  # To replace __del_
    )

    def __init__(self, mman, finfo, ofs=0, size=0):
        assert size > 0 and ofs >= 0, (mman, finfo, ofs, size)
        self.mman = mman
        self.finfo = finfo
        self.ofs = ofs
        self.size = size
        self._finalize = finalize(self, self.release)

    def __repr__(self):
        return "%s(%s, %i, %i)" % (type(self).__name__, self.finfo, self.ofs, self.size)

    def __hash__(self):
        return id(self)

    def __len__(self):
        return self.size

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """"Will raises if it has been double-entered."""
        with suppress(Exception if exc_type else ()):
            self.release()

    def close(self):
        """Closes the current windows. Does nothing if already closed."""
        if not self.closed:
            self.release()

    @property
    def path_or_fd(self):
        return self.finfo.path_or_fd

    @property
    def path(self):
        """:return: path of the underlying mapped file

        :raise AssertionError: if attached path is not a path"""
        pathfd = self.finfo.path_or_fd
        assert not isinstance(pathfd, int), (
            "Path queried on %s although cursor created with a file descriptor(%s)!"
            "\n  Use `fd` or `path_or_fd` properties instead." % (self, pathfd))

        return pathfd

    @property
    def fd(self):
        """:return: file descriptor used to create the underlying mapping.

        :raise AssertionError: if the mapping was not created by a file descriptor"""
        pathfd = self.finfo.path_or_fd
        assert isinstance(pathfd, int), (
            "File-descriptor queried on %s although cursor created with a path(%s)!"
            "\n  Use `path` or `path_or_fd` properties instead." % (self, pathfd))

    @property
    def ofs_end(self):
        """:return: Absolute offset to one byte beyond the mapping into the file"""
        return self.ofs + self.size

    @property
    def file_size(self):
        """:return: size of the underlying file"""
        return self.finfo.file_size

    def includes_ofs(self, ofs):
        """:return: True if the given offset can be read in our mapped region"""
        return self.ofs <= ofs < self.ofs + self.size


class MemmapRegion(WindowHandle):

    """Encapsulates the os-level `mmap` handle, which is aligned to pagesizes.

    :ivar ofs:
        **aligned** offset into the file to be mapped
    :ivar size:
        if size is larger then the file on disk, the whole file will be
        allocated the the size automatically adjusted

        .. Note::
            The actually size may be smaller than requested, either because
            the file-size is smaller, or the map was created between two existing regions.

    """
    __slots__ = ()

    #{ Interface

    @property
    def closed(self):
        return self.mman.is_region_closed(self)

    def cursors(self):
        """:return: a tuple of all cursors bound to the region"""
        return self.mman.cursors_for_region(self)

    def buffer(self):
        """:return: a buffer containing the memory"""
        return self.mman._mmap_for_region(self)

    def release(self):
        """Release all resources this instance might hold.

        Invoked by *mman* when closing or purging unused regions to make space.
        If invoked while still cursors are bound, they will fail later, when attempting
        to access the underlying mmap.
        """
        self.mman._release_region(self)

    #} END interface


class FixedWindowCursor(WindowHandle):

    """
    Pointer into the mapped region of the memory manager, keeping the map
    alive until it is destroyed and no other client uses it.

    .. Tip::
        Cursors should not be created manually, but though returned by
        :meth:`GreedyMemmapManager.make_cursor()` or :meth:`TilingMemmapManager.make_cursor()`.

        It is recommended to close a cursor once you are done reading/writing,
        to help its referred region to get collected sooner.

        Since it is a NON re-entrant, non thread-safe, optional context-manager,
        it may be used within a ``with ...:`` block.
    """

    @property
    def closed(self):
        return self.mman.is_cursor_closed(self)

    def make_cursor(self, offset=None, size=None, flags=None):
        """:return: a new cursor for the new offset/size/flags.

        For the params see :meth:`GreedyMemmapManager.make_cursor()`.
        """
        kwds = dict((k, v) for k, v in locals().items() if v is not None)
        kwds.pop('self')
        return self.mman.make_cursor(self.path_or_fd, **kwds)

    def next_cursor(self, offset=None, size=None, flags=0):
        """
        :param ofs:
            If not specified, it becomes ``self.ofs + self.size``.
        :param size:
            If not specified, it is fetched from this instance.
        :return:
            a new cursor for the new offset/size/flags.

        For the params see also :meth:`GreedyMemmapManager.make_cursor()`.
        """
        if offset is None:
            offset = self.ofs + self.size
        if size is None:
            size = self.size
        return self.mman.make_cursor(self.path_or_fd, offset, size, flags)

    def release(self):
        """Closes the current window. fails if already closed."""
        self.mman._release_cursor(self)

    def buffer(self):
        """Return a buffer object which allows access to our memory region from our offset
        to the window size. Please note that it might be smaller than you requested when created

        .. Note::
            buffers should not be cached passed the duration of your access as it will
            prevent resources from being freed even though they might not be accounted for anymore !"""
        region = self.region
        return buffer(region.buffer(), self.ofs - region.ofs, self.size)

    @property
    def region(self):
        """:return: our mapped region, or None if cursor is closed """
        return self.mman.region_for_cursor(self)


class SlidingWindowCursor(WindowHandle):

    """
    A read-only buffer like object which allows direct byte-wise object and slicing into
    memory of a mapped file. The mapping is controlled by the slicing

    The buffer is relative, that is if you map an offset, index 0 will map to the
    first byte at the offset you used during initialization.

    **Note:** Although this type effectively hides the fact that there are mapped windows
    underneath, it can unfortunately not be used in any non-pure python method which
    needs a buffer or string.
    """
    __slots__ = ('flags',)  # flags for cursor creation

    def __init__(self, mman, finfo, offset, size, flags):
        super(SlidingWindowCursor, self).__init__(mman, finfo, offset, size)
        self.flags = flags

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def __getitem__(self, i):
        size = self.size
        if isinstance(i, slice):
            return self.__getslice__(i.start or 0, i.stop or size)

        ii = size + i if i < 0 else i
        if not 0 <= ii < size:
            raise IndexError('Offset(%s) out of bounds %s!' % i, self)
        i = ii

        ofs = self.ofs
        ai = ofs + i  # absolute i
        with self.mman._cursor_bound(self, ai, 1, self.flags) as r:
            rofs = r.ofs
            return r.buffer()[ai - rofs]

    def __getslice__(self, i, j):
        # fast path, slice fully included - safes a concatenate operation and
        # should be the default

        cursor_bound = self.mman._cursor_bound
        flags = self.flags
        size = self.size

        if i < 0:
            i = max(0, size + i)
        if j == sys.maxsize:
            j = size
        else:
            if j < 0:
                j = max(0, size + j)

        ai = self.ofs + i  # absolute i
        l = j - i     # slice length
        # It's fastest to keep tokens and join later, especially in py3, which was 7 times slower
        # in the previous iteration of this code
        pyvers = sys.version_info[:2]
        if (3, 0) <= pyvers <= (3, 3):
            # Memory view cannot be joined below python 3.4 ...
            out = bytes()
            while l:
                with cursor_bound(self, ai, l, flags) as r:
                    rofs = r.ofs
                    d = r.buffer()[ai - rofs:ai + l - rofs]
                    ai += len(d)
                    l -= len(d)

                    # This is slower than the join ... but what can we do ...
                    out += d
                    #del(d)  # not needed, no memoryview

        else:
            md = []
            while l:
                with cursor_bound(self, ai, l, flags) as r:
                    rofs = r.ofs
                    d = r.buffer()[ai - rofs:ai + l - rofs]
                    ai += len(d)
                    l -= len(d)
                    # Make sure we don't keep references, as c.use_region() might attempt
                    # to free resources, but can't unless we use pure bytes
                    #if hasattr(d, 'tobytes'):  NO, no memoryvies...
                    #    d = d.tobytes()
                    md.append(d)

            out = bytes().join(md)
        # END fast or slow path

        return out

    #{ Interface

    @property
    def closed(self):
        """Closes only if parent mmemap-manager has closed."""
        return self.mman.closed

    def release(self):
        """Do nothing, regions are held only while its methods are running."""

    #}END interface

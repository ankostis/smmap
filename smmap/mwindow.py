"""
Memory-managers provide static or sliding windows on memory mapped files.

Cursors/Regiond Differences
=============================
- lifecycle: regions are long-lived managed by mman, cursors are only allowed as context-managers.
- offsets: cursors are have exactly placement of offsets, regions are page aligned

"""
import logging

from smmap.util import buffer, finalize
import sys


__all__ = ["FixedWindowCursor", "SlidingWindowCursor", "MapRegion"]

log = logging.getLogger(__name__)


class WindowHandle(object):
    """
    Abstract non-re-entrant no-reusable context-manager for a mman-managed memory window into a file.

    @property
    def self.closed():
        (abstract) return True if already closed

    def release():
        (abstract) must clean up any resources or fail on any irregularity

    """

    __slots__ = (
        'mman',         # the manger keeping all file regions
        'finfo',   # the file we are acting upon
        'ofs',          # the absolute offset from the actually mapped area to our start area
        'size',         # maximum size we should provide
        '_finalize',    # To replace __del_
        '__weakref__',  # To replace __del_
    )

    def __init__(self, mman, finfo, ofs=0, size=0):
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
        try:
            self.release()
        except Exception as ex:
            if exc_type:
                log.warning('Hidden exit-exception on %s: %s' % (self, ex), exc_info=1)
            else:
                raise ex

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


class MapRegion(WindowHandle):

    """Defines a mapped region of memory, aligned to pagesizes

    :ivar path_or_fd:
        path to the file to map, or the opened file descriptor
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

    """A buffer like object which allows direct byte-wise object and slicing into
    memory of a mapped file. The mapping is controlled by the slicing

    The buffer is relative, that is if you map an offset, index 0 will map to the
    first byte at the offset you used during initialization.

    **Note:** Although this type effectively hides the fact that there are mapped windows
    underneath, it can unfortunately not be used in any non-pure python method which
    needs a buffer or string"""
    __slots__ = (
        '_c',           # our cursor
        'flags',        # flags for cursor crearion
    )

    def __init__(self, mman, path_or_fd, offset=0, size=0, flags=0):
        # TODO: create sliding-buf from mman
        """Initalize the instance to operate on the given cursor.

        :param cursor: the cursor to the file you want to access
        :param offset: absolute offset in bytes
        :param size: the total size of the mapping. non-positives mean, as big possible.
            From that point on, the __len__ of the buffer will be the given size or the file size.
            If the size is larger than the mappable area, you can only access the actually available
            area, although the length of the buffer is reported to be your given size.
            Hence it is in your own interest to provide a proper size !
        :param flags: Additional flags to be passed to os.open
        :raise ValueError: if the buffer could not achieve a valid state"""
        self.flags = flags
        finfo = mman._get_or_create_finfo(path_or_fd)
        avail_size = finfo.file_size - offset
        if 0 < size < avail_size:
            avail_size = size
        super(SlidingWindowCursor, self).__init__(mman, finfo, offset, avail_size)
        self._c = None

    def __enter__(self):
        self._c = self.mman.make_cursor(self.path_or_fd, self.ofs, self.size, self.flags)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def __getitem__(self, i):
        if isinstance(i, slice):
            return self.__getslice__(i.start or 0, i.stop or self.size)
        c = self._c
        assert not c.closed
        if i < 0:
            i = self.size + i
        if not c.includes_ofs(i):
            c.release()
            self._c = c = c.make_cursor(i, 1)
        # END handle region usage
        return c.buffer()[i - c.ofs]

    def __getslice__(self, i, j):
        c = self._c
        # fast path, slice fully included - safes a concatenate operation and
        # should be the default
        assert not c.closed
        if i < 0:
            i = self.size + i
        if j == sys.maxsize:
            j = self.size
        if j < 0:
            j = self.size + j
        if (c.ofs <= i) and (j < c.ofs_end):
            b = c.ofs
            return c.buffer()[i - b:j - b]
        else:
            l = j - i                 # total length
            ofs = i
            # It's fastest to keep tokens and join later, especially in py3, which was 7 times slower
            # in the previous iteration of this code
            pyvers = sys.version_info[:2]
            if (3, 0) <= pyvers <= (3, 3):
                # Memory view cannot be joined below python 3.4 ...
                out = bytes()
                while l:
                    c.release()
                    self._c = c = c.make_cursor(ofs, l)
                    d = c.buffer()[:l]
                    ofs += len(d)
                    l -= len(d)
                    # This is slower than the join ... but what can we do ...
                    out += d
                    del(d)
                # END while there are bytes to read
                return out
            else:
                md = []
                while l:
                    c.release()
                    self._c = c = c.make_cursor(ofs, l)
                    d = c.buffer()[:l]
                    ofs += len(d)
                    l -= len(d)
                    # Make sure we don't keep references, as c.use_region() might attempt
                    # to free resources, but can't unless we use pure bytes
                    if hasattr(d, 'tobytes'):
                        d = d.tobytes()
                    md.append(d)
                # END while there are bytes to read
                return bytes().join(md)
        # END fast or slow path
    #{ Interface

    @property
    def closed(self):
        assert not self._c or not self._c.closed
        return not bool(self._c)

    def release(self):
        """Call this method once you are done using the instance. It is automatically
        destroys cursor, and should be called just in time to allow system
        resources to be freed.
        """
        ## TODO: Buf inherit cursor.
        #  For now, have to break the "fail" rul becuase is not implemented
        #          #  an a "one off" context-manager.
        if self._c:
            self._c.release()
            self._c = None

    @property
    def cursor(self):
        """:return: the current cursor providing access to the data, which is None initially"""
        return self._c

    #}END interface

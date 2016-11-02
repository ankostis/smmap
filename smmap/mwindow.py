"""
Memory-managers provide static or sliding windows on memory mapped files.

Cursors/Regiond Differences
=============================
- lifecycle: regions are long-lived managed by mman, cursors are only allowed as context-managers.
- offsets: cursors are have exactly placement of offsets, regions are page aligned

"""
import logging

from smmap.util import buffer, finalize


__all__ = ["WindowCursor", "MapRegion"]

log = logging.getLogger(__name__)


class _WindowHandle(object):
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
        'path_or_fd',   # the file we are acting upon
        'ofs',          # the absolute offset from the actually mapped area to our start area
        'size',         # maximum size we should provide
        '_finalize',    # To replace __del_
        '__weakref__',  # To replace __del_
    )

    def __init__(self, mman, path_or_fd, ofs=0, size=0):
        self.mman = mman
        self.path_or_fd = path_or_fd
        self.ofs = ofs
        self.size = size
        self._finalize = finalize(self, self.release)

    def __repr__(self):
        return "%s(%s, %i, %i)" % (type(self).__name__, self.path_or_fd, self.ofs, self.size)

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
    def path(self):
        """:return: path of the underlying mapped file

        :raise AssertionError: if attached path is not a path"""
        pathfd = self.path_or_fd
        assert not isinstance(pathfd, int), (
            "Path queried on %s although cursor created with a file descriptor(%s)!"
            "\n  Use `fd` or `path_or_fd` properties instead." % (self, pathfd))

        return pathfd

    @property
    def fd(self):
        """:return: file descriptor used to create the underlying mapping.

        :raise AssertionError: if the mapping was not created by a file descriptor"""
        pathfd = self.path_or_fd
        assert isinstance(pathfd, int), (
            "File-descriptor queried on %s although cursor created with a path(%s)!"
            "\n  Use `path` or `path_or_fd` properties instead." % (self, pathfd))

    @property
    def rlist(self):
        """:return: our mapped region, or None if nothing is mapped yet
        :raise AssertionError: if we have no current region"""
        return self.mman.rlist_for_path_or_fd(self.path_or_fd)

    @property
    def ofs_end(self):
        """:return: Absolute offset to one byte beyond the mapping into the file"""
        return self.ofs + self.size

    def includes_ofs(self, ofs):
        """:return: True if the given offset can be read in our mapped region"""
        return self.ofs <= ofs < self.ofs + self.size


class WindowCursor(_WindowHandle):

    """
    Pointer into the mapped region of the memory manager, keeping the map
    alive until it is destroyed and no other client uses it.

    .. Tip::
        Cursors should not be created manually, but though returned by
        :meth:`StaticWindowMapManager.make_cursor()` or :meth:`SlidingWindowMapManager.make_cursor()`.

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

        For the params see :meth:`StaticWindowMapManager.make_cursor()`.
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

        For the params see also :meth:`StaticWindowMapManager.make_cursor()`.
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

    def map(self):
        """
        :return: the underlying raw memory map. Please not that the offset and size is likely to be different
            to what you set as offset and size. Use it only if you are sure about the region it maps, which is the whole
            file in case of StaticWindowMapManager"""
        return self.region.buffer()

    @property
    def region(self):
        """:return: our mapped region, or None if cursor is closed """
        return self.mman.region_for_cursor(self)

    @property
    def file_size(self):
        """:return: size of the underlying file"""
        return self.rlist.file_size


class MapRegion(_WindowHandle):

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
        return self.mman.mmap_for_region(self)

    def release(self):
        """Release all resources this instance might hold.

        Invoked by *mman* when closing or purging unused regions to make space.
        If invoked while still cursors are bound, they will fail later, when attempting
        to access the underlying mmap.
        """
        self.mman._release_region(self)

    #} END interface

#} END utility classes

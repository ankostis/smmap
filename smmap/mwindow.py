"""Memory-managers provide static or sliding windows on memory mapped files."""
import logging
from mmap import mmap, ACCESS_READ
import os
import sys


__all__ = ["buffer",
           "WindowCursor", "MapRegion", "MapRegionList",
           ]

log = logging.getLogger(__name__)

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


class WindowCursor(object):

    """
    Pointer into the mapped region of the memory manager, keeping the map
    alive until it is destroyed and no other client uses it.

    Cursors should not be created manually, but are instead returned by the SlidingWindowMapManager

    **Note:**: The current implementation is suited for static and sliding window managers, but it also means
    that it must be suited for the somewhat quite different sliding manager. It could be improved, but
    I see no real need to do so."""
    __slots__ = (
        '_manager',  # the manger keeping all file regions
        '_rlist',   # a regions list with regions for our file
        '_region',  # our current class:`MapRegion` or None
        '_ofs',     # relative offset from the actually mapped area to our start area
        '_size'     # maximum size we should provide
    )

    def __init__(self, manager=None, regions=None):
        self._manager = manager
        self._rlist = regions
        self._region = None
        self._ofs = 0
        self._size = 0

    def __del__(self):
        self._destroy()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._destroy()

    def _destroy(self):
        """Destruction code to decrement counters"""
        self.unuse_region()

        if self._rlist is not None:
            # Actual client count, which doesn't include the reference kept by the manager, nor ours
            # as we are about to be deleted
            try:
                if len(self._rlist) == 0:
                    # Free all resources associated with the mapped file
                    self._manager._fdict.pop(self._rlist.path_or_fd())
                # END remove regions list from manager
            except (TypeError, KeyError):
                # sometimes, during shutdown, getrefcount is None. Its possible
                # to re-import it, however, its probably better to just ignore
                # this python problem (for now).
                # The next step is to get rid of the error prone getrefcount alltogether.
                pass
            # END exception handling
        # END handle regions

    def _copy_from(self, rhs):
        """Copy all data from rhs into this instance, handles usage count"""
        self._manager = rhs._manager
        self._rlist = type(rhs._rlist)(rhs._rlist)
        self._region = rhs._region
        self._ofs = rhs._ofs
        self._size = rhs._size

        for region in self._rlist:
            region.increment_client_count()

        if self._region is not None:
            self._region.increment_client_count()
        # END handle regions

    def __copy__(self):
        """copy module interface"""
        cpy = type(self)()
        cpy._copy_from(self)
        return cpy

    #{ Interface
    def assign(self, rhs):
        """Assign rhs to this instance. This is required in order to get a real copy.
        Alternativly, you can copy an existing instance using the copy module"""
        self._destroy()
        self._copy_from(rhs)

    def use_region(self, offset=0, size=0, flags=0):
        """Assure we point to a window which allows access to the given offset into the file

        :param offset: absolute offset in bytes into the file
        :param size: amount of bytes to map. If 0, all available bytes will be mapped
        :param flags: additional flags to be given to os.open in case a file handle is initially opened
            for mapping. Has no effect if a region can actually be reused.
        :return: this instance - it should be queried for whether it points to a valid memory region.
            This is not the case if the mapping failed because we reached the end of the file

        **Note:**: The size actually mapped may be smaller than the given size. If that is the case,
        either the file has reached its end, or the map was created between two existing regions"""
        need_region = True
        man = self._manager
        fsize = self._rlist.file_size()
        size = min(size or fsize, man.window_size() or fsize)   # clamp size to window size

        if self._region is not None:
            if self._region.includes_ofs(offset):
                need_region = False
            else:
                self.unuse_region()
            # END handle existing region
        # END check existing region

        # offset too large ?
        if offset >= fsize:
            return self
        # END handle offset

        if need_region:
            self._region = man._obtain_region(self._rlist, offset, size, flags, False)
            self._region.increment_client_count()
        # END need region handling

        self._ofs = offset - self._region._b
        self._size = min(size, self._region.ofs_end() - offset)

        return self

    def unuse_region(self):
        """Unuse the current region. Does nothing if we have no current region

        **Note:** the cursor unuses the region automatically upon destruction. It is recommended
        to un-use the region once you are done reading from it in persistent cursors as it
        helps to free up resource more quickly"""
        if self._region is not None:
            self._region.increment_client_count(-1)
        self._region = None
        # note: should reset ofs and size, but we spare that for performance. Its not
        # allowed to query information if we are not valid !

    def buffer(self):
        """Return a buffer object which allows access to our memory region from our offset
        to the window size. Please note that it might be smaller than you requested when calling use_region()

        **Note:** You can only obtain a buffer if this instance is_valid() !

        **Note:** buffers should not be cached passed the duration of your access as it will
        prevent resources from being freed even though they might not be accounted for anymore !"""
        return buffer(self._region.buffer(), self._ofs, self._size)

    def map(self):
        """
        :return: the underlying raw memory map. Please not that the offset and size is likely to be different
            to what you set as offset and size. Use it only if you are sure about the region it maps, which is the whole
            file in case of StaticWindowMapManager"""
        return self._region.map()

    def is_valid(self):
        """:return: True if we have a valid and usable region"""
        return self._region is not None

    def is_associated(self):
        """:return: True if we are associated with a specific file already"""
        return self._rlist is not None

    def ofs_begin(self):
        """:return: offset to the first byte pointed to by our cursor

        **Note:** only if is_valid() is True"""
        return self._region._b + self._ofs

    def ofs_end(self):
        """:return: offset to one past the last available byte"""
        # unroll method calls for performance !
        return self._region._b + self._ofs + self._size

    def size(self):
        """:return: amount of bytes we point to"""
        return self._size

    def region(self):
        """:return: our mapped region, or None if nothing is mapped yet
        :raise AssertionError: if we have no current region. This is only useful for debugging"""
        return self._region

    def includes_ofs(self, ofs):
        """:return: True if the given absolute offset is contained in the cursors
            current region

        **Note:** cursor must be valid for this to work"""
        # unroll methods
        return (self._region._b + self._ofs) <= ofs < (self._region._b + self._ofs + self._size)

    def file_size(self):
        """:return: size of the underlying file"""
        return self._rlist.file_size()

    def path_or_fd(self):
        """:return: path or file descriptor of the underlying mapped file"""
        return self._rlist.path_or_fd()

    def path(self):
        """:return: path of the underlying mapped file
        :raise ValueError: if attached path is not a path"""
        if isinstance(self._rlist.path_or_fd(), int):
            raise ValueError("Path queried although mapping was applied to a file descriptor")
        # END handle type
        return self._rlist.path_or_fd()

    def fd(self):
        """:return: file descriptor used to create the underlying mapping.

        **Note:** it is not required to be valid anymore
        :raise ValueError: if the mapping was not created by a file descriptor"""
        if isinstance(self._rlist.path_or_fd(), string_types()):
            raise ValueError("File descriptor queried although mapping was generated from path")
        # END handle type
        return self._rlist.path_or_fd()


class MapRegion(object):

    """Defines a mapped region of memory, aligned to pagesizes

    **Note:** deallocates used region automatically on destruction"""
    __slots__ = [
        '_b',   # beginning of mapping
        '_size',  # cached size of our memory map
        '_mf',  # mapped memory chunk (as returned by mmap)
        '_uc',  # total amount of usages
        '__weakref__'
    ]

    def __init__(self, path_or_fd, ofs, size, flags=0):
        """Initialize a region, allocate the memory map
        :param path_or_fd: path to the file to map, or the opened file descriptor
        :param ofs: **aligned** offset into the file to be mapped
        :param size: if size is larger then the file on disk, the whole file will be
            allocated the the size automatically adjusted
        :param flags: additional flags to be given when opening the file.
        :raise Exception: if no memory can be allocated"""
        self._b = ofs
        self._size = 0
        self._uc = 0

        if isinstance(path_or_fd, int):
            fd = path_or_fd
        else:
            fd = os.open(path_or_fd, os.O_RDONLY | getattr(os, 'O_BINARY', 0) | flags)
        # END handle fd

        try:
            actual_size = min(os.fstat(fd).st_size - ofs, size)
            self._mf = mmap(fd, actual_size, access=ACCESS_READ, offset=ofs)
            # END handle memory mode

            self._size = len(self._mf)
            # END handle buffer wrapping
        finally:
            if not isinstance(path_or_fd, int):
                os.close(fd)
            # END only close it if we opened it
        # END close file handle
        # We assume the first one to use us keeps us around
        self.increment_client_count()

    def __repr__(self):
        return "MapRegion<%i, %i>" % (self._b, self.size())

    #{ Interface

    def buffer(self):
        """:return: a buffer containing the memory"""
        return self._mf

    def map(self):
        """:return: a memory map containing the memory"""
        return self._mf

    def ofs_begin(self):
        """:return: absolute byte offset to the first byte of the mapping"""
        return self._b

    def size(self):
        """:return: total size of the mapped region in bytes"""
        return self._size

    def ofs_end(self):
        """:return: Absolute offset to one byte beyond the mapping into the file"""
        return self._b + self._size

    def includes_ofs(self, ofs):
        """:return: True if the given offset can be read in our mapped region"""
        return self._b <= ofs < self._b + self._size

    def client_count(self):
        """:return: number of clients currently using this region"""
        return self._uc

    def increment_client_count(self, ofs=1):
        """Adjust the usage count by the given positive or negative offset.
        If usage count equals 0, we will auto-release our resources
        :return: True if we released resources, False otherwise. In the latter case, we can still be used"""
        self._uc += ofs
        assert self._uc > -1, "Increments must match decrements, usage counter negative: %i" % self._uc

        if self.client_count() == 0:
            self.release()
            return True
        else:
            return False
        # end handle release

    def release(self, skip_client_count_check=False):
        """Release all resources this instance might hold. Must only be called if there usage_count() is zero"""
        self._mf.close()

        ## Only `mman.close()` invokes this method directly, regardless of client-counts.
        #  The rest, must be reported (mman does so, collectively).
        #
        if not skip_client_count_check and self._uc > 1:
            log.warning("Released region %s with '%s' active clients!", self, self._uc)

    #} END interface


class MapRegionList(list):

    """List of MapRegion instances associating a path with a list of regions."""
    __slots__ = (
        '_path_or_fd',  # path or file descriptor which is mapped by all our regions
        '_file_size'    # total size of the file we map
    )

    def __new__(cls, path):
        return super(MapRegionList, cls).__new__(cls)

    def __init__(self, path_or_fd):
        self._path_or_fd = path_or_fd
        self._file_size = None

    def path_or_fd(self):
        """:return: path or file descriptor we are attached to"""
        return self._path_or_fd

    def file_size(self):
        """:return: size of file we manager"""
        if self._file_size is None:
            if isinstance(self._path_or_fd, string_types()):
                self._file_size = os.stat(self._path_or_fd).st_size
            else:
                self._file_size = os.fstat(self._path_or_fd).st_size
            # END handle path type
        # END update file size
        return self._file_size

#} END utility classes

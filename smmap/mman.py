"""Module containing a memory memory manager which provides a sliding window on a number of memory mapped files"""
from functools import reduce
import logging
import sys

from .util import (
    PY3,
    MapWindow,
    MapRegion,
    MapRegionList,
    is_64_bit,
    string_types,
    buffer,
)
import gc


__all__ = ['managed_mmaps', "StaticWindowMapManager", "SlidingWindowMapManager", "WindowCursor"]
#{ Utilities
log = logging.getLogger(__name__)
#}END utilities


class WindowCursor(object):

    """
    Pointer into the mapped region of the memory manager, keeping the map
    alive until it is destroyed and no other client uses it.

    Cursors should not be created manually, but are instead returned by the SlidingWindowMapManager

    .. Tip::
        This is a NON re-entrant, not thread-safe, optional context-manager,
        to be used within a ``with ...:`` block, ensuring any left-overs regions
        are cleaned up.

    .. Warning::
        If not relying mmap-manager is not entered, :meth:`use_region()`` will scream.

    .. Note::
        The current implementation is suited for static and sliding window managers,
        but it also means that it must be suited for the somewhat quite different sliding manager.
        It could be improved, but I see no real need to do so."""
    __slots__ = (
        '_manager',     # the manger keeping all file regions
        '_rlist',       # a regions list with regions for our file
        '_region',      # our current class:`MapRegion` or None
        '_ofs',         # relative offset from the actually mapped area to our start area
        '_size',        # maximum size we should provide
        '_entered',     # boolean accounting context-entry/exit
    )

    def __init__(self, manager=None, regions=None):
        self._manager = manager
        self._rlist = regions
        self._region = None
        self._ofs = 0
        self._size = 0
        self._entered = False

    def __enter__(self):
        assert not self._entered
        self._entered = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        assert self._entered
        self._destroy()
        self._entered = False

    def __del__(self):
        if self.is_valid():
            log.warning("Alive %s: valid,=%s, associated=%s" %
                        (self, self.is_valid(), self.is_associated()))

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

    #{ Interface
    def use_region(self, offset=0, size=0, flags=0):
        """Assure we point to a window which allows access to the given offset into the file

        :param offset: absolute offset in bytes into the file
        :param size: amount of bytes to map. If 0, all available bytes will be mapped
        :param flags: additional flags to be given to os.open in case a file handle is initially opened
            for mapping. Has no effect if a region can actually be reused.
        :return: this instance - it should be queried for whether it points to a valid memory region.
            This is not the case if the mapping failed because we reached the end of the file

        .. Warning::
            The relying mmap-manager must have been "entered" before using this method.

        .. Note::
            The size actually mapped may be smaller than the given size. If that is the case,
            either the file has reached its end, or the map was created between two existing regions
        """
        self._manager._check_if_entered()

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

        self._ofs = offset - self._region._ofs
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
        return self._region._ofs + self._ofs

    def ofs_end(self):
        """:return: offset to one past the last available byte"""
        # unroll method calls for performance !
        return self._region._ofs + self._ofs + self._size

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
        return (self._region._ofs + self._ofs) <= ofs < (self._region._ofs + self._ofs + self._size)

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

    #} END interface


def managed_mmaps(check_entered=True):
    """Makes a memory-map context-manager instance for the correct python-version.

    :param bool check_entered:
        whether to scream if not used as context-manager (`with` block)
    :return:
        either :class:`SlidingWindowMapManager` or :class:`StaticWindowMapManager` (if PY2)

        If you want to change other default parameters of these classes, use them directly.

        .. Tip::
            Use it in a ``with ...:`` block, to free cached (and unused) resources.

    """
    mman = SlidingWindowMapManager if PY3 else StaticWindowMapManager

    return mman(check_entered=check_entered)


class StaticWindowMapManager(object):

    """Provides a manager which will produce single size cursors that are allowed
    to always map the whole file.

    Clients must be written to specifically know that they are accessing their data
    through a StaticWindowMapManager, as they otherwise have to deal with their window size.

    These clients would have to use a SlidingWindowMapBuffer to hide this fact.

    This type will always use a maximum window size, and optimize certain methods to
    accommodate this fact

    .. Tip::
        The *memory-managers* are re-entrant, but not thread-safe context-manager(s),
        to be used within a ``with ...:`` block, ensuring any left-overs cursors are cleaned up.
        If not entered, :meth:`make_cursor()` and/or :meth:`WindowCursor.use_region()` will scream.

    """

    __slots__ = [
        '_fdict',               # mapping of path -> StorageHelper (of some kind
        '_window_size',         # maximum size of a window
        '_max_memory_size',     # maximum amount of memory we may allocate
        '_max_handle_count',    # maximum amount of handles to keep open
        '_memory_size',         # currently allocated memory size
        '_handle_count',        # amount of currently allocated file handles
        '_entered',             # updated on enter/exit, when 0, `close()`
        'check_entered',        # bool, whether to scream if not used as context-manager (`with` block)
    ]

    #{ Configuration
    MapRegionListCls = MapRegionList
    MapWindowCls = MapWindow
    MapRegionCls = MapRegion
    WindowCursorCls = WindowCursor
    #} END configuration

    _MB_in_bytes = 1024 * 1024

    def __init__(self, window_size=0, max_memory_size=0, max_open_handles=sys.maxsize,
                 check_entered=True):
        """initialize the manager with the given parameters.
        :param window_size: if -1, a default window size will be chosen depending on
            the operating system's architecture. It will internally be quantified to a multiple of the page size
            If 0, the window may have any size, which basically results in mapping the whole file at one
        :param max_memory_size: maximum amount of memory we may map at once before releasing mapped regions.
            If 0, a viable default will be set depending on the system's architecture.
            It is a soft limit that is tried to be kept, but nothing bad happens if we have to over-allocate
        :param max_open_handles: if not maxint, limit the amount of open file handles to the given number.
            Otherwise the amount is only limited by the system itself. If a system or soft limit is hit,
            the manager will free as many handles as possible
        :param bool check_entered: whether to scream if not used as context-manager (`with` block)
        """
        self._fdict = dict()
        self._window_size = window_size
        self._max_memory_size = max_memory_size
        self._max_handle_count = max_open_handles
        self._memory_size = 0
        self._handle_count = 0
        self._entered = 0
        self.check_entered = check_entered

        if window_size < 0:
            coeff = 64
            if is_64_bit():
                coeff = 1024
            # END handle arch
            self._window_size = coeff * self._MB_in_bytes
        # END handle max window size

        if max_memory_size == 0:
            coeff = 1024
            if is_64_bit():
                coeff = 8192
            # END handle arch
            self._max_memory_size = coeff * self._MB_in_bytes
        # END handle max memory size

    def __enter__(self):
        assert self._entered >= 0, self._entered
        self._entered += 1

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        assert self._entered > 0, self._entered
        self._entered -= 1
        if self._entered == 0:
            # Try to close all file-handles
            #(a *Windows* only issue, and probably not fixed)
            gc.collect()
            leaft_overs = self.collect()
            if leaft_overs:
                log.debug("Cleaned up %s left-over mmap-regions." % leaft_overs)

    def __del__(self):
        if self._entered != 0:
            log.warning("Missed %s exit(s) on %s!" % (self._entered, self))
            self.close()

    def close(self):
        self.collect()

    #{ Internal Methods

    def _purge_lru_regions(self, size):
        """Unmap least-recently-used regions that have no client

        :param int size:
            size of the region we want to map next (assuming its not already mapped partially or full
            if 0, we try to free any available region
        :return:
            Amount of freed regions

        .. Note::
            We don't raise exceptions anymore, in order to keep the system working, allowing temporary overallocation.
            If the system runs out of memory, it will tell.

        .. TODO::
            Implement a case where all unusued regions are discarded efficiently.
            Currently its only brute force.
        """
        ## Collect all candidate (rlist, region) pairs, that is,
        #  those with a single client, us.
        #
        region_pairs = [(rlist, r)
                        for _, rlist in self._fdict.items()
                        for r in rlist
                        if r.client_count() <= 1]  # The `<` never matches.

        ## Purge the first candidates until enough memory freed.
        #
        mem_limit = (size and self._max_memory_size - size) or 0  # or kicks in when `size == 0`.
        num_found = 0
        for rlist, region in region_pairs:
            assert self._memory_size >= 0, self._memory_size

            if self._memory_size <= mem_limit:
                break

            num_found += 1
            self._memory_size -= region.size()
            rlist.remove(region)
            region.increment_client_count(-1)
            self._handle_count -= 1
            assert self._handle_count >= 0, self._handle_count

        return num_found

    def _obtain_region(self, a, offset, size, flags, is_recursive):
        """Utilty to create a new region - for more information on the parameters,
        see MapCursor.use_region.
        :param a: A regions (a)rray
        :return: The newly created region"""
        if self._memory_size + size > self._max_memory_size:
            self._purge_lru_regions(size)
        # END handle collection

        r = None
        if a:
            assert len(a) == 1
            r = a[0]
        else:
            try:
                r = self.MapRegionCls(a.path_or_fd(), 0, sys.maxsize, flags)
            except Exception:
                # apparently we are out of system resources or hit a limit
                # As many more operations are likely to fail in that condition (
                # like reading a file from disk, etc) we free up as much as possible
                # As this invalidates our insert position, we have to recurse here
                if is_recursive:
                    # we already tried this, and still have no success in obtaining
                    # a mapping. This is an exception, so we propagate it
                    raise
                # END handle existing recursion
                self._purge_lru_regions(0)
                return self._obtain_region(a, offset, size, flags, True)
            # END handle exceptions

            self._handle_count += 1
            self._memory_size += r.size()
            a.append(r)
        # END handle array

        assert r.includes_ofs(offset)
        return r

    def _check_if_entered(self):
        if self.check_entered and self._entered <= 0:
            raise ValueError('Context-manager %s not entered!' % self)

    #}END internal methods

    #{ Interface
    def make_cursor(self, path_or_fd):
        """
        :return: a cursor pointing to the given path or file descriptor.
            It can be used to map new regions of the file into memory

        **Note:** if a file descriptor is given, it is assumed to be open and valid,
        but may be closed afterwards. To refer to the same file, you may reuse
        your existing file descriptor, but keep in mind that new windows can only
        be mapped as long as it stays valid. This is why the using actual file paths
        are preferred unless you plan to keep the file descriptor open.

        **Note:** file descriptors are problematic as they are not necessarily unique, as two
        different files opened and closed in succession might have the same file descriptor id.

        **Note:** Using file descriptors directly is faster once new windows are mapped as it
        prevents the file to be opened again just for the purpose of mapping it."""
        self._check_if_entered()

        regions = self._fdict.get(path_or_fd)
        if regions:
            assert not regions.collect_closed_regions(), regions.collect_closed_regions()
        else:
            regions = self.MapRegionListCls(path_or_fd)
            self._fdict[path_or_fd] = regions
        # END obtain region for path
        return self.WindowCursorCls(self, regions)

    def collect(self):
        """Collect all available free-to-collect mapped regions
        :return: Amount of freed handles"""
        return self._purge_lru_regions(0)

    def num_file_handles(self):
        """:return: amount of file handles in use. Each mapped region uses one file handle"""
        return self._handle_count

    def num_open_files(self):
        """Amount of opened files in the system"""
        return reduce(lambda x, y: x + y, (1 for rlist in self._fdict.values() if len(rlist) > 0), 0)

    def window_size(self):
        """:return: size of each window when allocating new regions"""
        return self._window_size

    def mapped_memory_size(self):
        """:return: amount of bytes currently mapped in total"""
        return self._memory_size

    def max_file_handles(self):
        """:return: maximium amount of handles we may have opened"""
        return self._max_handle_count

    def max_mapped_memory_size(self):
        """:return: maximum amount of memory we may allocate"""
        return self._max_memory_size

    #} END interface

    #{ Special Purpose Interface

    def force_map_handle_removal_win(self, base_path):
        """ONLY AVAILABLE ON WINDOWS
        On windows removing files is not allowed if anybody still has it opened.
        If this process is ourselves, and if the whole process uses this memory
        manager (as far as the parent framework is concerned) we can enforce
        closing all memory maps whose path matches the given base path to
        allow the respective operation after all.
        The respective system must NOT access the closed memory regions anymore !
        This really may only be used if you know that the items which keep
        the cursors alive will not be using it anymore. They need to be recreated !
        :return: Amount of closed handles

        **Note:** does nothing on non-windows platforms"""
        if sys.platform != 'win32':
            return
        # END early bailout

        num_closed = 0
        for path, rlist in self._fdict.items():
            if path.startswith(base_path):
                for region in rlist:
                    region.release()
                    num_closed += 1
            # END path matches
        # END for each path
        return num_closed
    #} END special purpose interface


class SlidingWindowMapManager(StaticWindowMapManager):

    """Maintains a list of ranges of mapped memory regions in one or more files and allows to easily
    obtain additional regions assuring there is no overlap.
    Once a certain memory limit is reached globally, or if there cannot be more open file handles
    which result from each mmap call, the least recently used, and currently unused mapped regions
    are unloaded automatically.

    **Note:** currently not thread-safe !

    **Note:** in the current implementation, we will automatically unload windows if we either cannot
        create more memory maps (as the open file handles limit is hit) or if we have allocated more than
        a safe amount of memory already, which would possibly cause memory allocations to fail as our address
        space is full."""

    __slots__ = ()

    def __init__(self, window_size=-1, max_memory_size=0, max_open_handles=sys.maxsize,
                 check_entered=True):
        """Adjusts the default window size to -1"""
        super(SlidingWindowMapManager, self).__init__(window_size, max_memory_size, max_open_handles, check_entered)

    def _obtain_region(self, a, offset, size, flags, is_recursive):
        # bisect to find an existing region. The c++ implementation cannot
        # do that as it uses a linked list for regions.
        r = None
        lo = 0
        hi = len(a)
        while lo < hi:
            mid = (lo + hi) // 2
            ofs = a[mid]._ofs
            if ofs <= offset:
                if a[mid].includes_ofs(offset):
                    r = a[mid]
                    break
                # END have region
                lo = mid + 1
            else:
                hi = mid
            # END handle position
        # END while bisecting

        if r is None:
            window_size = self._window_size
            left = self.MapWindowCls(0, 0)
            mid = self.MapWindowCls(offset, size)
            right = self.MapWindowCls(a.file_size(), 0)

            # we want to honor the max memory size, and assure we have anough
            # memory available
            # Save calls !
            if self._memory_size + window_size > self._max_memory_size:
                self._purge_lru_regions(window_size)
            # END handle collection

            # we assume the list remains sorted by offset
            insert_pos = 0
            len_regions = len(a)
            if len_regions == 1:
                if a[0]._ofs <= offset:
                    insert_pos = 1
                # END maintain sort
            else:
                # find insert position
                insert_pos = len_regions
                for i, region in enumerate(a):
                    if region._ofs > offset:
                        insert_pos = i
                        break
                    # END if insert position is correct
                # END for each region
            # END obtain insert pos

            # adjust the actual offset and size values to create the largest
            # possible mapping
            if insert_pos == 0:
                if len_regions:
                    right = self.MapWindowCls.from_region(a[insert_pos])
                # END adjust right side
            else:
                if insert_pos != len_regions:
                    right = self.MapWindowCls.from_region(a[insert_pos])
                # END adjust right window
                left = self.MapWindowCls.from_region(a[insert_pos - 1])
            # END adjust surrounding windows

            mid.extend_left_to(left, window_size)
            mid.extend_right_to(right, window_size)
            mid.align()

            # it can happen that we align beyond the end of the file
            if mid.ofs_end() > right.ofs:
                mid.size = right.ofs - mid.ofs
            # END readjust size

            # insert new region at the right offset to keep the order
            try:
                if self._handle_count >= self._max_handle_count:
                    raise Exception
                # END assert own imposed max file handles
                r = self.MapRegionCls(a.path_or_fd(), mid.ofs, mid.size, flags)
            except Exception:
                # apparently we are out of system resources or hit a limit
                # As many more operations are likely to fail in that condition (
                # like reading a file from disk, etc) we free up as much as possible
                # As this invalidates our insert position, we have to recurse here
                if is_recursive:
                    # we already tried this, and still have no success in obtaining
                    # a mapping. This is an exception, so we propagate it
                    raise
                # END handle existing recursion
                self._purge_lru_regions(0)
                return self._obtain_region(a, offset, size, flags, True)
            # END handle exceptions

            self._handle_count += 1
            self._memory_size += r.size()
            a.insert(insert_pos, r)
        # END create new region
        return r

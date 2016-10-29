"""Module containing a memory memory manager which provides a sliding window on a number of memory mapped files."""
from functools import reduce
import logging
import sys

from .mwindow import (
    WindowCursor,
    MapRegion,
    MapRegionList,
)
from .util import (
    is_64_bit,
    suppress,
)


__all__ = ["StaticWindowMapManager", "SlidingWindowMapManager",
           "ALLOCATIONGRANULARITY", "align_to_mmap", "_MapWindow"]

log = logging.getLogger(__name__)


try:
    from mmap import ALLOCATIONGRANULARITY
except ImportError:
    # in python pre 2.6, the ALLOCATIONGRANULARITY does not exist as it is mainly
    # useful for aligning the offset. The offset argument doesn't exist there though
    from mmap import PAGESIZE as ALLOCATIONGRANULARITY
# END handle pythons missing quality assurance


def align_to_mmap(num, round_up):
    """
    Align the given integer number to the closest page offset, which usually is 4096 bytes.

    :param round_up: if True, the next higher multiple of page size is used, otherwise
        the lower page_size will be used (i.e. if True, 1 becomes 4096, otherwise it becomes 0)
    :return: num rounded to closest page"""
    res = (num // ALLOCATIONGRANULARITY) * ALLOCATIONGRANULARITY
    if round_up and (res != num):
        res += ALLOCATIONGRANULARITY
    # END handle size
    return res


class _MapWindow(object):

    """Utility type which is used to snap windows towards each other, and to adjust their size"""
    __slots__ = (
        'ofs',      # offset into the file in bytes
        'size'              # size of the window in bytes
    )

    def __init__(self, offset, size):
        self.ofs = offset
        self.size = size

    def __repr__(self):
        return "_MapWindow(%i, %i)" % (self.ofs, self.size)

    @classmethod
    def from_region(cls, region):
        """:return: new window from a region"""
        return cls(region._b, region.size())

    def ofs_end(self):
        return self.ofs + self.size

    def align(self):
        """Assures the previous window area is contained in the new one"""
        nofs = align_to_mmap(self.ofs, 0)
        self.size += self.ofs - nofs    # keep size constant
        self.ofs = nofs
        self.size = align_to_mmap(self.size, 1)

    def extend_left_to(self, window, max_size):
        """Adjust the offset to start where the given window on our left ends if possible,
        but don't make yourself larger than max_size.
        The resize will assure that the new window still contains the old window area"""
        rofs = self.ofs - window.ofs_end()
        nsize = rofs + self.size
        rofs -= nsize - min(nsize, max_size)
        self.ofs = self.ofs - rofs
        self.size += rofs

    def extend_right_to(self, window, max_size):
        """Adjust the size to make our window end where the right window begins, but don't
        get larger than max_size"""
        self.size = min(self.size + (window.ofs - self.ofs_end()), max_size)


class StaticWindowMapManager(object):

    """Provides a manager which will produce single size cursors that are allowed
    to always map the whole file.

    Clients must be written to specifically know that they are accessing their data
    through a StaticWindowMapManager, as they otherwise have to deal with their window size.
    These clients would have to use a SlidingWindowMapBuffer to hide this fact.

    This type will always use a maximum window size, and optimize certain methods to
    accommodate this fact

    .. Tip::
        This is can be used optionally as a non-reetrant context-manager
        inside a ``with ...:`` block.  Any errors on :meth:`close()` will be reported
        as warnings.
    """

    __slots__ = [
        '_fdict',           # mapping of path -> StorageHelper (of some kind
        '_window_size',     # maximum size of a window
        '_max_memory_size',  # maximum amount of memory we may allocate
        '_max_handle_count',        # maximum amount of handles to keep open
        '_memory_size',     # currently allocated memory size
        '_handle_count',        # amount of currently allocated file handles
    ]

    #{ Configuration
    MapRegionListCls = MapRegionList
    _MapWindowCls = _MapWindow
    MapRegionCls = MapRegion
    WindowCursorCls = WindowCursor
    #} END configuration

    _MB_in_bytes = 1024 * 1024

    def __init__(self, window_size=0, max_memory_size=0, max_open_handles=sys.maxsize):
        """initialize the manager with the given parameters.
        :param window_size: if -1, a default window size will be chosen depending on
            the operating system's architecture. It will internally be quantified to a multiple of the page size
            If 0, the window may have any size, which basically results in mapping the whole file at one
        :param max_memory_size: maximum amount of memory we may map at once before releasing mapped regions.
            If 0, a viable default will be set depending on the system's architecture.
            It is a soft limit that is tried to be kept, but nothing bad happens if we have to over-allocate
        :param max_open_handles: if not maxint, limit the amount of open file handles to the given number.
            Otherwise the amount is only limited by the system itself. If a system or soft limit is hit,
            the manager will free as many handles as possible"""
        self._fdict = {}
        self._window_size = window_size
        self._max_memory_size = max_memory_size
        self._max_handle_count = max_open_handles
        self._memory_size = 0
        self._handle_count = 0

        if window_size < 0:
            coeff = 64
            if is_64_bit:
                coeff = 1024
            # END handle arch
            self._window_size = coeff * self._MB_in_bytes
        # END handle max window size

        if max_memory_size == 0:
            coeff = 1024
            if is_64_bit:
                coeff = 8192
            # END handle arch
            self._max_memory_size = coeff * self._MB_in_bytes
        # END handle max memory size

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

    #}END internal methods

    #{ Interface
    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with suppress(Exception):
            self.close()

    def close(self):
        """Close all regions and relying memmaps.

        :raise: any error while closing

        .. Note::
            Not to fail on *Windows*, all files referencing mmaps must have been closed.
            If used as a context manager, any errors are suppressed.
        """
        n_active_regions = n_clients = 0
        regions = (r for rlist in self._fdict.values() for r in rlist)
        for region in regions:
            cc = region.client_count()
            if cc > 1:
                n_active_regions += 1
                n_clients += cc - 1  # discount our reference
            region.release(skip_client_count_check=True)
        self._fdict = None  # make this instance unsuable
        if n_active_regions:
            raise ValueError('Mem-man %s closed with %s active-regions, held by %s clients.' %
                             (self, n_active_regions, n_clients))

        self._fdict = None

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
        rlist = self._fdict.get(path_or_fd)
        if rlist is None:
            rlist = self.MapRegionListCls(path_or_fd)
            self._fdict[path_or_fd] = rlist
        # END obtain region for path
        return self.WindowCursorCls(self, rlist)

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

    def __init__(self, window_size=-1, max_memory_size=0, max_open_handles=sys.maxsize):
        """Adjusts the default window size to -1"""
        super(SlidingWindowMapManager, self).__init__(window_size, max_memory_size, max_open_handles)

    def _obtain_region(self, a, offset, size, flags, is_recursive):
        # bisect to find an existing region. The c++ implementation cannot
        # do that as it uses a linked list for regions.
        r = None
        lo = 0
        hi = len(a)
        while lo < hi:
            mid = (lo + hi) // 2
            ofs = a[mid]._b
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
            left = self._MapWindowCls(0, 0)
            mid = self._MapWindowCls(offset, size)
            right = self._MapWindowCls(a.file_size(), 0)

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
                if a[0]._b <= offset:
                    insert_pos = 1
                # END maintain sort
            else:
                # find insert position
                insert_pos = len_regions
                for i, region in enumerate(a):
                    if region._b > offset:
                        insert_pos = i
                        break
                    # END if insert position is correct
                # END for each region
            # END obtain insert pos

            # adjust the actual offset and size values to create the largest
            # possible mapping
            if insert_pos == 0:
                if len_regions:
                    right = self._MapWindowCls.from_region(a[insert_pos])
                # END adjust right side
            else:
                if insert_pos != len_regions:
                    right = self._MapWindowCls.from_region(a[insert_pos])
                # END adjust right window
                left = self._MapWindowCls.from_region(a[insert_pos - 1])
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

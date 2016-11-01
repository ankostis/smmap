"""Module containing a memory memory manager which provides a sliding window on a number of memory mapped files."""
from collections import OrderedDict
import logging
import os
import sys

from smmap.util import string_types, Relation, PY3

from .mwindow import (
    WindowCursor,
    MapRegion,
)
from .util import (
    is_64_bit,
    suppress,
)


__all__ = ['managed_mmaps', "StaticWindowMapManager", "SlidingWindowMapManager",
           "ALLOCATIONGRANULARITY", "align_to_mmap"]

log = logging.getLogger(__name__)


try:
    from mmap import ALLOCATIONGRANULARITY
except ImportError:
    # in python pre 2.6, the ALLOCATIONGRANULARITY does not exist as it is mainly
    # useful for aligning the offset. The offset argument doesn't exist there though
    from mmap import PAGESIZE as ALLOCATIONGRANULARITY
# END handle pythons missing quality assurance


def managed_mmaps(window_size, max_memory_size, max_open_handles):
    """Makes a memory-map context-manager instance for the correct python-version.

    :return:
        either :class:`SlidingWindowMapManager` or :class:`StaticWindowMapManager` (if PY2).

        If you want to change other default parameters of these classes, use them directly.

        .. Tip::
            The *memory-managers* are (optionally) re-entrant, but not thread-safe, context-manager(s),
            to be used within a ``with ...:`` block, ensuring any left-overs cursors are cleaned up.
            If not entered, :meth:`make_cursor()` will scream.

            You may use :class:`contextlib.ExitStack()` to store them for longer-term lifetime.
    """
    mman = SlidingWindowMapManager if PY3 else StaticWindowMapManager

    return mman(**locals())


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
        return cls(region.ofs, region.size)

    @property
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
        rofs = self.ofs - window.ofs_end
        nsize = rofs + self.size
        rofs -= nsize - min(nsize, max_size)
        self.ofs = self.ofs - rofs
        self.size += rofs

    def extend_right_to(self, window, max_size):
        """Adjust the size to make our window end where the right window begins, but don't
        get larger than max_size"""
        self.size = min(self.size + (window.ofs - self.ofs_end), max_size)


class _RegionList(list):

    """List of MapRegion instances associating a path with a list of regions."""
    __slots__ = (
        'path_or_fd',  # path or file descriptor which is mapped by all our regions
        '_file_size'    # total size of the file we map
    )

    def __new__(cls, path):
        return super(_RegionList, cls).__new__(cls)

    def __init__(self, path_or_fd):
        self.path_or_fd = path_or_fd
        self._file_size = None

    def file_size(self):
        """:return: size of file we manager"""
        if self._file_size is None:
            if isinstance(self.path_or_fd, string_types):
                self._file_size = os.stat(self.path_or_fd).st_size
            else:
                self._file_size = os.fstat(self.path_or_fd).st_size
            # END handle path type
        # END update file size
        return self._file_size


_MB_in_bytes = 1024 * 1024


class StaticWindowMapManager(object):

    """Provides a manager which will produce single size cursors that are allowed
    to always map the whole file.

    Clients must be written to specifically know that they are accessing their data
    through a StaticWindowMapManager, as they otherwise have to deal with their window size.
    These clients would have to use a SlidingWindowMapBuffer to hide this fact.

    This type will always use a maximum window size, and optimize certain methods to
    accommodate this fact

    .. Tip::
        This is can be used optionally as a non-reetrant reusable context-manager
        inside a ``with ...:`` block, to enusre eny resources are cleared.
        Any errors on :meth:`close()` will be reported as warnings.
    """

    __slots__ = [
        '_ix_path_rlist',       # 1-1 registry of {path   <-> rlist[regions]}
        '_ix_cur_reg',          # N-1 registry of {cursor --> region}
        '_ix_reg_lru',          # Just for implementing LRU purge f regions
        '_window_size',     # maximum size of a window
        'max_memory_size',  # maximum amount of memory we may allocate
        'max_handle_count',        # maximum amount of handles to keep open
    ]

    #{ Configuration
    _RegionListCls = _RegionList
    _MapWindowCls = _MapWindow
    MapRegionCls = MapRegion
    WindowCursorCls = WindowCursor
    #} END configuration

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
        self._ix_path_rlist = Relation(one2one=1, kname='PATH_OR_FD', vname='RLIST')
        self._ix_cur_reg = Relation(kname='CURSOR', vname='REGION')
        self._ix_reg_lru = OrderedDict()
        self.max_handle_count = max_open_handles

        if window_size < 0:
            coeff = 64
            if is_64_bit:
                coeff = 1024
            # END handle arch
            window_size = coeff * _MB_in_bytes
        self._window_size = window_size

        if max_memory_size == 0:
            coeff = 1024
            if is_64_bit:
                coeff = 8192
            # END handle arch
            max_memory_size = coeff * _MB_in_bytes
        self.max_memory_size = max_memory_size

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
                        for _, rlist in self._ix_path_rlist.items()
                        for r in rlist
                        if not self.is_region_used(r)]

        ## Purge the first candidates until enough memory freed.
        #
        mem_limit = (size and self.max_memory_size - size) or 0  # or kicks in when `size == 0`.
        num_found = 0
        for rlist, region in region_pairs:
            assert self.mapped_memory_size >= 0, self.mapped_memory_size

            if self.mapped_memory_size < mem_limit and self.num_open_regions < self.max_handle_count:
                break

            self._release_region(region)
            num_found += 1

        return num_found

    def _obtain_region(self, rlist, offset, size, flags, is_recursive):
        """Create rlist new region without registering it.

        For more information on the parameters, see :meth:`make_cursor()`.

        :param rlist: A regions (rlist)rray
        :return: The newly created region"""
        if (self.mapped_memory_size + size > self.max_memory_size or
                self.num_open_regions >= self.max_handle_count):
            self._purge_lru_regions(size)
        # END handle collection

        r = None
        if rlist:
            assert len(rlist) == 1
            r = rlist[0]
        else:
            try:
                r = self.MapRegionCls(self, rlist.path_or_fd, 0, sys.maxsize, flags)
            except Exception:
                ## Apparently we are out of system resources.
                #  We free up as many regions as possible and retry,
                #  unless we already did that.
                #
                if is_recursive:
                    raise
                self._purge_lru_regions(0)
                return self._obtain_region(rlist, offset, size, flags, True)
            # END handle exceptions

            rlist.append(r)
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

        n_cursors = self.num_open_cursors
        n_active_regions = self.num_open_regions
        for region in self._ix_reg_lru:
            region.release()

        self._ix_cur_reg.clear()
        self._ix_path_rlist.clear()
        self._ix_reg_lru.clear()
        if n_cursors:
            raise ValueError('%s closed with %s active-regions, held by %s cursors.' %
                             (self, n_active_regions, n_cursors))

    def rlist_for_path_or_fd(self, path_or_fd):
        return self._ix_path_rlist.get(path_or_fd)

    def region_for_cursor(self, cursor):
        return self._ix_cur_reg.get(cursor)

    def cursors_for_region(self, region):
        """:return: a tuple of all cursors bound to the region"""
        return tuple(c for c, r in self._ix_cur_reg.items() if r is region)

    def is_region_used(self, region):
        return region in set(self._ix_cur_reg.values())

    def is_cursor_valid(self, cursor):
        return cursor in self._ix_cur_reg

    def _bind_cursor(self, cursor, region):
        """fails if indexes not in perfect shape (double registrations, etc)"""
        self._ix_cur_reg.put(cursor, region)
        if region not in self._ix_reg_lru:
            self._ix_reg_lru[region] = None
        else:
            self._ix_reg_lru.move_to_end(region)  # maintain LRU

    def _release_region(self, region):
        """fails if indexes not in perfect shape"""
        region._mf.close()
        region._mf = None
        _ix_path_rlist = self._ix_path_rlist
        rlist = _ix_path_rlist[region.path_or_fd]
        rlist.remove(region)
        del self._ix_reg_lru[region]

    def _release_cursor(self, cursor):
        """(protected) fails if indexes not in perfect shape"""
        self._ix_cur_reg.take(cursor)

    def get_or_create_rlist(self, path_or_fd):
        rlist = self._ix_path_rlist.get(path_or_fd)
        if rlist is None:
            rlist = self._RegionListCls(path_or_fd)
            self._ix_path_rlist[path_or_fd] = rlist
        return rlist

    def make_cursor(self, path_or_fd, offset=0, size=0, flags=0):
        """Create a cursor to read/write using memory-mapped file

        .. Tip::
            It is recommended to close a cursor once you are done reading/writing,
            to help its referred region to get collected sooner.

            Since it is a NON re-entrant, non thread-safe, optional context-manager,
            it may be used within a ``with ...:`` block.

        :param path_or_fd:
            Either a file-path or a file descriptor assumed to be open and valid,
                that will be closed afterwards. To refer to the same file, you may reuse
                your existing file descriptor, but keep in mind that new windows can only
                be mapped as long as it stays valid. This is why using actual file paths
                are preferred unless you plan to keep the file descriptor open.

            .. Note::
                File descriptors are problematic as they are not necessarily unique, as two
                different files opened and closed in succession might have the same file descriptor id.

                But using file descriptors directly is faster once new windows are mapped,
                as it prevents the file to be opened again just for the purpose of mapping it.

        :param offset:
            absolute offset in bytes into the file
        :param size:
            amount of bytes to map. If 0, all available bytes will be mapped

            .. Note::
                The actually size may be smaller than requested, either because
                the file-size is smaller, or the map was created between two existing regions.

        :param flags:
            additional flags for ``os.open()`` in case there is no region open for this file.
            Has no effect in case an existing region gets reused.
        :return:
            a :class:`WindowCursor` pointing to the given path or file descriptor,
            or fails if offset was beyond the end of the file
            """
        rlist = self.get_or_create_rlist(path_or_fd)

        fsize = rlist.file_size()
        size = min(size or fsize, self._window_size or fsize)   # clamp size to window size

        if offset >= fsize:
            raise ValueError("Offset(%s) beyond file-size(%s) for file: %r"
                             % (offset, fsize, path_or_fd))

        region = self._obtain_region(rlist, offset, size, flags, False)

        size = min(size, region.ofs_end - offset)
        cursor = self.WindowCursorCls(self, path_or_fd, offset, size)

        ## Register here so cursor cannot not re-validate itself.
        self._bind_cursor(cursor, region)

        return cursor

    def collect(self):
        """Collect all available free-to-collect mapped regions
        :return: Amount of freed handles"""
        return self._purge_lru_regions(0)

    @property
    def mapped_memory_size(self):
        return sum(r.size for r in self._ix_reg_lru)

    @property
    def num_open_regions(self):
        """:return: amount of open regions (used or unused); correspond to open mmaps.

        The invariant `num_open_regions = num_used_regions + num_unused_regions` applies.
        """
        return len(self._ix_reg_lru)

    @property
    def num_used_regions(self):
        """:return: the number of regions bound to a cursor

        The invariant `num_open_regions = num_used_regions + num_unused_regions` applies.

        The unused regions still waste resources, and may be collected by :meth:`collect()`.
        """
        return len(set(self._ix_cur_reg.values()))

    @property
    def num_open_cursors(self):
        return len(self._ix_cur_reg)

    def window_size(self):
        """:return: size of each window when allocating new regions"""
        return self._window_size

    def max_file_handles(self):
        """:return: maximium amount of handles we may have opened"""
        return self.max_handle_count

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
        for path, rlist in self._ix_path_rlist.items():
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

    def _obtain_region(self, rlist, offset, size, flags, is_recursive):
        # bisect to find an existing region. The c++ implementation cannot
        # do that as it uses rlist linked list for regions.
        r = None
        lo = 0
        hi = len(rlist)
        while lo < hi:
            mid = (lo + hi) // 2
            ofs = rlist[mid].ofs
            if ofs <= offset:
                if rlist[mid].includes_ofs(offset):
                    r = rlist[mid]
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
            right = self._MapWindowCls(rlist.file_size(), 0)

            # we want to honor the max memory size, and assure we have anough
            # memory available
            # Save calls !
            if (self.mapped_memory_size + window_size > self.max_memory_size or
                    self.num_open_regions >= self.max_handle_count):
                self._purge_lru_regions(window_size)
            # END handle collection

            # we assume the list remains sorted by offset
            insert_pos = 0
            len_regions = len(rlist)
            if len_regions == 1:
                if rlist[0].ofs <= offset:
                    insert_pos = 1
                # END maintain sort
            else:
                # find insert position
                insert_pos = len_regions
                for i, region in enumerate(rlist):
                    if region.ofs > offset:
                        insert_pos = i
                        break
                    # END if insert position is correct
                # END for each region
            # END obtain insert pos

            # adjust the actual offset and size values to create the largest
            # possible mapping
            if insert_pos == 0:
                if len_regions:
                    right = self._MapWindowCls.from_region(rlist[insert_pos])
                # END adjust right side
            else:
                if insert_pos != len_regions:
                    right = self._MapWindowCls.from_region(rlist[insert_pos])
                # END adjust right window
                left = self._MapWindowCls.from_region(rlist[insert_pos - 1])
            # END adjust surrounding windows

            mid.extend_left_to(left, window_size)
            mid.extend_right_to(right, window_size)
            mid.align()

            # it can happen that we align beyond the end of the file
            if mid.ofs_end > right.ofs:
                mid.size = right.ofs - mid.ofs
            # END readjust size

            # insert new region at the right offset to keep the order
            try:
                r = self.MapRegionCls(self, rlist.path_or_fd, mid.ofs, mid.size, flags)
            except Exception:
                ## Apparently we are out of system resources.
                #  We free up as many regions as possible and retry,
                #  unless we already did that.
                #
                if is_recursive:
                    raise
                self._purge_lru_regions(0)
                return self._obtain_region(rlist, offset, size, flags, True)
            # END handle exceptions

            rlist.insert(insert_pos, r)
        # END create new region
        return r

"""Module containing a memory memory manager which provides a sliding window on a number of memory mapped files."""
from collections import namedtuple
import logging
import mmap
import os
import sys

from smmap.util import string_types, Relation, PY3, finalize

from .mwindow import (
    FixedWindowCursor,
    MapRegion,
)
from .util import (
    is_64_bit,
    suppress,
    ExitStack,
)


__all__ = ['managed_mmaps', "GreedyMemmapManager", "TilingMemmapManager",
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
        either :class:`TilingMemmapManager` or :class:`GreedyMemmapManager` (if PY2).

        If you want to change other default parameters of these classes, use them directly.

        .. Tip::
            The *memory-managers* are (optionally) re-entrant, but not thread-safe, context-manager(s),
            to be used within a ``with ...:`` block, ensuring any left-overs cursors are cleaned up.
            If not entered, :meth:`make_cursor()` will scream.

            You may use :class:`contextlib.ExitStack()` to store them for longer-term lifetime.
    """
    mman = TilingMemmapManager if PY3 else GreedyMemmapManager

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


class FileInfo(namedtuple('FileInfo', 'path_or_fd file_size')):

    """Holds file attributes such as path-or-fd, size."""

    def __new__(cls, path_or_fd):
        if isinstance(path_or_fd, string_types):
            file_size = os.stat(path_or_fd).st_size
        else:
            file_size = os.fstat(path_or_fd).st_size
        return super(FileInfo, cls).__new__(cls, path_or_fd, file_size)

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


_MB_in_bytes = 1024 * 1024


class MemmapManagerError(Exception):
    """Exceptions related to release of resources by memory-manager"""


class MemmapManager(object):

    """
    .. Tip::
        This is can be used optionally as a non-reetrant reusable context-manager
        inside a ``with ...:`` block, to enusre eny resources are cleared.
        Any errors on :meth:`close()` will be reported as warnings.
    """

    __slots__ = (
        '_ix_path_finfo',       # 1-1 registry of {path   <-> finfo}
        '_ix_reg_mmap',         # 1-1 registry of {region <-> mmap} (LRU!)
        '_ix_cur_reg',          # N-1 registry of {cursor --> region}
        'window_size',         # maximum size of a window
        'max_memory_size',      # maximum amount of memory we may allocate
        'max_regions_count',     # maximum amount of handles to keep open
        '_finalize',            # To replace __del_
        '__weakref__',          # To replace __del_
    )

    #{ Configuration
    _MapWindowCls = _MapWindow
    MapRegionCls = MapRegion
    WindowCursorCls = FixedWindowCursor
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
        self._ix_path_finfo = Relation(kname='PATH_OR_FD', vname='RINFO',
                                       one2one=1,
                                       on_errors=self._wrap_index_ex)
        self._ix_cur_reg = Relation(kname='CURSOR', vname='REGION',
                                    on_errors=self._wrap_index_ex)
        self._ix_reg_mmap = Relation(kname='REGION', vname='MMAP',
                                     one2one=1,
                                     on_errors=self._wrap_index_ex)
        self.max_regions_count = max_open_handles

        if window_size < 0:
            coeff = 64
            if is_64_bit:
                coeff = 1024
            # END handle arch
            window_size = coeff * _MB_in_bytes
        self.window_size = window_size

        if max_memory_size == 0:
            coeff = 1024
            if is_64_bit:
                coeff = 8192
            # END handle arch
            max_memory_size = coeff * _MB_in_bytes
        self.max_memory_size = max_memory_size
        self._finalize = finalize(self, self.close)

    #{ Internal Methods

    def _wrap_index_ex(self, rel, action, key, val, ex):
        raise MemmapManagerError(*ex.args)

    def _make_region(self, finfo, ofs=0, size=0, flags=0):
        # type: (List[MapRegion], int, int, int, int) -> MapRegion
        """
        Creates and wraps the actual mmap in a region according to the given boundaries.

        :param flags:
            additional flags to be given when opening the file.
        :raise Exception:
            if no memory can be allocated

        .. Warning::
            In case of error (i.e. not enough memory) and an open fd was passed in,
            the client is responsible to close it!
        """
        path_or_fd = finfo.path_or_fd
        is_file_open = isinstance(path_or_fd, int)
        if is_file_open:
            fd = path_or_fd
        else:
            fd = os.open(path_or_fd, os.O_RDONLY | getattr(os, 'O_BINARY', 0) | flags)

        try:
            requested_size = min(os.fstat(fd).st_size - ofs, size)
            memmap = mmap.mmap(fd, requested_size, access=mmap.ACCESS_READ, offset=ofs)
            ok = False
            try:
                actual_size = len(memmap)
                region = self.MapRegionCls(self, finfo, ofs, actual_size)
                self._ix_reg_mmap.put(region, memmap)
                ok = True
            finally:
                if not ok:
                    memmap.close()
        finally:
            ## Only close it if we opened it.
            #
            if not is_file_open:
                os.close(fd)

            return region

    def _purge_lru_regions(self, size_to_free):
        """Release LRU regions with no clients to satisfy memory/mmep-handle criteria.

        :param int size_to_free:
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
        ## Traverse the LRU-sorted `_ix_reg_mmap` and purge regions with no clients.
        #  until enough memory/mmaps are free.
        #
        mem_limit = (size_to_free and self.max_memory_size - size_to_free) or 0  # or kicks in when `size == 0`.
        used_regions = set(self._ix_cur_reg.values())
        num_found = 0
        for region in list(self._ix_reg_mmap):  # copy to modify
            if region in used_regions:
                continue

            assert self.mapped_memory_size >= 0, self.mapped_memory_size
            if self.mapped_memory_size < mem_limit and self.num_open_regions < self.max_regions_count:
                break

            self._release_region(region)
            num_found += 1

        return num_found

    #}END internal methods

    #{ Interface
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
        if self.closed:
            return
        n_cursors = self.num_open_cursors
        n_active_regions = self.num_open_regions
        mmap_errors = []
        for memmap in self._ix_reg_mmap.values():
            try:
                memmap.close()
            except Exception as ex:
                mmap_errors.append(ex)

        ## Mark this instances as "closed".
        self._ix_reg_mmap = self._ix_path_finfo = self._ix_cur_reg = None

        ## Now report errors encountered.
        #
        if n_cursors or mmap_errors:
            if mmap_errors:
                mmap_msg = ", and %s MMap closing-failures: \n  %s" % (
                    len(mmap_errors), '\n  '.join(str(e) for e in mmap_errors))
            else:
                mmap_msg = ''

            msg = "Closed %s with %s active-Regions, held by %s Cursors%s!" % (
                self, n_active_regions, n_cursors, mmap_msg)

            raise ValueError(msg)

    @property
    def closed(self):
        return self._ix_reg_mmap is None

    def regions_for_finfo(self, finfo):
        return [r for r in self._ix_reg_mmap if r.finfo is finfo]

    def regions_for_path(self, path_or_fd):
        return [r for r in self._ix_reg_mmap if r.finfo.path_or_fd == path_or_fd]

    def mmap_for_region(self, region):
        return self._ix_reg_mmap.get(region)

    def region_for_cursor(self, cursor):
        return self._ix_cur_reg.get(cursor)

    def cursors_for_region(self, region):
        """:return: a tuple of all cursors bound to the region"""
        return tuple(c for c, r in self._ix_cur_reg.items() if r is region)

    def is_region_closed(self, region):
        return region not in self._ix_reg_mmap

    def is_region_used(self, region):
        return region in set(self._ix_cur_reg.values())

    def is_cursor_closed(self, cursor):
        return cursor not in self._ix_cur_reg

    def _bind_cursor(self, cursor, region):
        """fails if indexes not in perfect shape (double registrations, etc)"""
        self._ix_cur_reg.put(cursor, region)
        self._ix_reg_mmap.hit(region)  # maintain LRU

    def _release_region(self, region):
        """fails if indexes not in perfect shape, but keeps them intact in that case, to retry"""
        if self.closed:
            return

        finfo = region.finfo

        with ExitStack() as exs:
            _ix_path_finfo = exs.enter_context(self._ix_path_finfo)
            _ix_reg_mmap = exs.enter_context(self._ix_reg_mmap)

            memmap = _ix_reg_mmap.take(region)
            if not self.regions_for_finfo(finfo):
                _ix_path_finfo.take(finfo.path_or_fd)
            memmap.close()  # Has to be the last step because it cannot rollback.

    def _release_cursor(self, cursor):
        """(protected) fails if indexes not in perfect shape"""
        if not self.closed:
            self._ix_cur_reg.take(cursor)
            #
            # Note: we do not release cursor's region if it's "unused",
            #  but leave it "cached", until resources become scarce,
            ## in which case, the `_purge_lru_regions()` cleans them up.

    def _get_or_create_finfo(self, path_or_fd):
        finfo = self._ix_path_finfo.get(path_or_fd)
        if finfo is None:
            finfo = FileInfo(path_or_fd)
            self._ix_path_finfo[path_or_fd] = finfo
        return finfo

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
            a :class:`FixedWindowCursor` pointing to the given path or file descriptor,
            or fails if offset was beyond the end of the file
            """
        finfo = self._get_or_create_finfo(path_or_fd)
        region = self._obtain_region(finfo, offset, size, flags, False)
        size = min(size or finfo.file_size, region.ofs_end - offset)
        cursor = self.WindowCursorCls(self, finfo, offset, size)

        ## Register here so cursor cannot not re-validate itself.
        self._bind_cursor(cursor, region)

        return cursor

    def collect(self):
        """Collect all available free-to-collect mapped regions
        :return: Amount of freed handles"""
        return self._purge_lru_regions(0)

    @property
    def mapped_memory_size(self):
        return sum(r.size for r in self._ix_reg_mmap)

    @property
    def num_open_regions(self):
        """:return: amount of open regions (used or unused); correspond to open mmaps.

        The invariant `num_open_regions = num_used_regions + num_unused_regions` applies.
        """
        return len(self._ix_reg_mmap)

    @property
    def num_used_regions(self):
        """:return: the number of regions bound to a cursor

        The invariant `num_open_regions = num_used_regions + num_unused_regions` applies.

        The unused regions still waste resources, and may be collected by :meth:`collect()`.
        """
        return len(set(self._ix_cur_reg.values()))

    @property
    def num_open_files(self):
        """:return: the number of files that opens regions exist for
        """
        return len(self._ix_path_finfo)

    @property
    def num_open_cursors(self):
        return len(self._ix_cur_reg)

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
        for path in list(self._ix_path_finfo):  # copy to modify it
            if not isinstance(path, int) and path.startswith(base_path):
                for region in self.regions_for_path(path):
                    region.release()
                    num_closed += 1
            # END path matches
        # END for each path
        return num_closed
    #} END special purpose interface


class GreedyMemmapManager(MemmapManager):
    """
    A manager producing cursors that always map the whole file.

    Clients must be written to specifically know that they are accessing their data
    through a GreedyMemmapManager, as they otherwise have to deal with their window size.
    These clients would have to use a SlidingWindowCursor to hide this fact.
    """
    def _obtain_region(self, finfo, offset, size, flags, is_recursive):
        """Create new region without registering it.

        For more information on the parameters, see :meth:`make_cursor()`.

        :return: The newly created region
        """
        fsize = finfo.file_size
        if offset >= fsize:
            raise ValueError("Offset(%s) beyond file-size(%s) for file: %r"
                             % (offset, fsize, finfo))

        rlist = self.regions_for_finfo(finfo)
        if rlist:
            assert len(rlist) == 1
            r = rlist[0]
        else:
            size = min(size or fsize, self.window_size or fsize)   # clamp size to file-size

            if (self.mapped_memory_size + size > self.max_memory_size or
                    self.num_open_regions >= self.max_regions_count):
                self._purge_lru_regions(size)
                is_recursive = True  # Don't recurse below, just cleaned all there is.

            try:
                r = self._make_region(finfo, 0, sys.maxsize, flags)
            except Exception:
                ## Apparently we are out of system resources.
                #  We free up as many regions as possible and retry,
                #  unless we already did that.
                #
                if is_recursive:
                    raise
                self._purge_lru_regions(0)
                r = self._obtain_region(finfo, offset, size, flags, True)

        assert r.includes_ofs(offset)
        return r


class TilingMemmapManager(MemmapManager):

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
        super(TilingMemmapManager, self).__init__(window_size, max_memory_size, max_open_handles)

    def _obtain_region(self, finfo, offset, size, flags, is_recursive):
        ## Bisect to find an existing region.
        #  The c++ implementation cannot do that as
        #  it uses rlist linked list for regions.
        #
        fsize = finfo.file_size
        if offset >= fsize:
            raise ValueError("Offset(%s) beyond file-size(%s) for file: %r"
                             % (offset, fsize, finfo))

        rlist = sorted(self.regions_for_finfo(finfo), key=lambda r: r.ofs)
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
            window_size = self.window_size
            size = min(size or fsize, self.window_size or fsize)   # clamp size to window size

            ## We honor max memory size, and assure we have enough memory available.
            #
            if (self.mapped_memory_size + size > self.max_memory_size or
                    self.num_open_regions >= self.max_regions_count):
                self._purge_lru_regions(window_size)
                is_recursive = True  # Don't recurse below, just cleaned all there is.

            left = self._MapWindowCls(0, 0)
            mid = self._MapWindowCls(offset, size)
            right = self._MapWindowCls(fsize, 0)

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
                r = self._make_region(finfo, mid.ofs, mid.size, flags)
            except Exception:
                ## Apparently we are out of system resources.
                #  We free up as many regions as possible and retry,
                #  unless we already did that.
                #
                if is_recursive:
                    raise
                self._purge_lru_regions(0)
                r = self._obtain_region(finfo, offset, size, flags, True)
            # END handle exceptions
        # END create new region
        assert r.includes_ofs(offset)
        return r

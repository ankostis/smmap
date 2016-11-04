"""
Module containing a memory memory manager which provides a sliding window on a number of memory mapped files.

.. default-role:: object
"""
from collections import namedtuple
from contextlib import contextmanager
import logging
import mmap
import os
import sys

from .mwindow import (
    MemmapRegion,
    FixedWindowCursor,
    SlidingWindowCursor,
)
from .util import (
    PY3,
    is_64_bit,
    suppress,
    string_types,
    Relation,
    finalize,
    ExitStack,
)


__all__ = ['managed_mmaps', "MemmapManagerError",
           "GreedyMemmapManager", "TilingMemmapManager",
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
        """Extends offset downwards and size upwards so that region respects OS page-alignments."""
        nofs = align_to_mmap(self.ofs, 0)
        self.size += self.ofs - nofs                # keep end-point constant
        self.ofs = nofs
        ## Do NOT align end-point, to respect `window-size`,
        #  and to save some time loading from disk.
        #self.size = align_to_mmap(self.size, 1)

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
    """
    Exceptions related to release of resources by memory-manager

    Always ``arg[0]`` is the *mem-manager*.
    """
    def __init__(self, mman, errs):
        msg = '\n  '.join(str(errs).split('\n'))  # indent by 2
        super(MemmapManagerError, self).__init__(mman, msg)


class MemmapManager(object):

    """
    Creates and manages window-handles (regions & cursors) for memory-mapped files.

    - The fundamental memory-handle type to be managed in the :class:`MMapRegion` because
      they encapsulate the underlying os-level *mmap*.

    - Different managers produce *regions* with different sizes, and consequently,
      different *cursor* sizes.

    - *Ranges* becoming "unused" are not immediatelly closed

    - Allocations might fail once a the :attr:`max_memory_size` or :attr:`max_regions_count` limits
      are reached, or if the allocation of os-level *mmaps* fail.  In that case, the least recently used (LRU),
      but "unused" *regions* are automatically released.

    - This is can be used optionally as a non-reetrant reusable context-manager
      inside a ``with ...:`` block, to enusre eny resources are cleared.
      Any errors on :meth:`close()` will be reported as warnings.

    - Not thread-safe!

    """

    __slots__ = (
        '_ix_path_finfo',       # 1-1 registry of {path   <-> finfo}
        '_ix_reg_mmap',         # 1-1 registry of {region <-> mmap} (LRU!)
        '_ix_cur_reg',          # N-1 registry of {cursor --> region}
        'window_size',          # maximum size of a windo
        'max_memory_size',      # maximum amount of memory we may allocate
        'max_regions_count',    # maximum amount of handles to keep open
        '_finalize',            # To replace __del_
        '__weakref__',          # To replace __del_
    )

    #{ Configuration
    _MapWindowCls = _MapWindow
    MapRegionCls = MemmapRegion
    FixedCursorCls = FixedWindowCursor
    SlidingCursorCls = SlidingWindowCursor
    #} END configuration

    def __init__(self, window_size=0, max_memory_size=0, max_open_handles=sys.maxsize):
        """initialize the manager with the given parameters.
        :param window_size: if -1, a default window size will be chosen depending on
            the operating system's architecture. It will internally be quantified to a multiple of the page size
            If 0, the window may have any size, which basically results for *greedy-memman*,
            mapping the whole file at one, and for *tiling-memmap*, ... TODO
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
        if PY3:
            exec('raise MemmapManagerError(self, *ex.args) from None')
        else:
            raise MemmapManagerError(self, *ex.args)

    def __repr__(self):
        if self.closed:
            return "%s(winsize=%s, CLOSED)" % (type(self).__name__, self.window_size)
        else:
            return "%s(winsize=%s, files=%s, regs=(%s, %s), curs=%s)" % (
                type(self).__name__, self.window_size, self.num_open_files,
                self.num_open_regions, self.num_used_regions,
                self.num_open_cursors)

    def _open_region(self, finfo, ofs, size, flags=0):
        # type: (List[MemmapRegion], int, int, int, int) -> MemmapRegion
        """
        Opens the os-level mmap according to the given boundaries, wraps it in a region and registers it.

        Don't use that - use :meth:`_obtain_region()` (which invokes this).

        :param flags:
            additional flags to be given when opening the file.
        :raise Exception:
            if no memory can be allocated

        .. Warning::
            In case of error (i.e. not enough memory) and an open fd was passed in,
            the client is responsible to close it!
        """
        fd = finfo.path_or_fd
        is_path = not isinstance(fd, int)
        if is_path:
            fd = os.open(fd, os.O_RDONLY | getattr(os, 'O_BINARY', 0) | flags)
        try:
            avail_size = finfo.file_size - ofs
            if size > 0:
                avail_size = min(avail_size, size)  # TODO: modify also here for WRITTABLE-Regions
            memmap = mmap.mmap(fd, avail_size, access=mmap.ACCESS_READ, offset=ofs)
            try:
                region = self.MapRegionCls(self, finfo, ofs, len(memmap))
                self._ix_reg_mmap.put(region, memmap)
            except Exception as ex:
                log.warning("Failed allocating *mmap* (%s, %s) for %r due to:  %s!",
                            avail_size, ofs, finfo.path_or_fd, ex)
                memmap.close()
                raise
        finally:
            ## Only close it if we opened it.
            #
            if is_path:
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
        with suppress(Exception if exc_type else ()):
            self.close()

    def close(self):
        """Close all regions and relying memmaps, unless resources cannot close.

        :raise: MemmapManagerError or any unexpected error while closing

        .. Warning::
            In case of failures, the *mman-manager* does NOT close!
            You have to do it later, after clearing resources (possibly in debug).

        .. Note::
            Not to fail on *Windows*, all files and memoryviews referencing mmaps must
            have been closed. If used as a context manager, any errors are suppressed.
            if there is a main-body error.
        """
        if self.closed:
            return

        status_str = str(self)  # for error-messages
        used_regions = list(self._ix_cur_reg.items())

        # cached vars
        _ix_reg_mmap = self._ix_reg_mmap
        _ix_cur_reg = self._ix_cur_reg

        errors = []
        closed_regions = []
        for region, memmap in _ix_reg_mmap.items():
            try:
                memmap.close()
                closed_regions.append(region)
            except Exception as ex:
                errors.append('%s: %s' % (region, ex))

        if not errors:
            ## If all mmaps closed fine, shutdown "quickly"
            #  marking this instances "closed".
            self._ix_reg_mmap = self._ix_path_finfo = self._ix_cur_reg = None

            ## Report open cursors, but this is not deadly.
            #
            if used_regions:
                log.warning("Closed with %s active handles: %s", status_str, status_str)
        else:
            # If mmaps were left open, do NOT close!
            #  Remove only those that closed fine.
            #
            if closed_regions:
                closed_regions = set(closed_regions)
                for cursor in (c
                               for c, r in _ix_cur_reg.items()
                               if r in closed_regions):
                    try:
                        self._release_cursor(cursor)
                    except MemmapManagerError as ex:
                        errors.append('%s: %s' % (cursor, ex))

                for region in closed_regions:
                    try:
                        self._release_region(region)
                    except MemmapManagerError as ex:
                        errors.append('%s: %s' % (region, ex))

            ## Let's hope indexes are left in a good shape...

            ## Scream the errors encountered.
            #
            error_msgs = '\n  '.join('%i. %s' % (i, '\n     '.join(e.split('\n')))  # indent error-bodies by 5
                                     for i, e in enumerate(errors, 1))
            msg = "Failed closing %s due to active handles and %s closing-failures:\n  %s" % (
                status_str, len(errors), error_msgs)
            #
            # Result:
            #    MemmapManagerError(TilingMemmapManager(...)):
            #      Failed closing TilingMemmapManager(...) due to active handles and 2 closing-failures:
            #        1. MemmapRegion(FileInfo(path_or_fd='\\0zthoux2', ...): cannot close exported pointers exist
            ##       2. MemmapRegion(FileInfo(path_or_fd='\\k04hrtdj', ...): cannot close exported pointers exist

            raise MemmapManagerError(self, msg)

    @property
    def closed(self):
        return self._ix_reg_mmap is None

    def regions_for_finfo(self, finfo):
        return [r for r in self._ix_reg_mmap if r.finfo is finfo]

    def regions_for_path(self, path_or_fd):
        return [r for r in self._ix_reg_mmap if r.finfo.path_or_fd == path_or_fd]

    def _mmap_for_region(self, region):
        """Use it only for debuggging."""
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

    @contextmanager
    def _cursor_bound(self, cursor, offset=0, size=0, flags=0):
        """Used by the *sliding-cursor* only - fixed cursors are bound on construction."""
        region = self._obtain_region(cursor.finfo, offset, size, flags, False)
        self._bind_cursor(cursor, region)

        yield region

        self._release_cursor(cursor)

    def _release_region(self, region):
        """Remove `region` from ``_ix_reg_mmap`` and ``_ix_path_finfo`` indexes only.

        Fails if indexes not in perfect shape, but keeps them intact in that case, to retry
        """
        if self.closed:
            return

        finfo = region.finfo

        with ExitStack() as exs:
            _ix_path_finfo = exs.enter_context(self._ix_path_finfo)
            _ix_reg_mmap = exs.enter_context(self._ix_reg_mmap)

            memmap = _ix_reg_mmap.take(region)
            if not self.regions_for_finfo(finfo):
                _ix_path_finfo.take(finfo.path_or_fd)
            try:
                memmap.close()  # Has to be the last step because it cannot rollback.
            except Exception as ex:
                ## Explain which mmap failed to close.
                log.error("Failed closing %s due to: %s", region, ex)
                raise

    def _release_cursor(self, cursor):
        """Remove `cursor` from ``_ix_cur_reg`` index only.

        (protected) fails if indexes not in perfect shape

        .. Note::
            we do not release cursor's region if it becomes "unused",
            but leave it "cached", until resources become scarce, in which case,
            the :meth:`_purge_lru_regions()` or :meth:`close()` cleans them up.

        """
        if not self.closed:
            self._ix_cur_reg.take(cursor)
            #

    def _get_or_create_finfo(self, path_or_fd):
        finfo = self._ix_path_finfo.get(path_or_fd)
        if finfo is None:
            finfo = FileInfo(path_or_fd)
            self._ix_path_finfo[path_or_fd] = finfo
        return finfo

    def make_cursor(self, path_or_fd, offset=0, size=0, flags=0, sliding=False):
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
            the total size of the mapping requested. A non-positive means "as big possible".
            The actual size of the cursor returned (the ``len()``) depends
            a0 on the type of the *memmap-manager* and on the `sliding` argument:

            - If ``sliding==False``, it may be smaller than requested, either because
              the file-size was smaller, the *memmap-manager*'s :attr:`window_size` is smaller,
              or because the map was created between two existing regions.

            - If ``sliding==True`` and it is a :class:`TilingMemmapManager`,
              it will always be the exact given size, if it's positive, or the files-size .

        :param flags:
            additional flags for ``os.open()`` in case there is no region open for this file.
            Has no effect in case an existing region gets reused.

        :return:
            a *cursor* pointing to the given path or file descriptor, or fail
            if offset was beyond the end of the file, or this manager  is the :class:`GreedyMemmapManager`
            and the file is too big to fit into the memory.

            If this manager is the :class:`TilingMemmapManager`, the actual class of the cursor
            depends on the `sliding` argument:

            - If ``sliding==False``, :class:`FixedWindowCursor`,
            - If ``sliding==True``, :class:`SlidingWindowCursor`,

        """
        if offset < 0:
            raise IndexError("Cursor offset must be non-negative: %s, %s, %s!", offset, size, path_or_fd)

        finfo = self._get_or_create_finfo(path_or_fd)
        fsize = finfo.file_size

        if sliding:
            if not isinstance(self, TilingMemmapManager):
                raise ValueError("Only TILING-memap-managers can create SLIDING-cursors!")
            if size <= 0:
                size = fsize - offset
            cursor = self.SlidingCursorCls(self, finfo, offset, size, flags)
            ## No region binding, happens internally, on each method call of the cursor.

        else:
            region = self._obtain_region(finfo, offset, size, flags, False)
            avail_size = min(fsize, region.ofs_end - offset)
            if 0 < size < avail_size:
                avail_size = size
            cursor = self.FixedCursorCls(self, finfo, offset, avail_size)
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
        finfos = set(self._ix_path_finfo.values())
        return sum(1 for r in self._ix_reg_mmap if r.finfo in finfos)

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
    A manager mapping each file into a single full-size region.

    - If a positive :attr:`window_size`` has been set, opening *regions* for files with sizes
      exceeding this limit will fail!  Use *sliding-memmap-managers* for such files.

    - Clients using cursors from this manager may be simpler as they can access.
    """
    def _obtain_region(self, finfo, offset, _, flags, is_recursive):
        """Create new region without registering it.

        For more information on the parameters, see :meth:`make_cursor()`.

        :return: The newly created region
        """
        fsize = finfo.file_size

        if offset >= fsize:
            raise ValueError("Offset(%s) beyond file-size(%s) for file: %r" %
                             (offset, fsize, finfo))

        window_size = self.window_size
        if window_size > 0 and fsize > window_size:
            raise ValueError("File-size exceeds window-size limit %s: %r" %
                             (window_size, finfo))

        rlist = self.regions_for_finfo(finfo)
        if rlist:
            assert len(rlist) == 1
            r = rlist[0]
        else:
            ## Clamp size to file-size/window-size
            #
            if (self.mapped_memory_size + fsize > self.max_memory_size or
                    self.num_open_regions >= self.max_regions_count):
                self._purge_lru_regions(fsize)
                is_recursive = True  # Don't recurse below, just cleaned all there is.

            try:
                r = self._open_region(finfo, 0, fsize, flags)
            except Exception:
                ## Apparently we are out of system resources.
                #  We free up as many regions as possible and retry,
                #  unless we already did that.
                #
                if is_recursive:
                    raise
                self._purge_lru_regions(0)
                r = self._obtain_region(finfo, 0, fsize, flags, True)

        assert r.includes_ofs(offset)
        return r


class TilingMemmapManager(MemmapManager):

    """
    A manager where there can be multiple, but non-overlapping *regions* open for a single file.

    Clients wishing to maintain *greedy-memmap-manaaer*'s simplicity may use *sliding-cursors*.
    """

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
            ## Clamp size to file-size/window-size
            #
            avail_size = min(fsize, window_size)
            if 0 < size < avail_size:
                avail_size = size

            left = self._MapWindowCls(0, 0)
            mid = self._MapWindowCls(offset, avail_size)
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

            ## We honor max memory size, and assure we have enough memory available.
            #
            if (self.mapped_memory_size + mid.size >= self.max_memory_size or
                    self.num_open_regions >= self.max_regions_count):
                self._purge_lru_regions(mid.size)
                is_recursive = True  # Don't recurse below, just cleaned all there is.

            # insert new region at the right offset to keep the order
            try:
                r = self._open_region(finfo, mid.ofs, mid.size, flags)
            except Exception:
                ## Apparently we are out of system resources.
                #  We free up as many regions as possible and retry,
                #  unless we already did that.
                #
                if is_recursive:
                    raise
                self._purge_lru_regions(0)
                r = self._obtain_region(finfo, offset, avail_size, flags, True)
            # END handle exceptions
        # END create new region
        assert r.includes_ofs(offset)
        return r

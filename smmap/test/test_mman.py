from __future__ import print_function

import os
from random import randint
import sys
from time import time
from unittest.case import skipIf

from smmap.mman import (
    SlidingWindowMapManager,
    StaticWindowMapManager,
    _MapWindow)
from smmap.mman import align_to_mmap
from smmap.util import PY3

from .lib import TestBase, FileCreator
from smmap.buf import SlidingWindowMapBuffer


class TestMMan(TestBase):

    def test_window(self):
        wl = _MapWindow(0, 1)        # left
        wc = _MapWindow(1, 1)        # center
        wc2 = _MapWindow(10, 5)      # another center
        wr = _MapWindow(8000, 50)    # right

        assert wl.ofs_end == 1
        assert wc.ofs_end == 2
        assert wr.ofs_end == 8050

        # extension does nothing if already in place
        maxsize = 100
        wc.extend_left_to(wl, maxsize)
        assert wc.ofs == 1 and wc.size == 1
        wl.extend_right_to(wc, maxsize)
        wl.extend_right_to(wc, maxsize)
        assert wl.ofs == 0 and wl.size == 1

        # an actual left extension
        pofs_end = wc2.ofs_end
        wc2.extend_left_to(wc, maxsize)
        assert wc2.ofs == wc.ofs_end and pofs_end == wc2.ofs_end

        # respects maxsize
        wc.extend_right_to(wr, maxsize)
        assert wc.ofs == 1 and wc.size == maxsize
        wc.extend_right_to(wr, maxsize)
        assert wc.ofs == 1 and wc.size == maxsize

        # without maxsize
        wc.extend_right_to(wr, sys.maxsize)
        assert wc.ofs_end == wr.ofs and wc.ofs == 1

        # extend left
        wr.extend_left_to(wc2, maxsize)
        wr.extend_left_to(wc2, maxsize)
        assert wr.size == maxsize

        wr.extend_left_to(wc2, sys.maxsize)
        assert wr.ofs == wc2.ofs_end

        wc.align()
        assert wc.ofs == 0 and wc.size == align_to_mmap(wc.size, True)

    def test_memory_manager(self):
        slide_man = SlidingWindowMapManager()
        static_man = StaticWindowMapManager()

        for man in (static_man, slide_man):
            with man:
                assert man.num_open_regions == 0
                assert man.num_open_cursors == 0
                winsize_cmp_val = 0
                if isinstance(man, StaticWindowMapManager):
                    winsize_cmp_val = -1
                # END handle window size
                assert man.window_size() > winsize_cmp_val
                assert man.mapped_memory_size == 0
                assert man.max_memory_size > 0

                # collection doesn't raise in 'any' mode
                self.assertEqual(man._purge_lru_regions(0), 0)
                # doesn't raise if we are within the limit
                self.assertEqual(man._purge_lru_regions(10), 0)
                # doesn't fail if we over-allocate
                self.assertEqual(man._purge_lru_regions(sys.maxsize), 0)

                # use a region, verify most basic functionality
                with FileCreator(self.k_window_test_size, "manager_test") as fc:
                    fd = os.open(fc.path, os.O_RDONLY)
                    try:
                        for item in (fc.path, fd):
                            with man.make_cursor(item) as c:
                                assert c.path_or_fd is item
                            with c.make_cursor(10, 10) as c:
                                assert c.ofs == 10
                                assert c.size == 10
                                with open(fc.path, 'rb') as fp:
                                    assert c.buffer()[:] == fp.read(20)[10:]

                        if isinstance(item, int):
                            self.assertRaises(AssertionError, getattr, c, 'path')
                        else:
                            self.assertRaises(AssertionError, getattr, c, 'fd')
                        # END handle value error
                    # END for each input
                    finally:
                        os.close(fd)
        # END for each manasger type

    @skipIf(not PY3, "missing `assertRaisesRegex()` ")
    def test_memory_manager_close_with_active_regions(self):
        with FileCreator(self.k_window_test_size, "manager_test") as fc:
            with SlidingWindowMapManager() as mman:
                ## Check that `cursors.close()` without complaints if `mman` has closed prematurely
                with mman.make_cursor(fc.path):
                    exmsg = "with 1 active-Regions, held by 1 Cursors!"
                    self.assertRaisesRegex(ValueError, exmsg, mman.close)

    def test_memman_operation(self):
        # test more access, force it to actually unmap regions
        with FileCreator(self.k_window_test_size, "manager_operation_test") as fc:
            with open(fc.path, 'rb') as fp:
                data = fp.read()
            fd = os.open(fc.path, os.O_RDONLY)
            try:
                max_num_handles = 15
                # small_size =
                for mtype, args in ((StaticWindowMapManager, (0, fc.size // 3, max_num_handles)),
                                    (SlidingWindowMapManager, (fc.size // 100, fc.size // 3, max_num_handles)),):
                    for item in (fc.path, fd):
                        assert len(data) == fc.size

                        # small windows, a reasonable max memory. Not too many regions at once
                        with mtype(window_size=args[0], max_memory_size=args[1], max_open_handles=args[2]) as man:
                            with man.make_cursor(item) as c:
                                # still empty (more about that is tested in test_memory_manager()
                                assert man.num_open_cursors == 1
                                if not isinstance(man, SlidingWindowMapManager):
                                    assert man.mapped_memory_size == fc.size

                            base_offset = 5000
                            # window size is 0 for static managers, hence size will be 0.
                            # We take that into consideration
                            size = man.window_size() // 2
                            with c.make_cursor(base_offset, size) as c:
                                rr = c.region
                                assert len(rr.cursors()) == 1  # the manager and the cursor and us

                                assert man.num_open_cursors == 1
                                assert man.num_open_regions == 1
                                assert man.mapped_memory_size == rr.size

                                # assert c.size == size        # the cursor may overallocate in its static version
                                assert c.ofs == base_offset
                                assert rr.ofs == 0        # it was aligned and expanded
                                if man.window_size():
                                    # but isn't larger than the max window (aligned)
                                    assert rr.size == align_to_mmap(man.window_size(), True)
                                else:
                                    assert rr.size == fc.size
                                # END ignore static managers which dont use windows and are aligned to file boundaries

                                assert c.buffer()[:] == data[base_offset:base_offset + (size or c.size)]
                                pass

                            # obtain second window, which spans the first part of the file;
                            # it is a still the same window
                            nsize = (size or fc.size) - 10
                            with c.make_cursor(0, nsize) as c:
                                assert c.region is rr
                                if man.max_memory_size == fc.size:
                                    assert man.num_open_regions == 2
                                else:
                                    assert man.num_open_regions == 1
                                assert c.size == nsize
                                assert c.ofs == 0
                                assert c.buffer()[:] == data[:nsize]

                            # map some part at the end, our requested size cannot be kept
                            overshoot = 4000
                            base_offset = fc.size - (size or c.size) + overshoot
                            with c.make_cursor(base_offset, size) as c:
                                if man.window_size():
                                    assert man.num_open_regions == 2
                                    assert c.size < size
                                    assert c.region is not rr  # old region is still available,
                                    # but has not curser ref anymore
                                    assert len(rr.cursors()) == 1  # only held by manager
                                else:
                                    assert c.size < fc.size
                                # END ignore static managers which only have one handle per file
                                rr = c.region
                                assert len(rr.cursors()) == 1  # manager + cursor
                                assert rr.ofs < c.ofs  # it should have extended itself to the left
                                assert rr.ofs_end <= fc.size  # it cannot be larger than the file
                                assert c.buffer()[:] == data[base_offset:base_offset + (size or c.size)]

                            assert c.closed
                            if man.window_size():
                                # but doesn't change anything regarding the handle count - we cache it and only
                                # remove mapped regions if we have to
                                assert man.num_open_regions == 2
                            # END ignore this for static managers

                            # iterate through the windows, verify data contents
                            # this will trigger map collection after a while
                            max_random_accesses = 5000
                            num_random_accesses = max_random_accesses
                            memory_read = 0
                            st = time()

                            max_memory_size = man.max_memory_size
                            max_file_handles = man.max_file_handles()
                            while num_random_accesses:
                                num_random_accesses -= 1
                                base_offset = randint(0, fc.size - 1)

                                # precondition
                                if man.window_size():
                                    assert max_memory_size >= man.mapped_memory_size
                                # END statics will overshoot, which is fine
                                assert max_file_handles >= man.num_open_regions
                                with c.make_cursor(base_offset, (size or c.size)) as c:
                                    csize = c.size
                                    assert c.buffer()[:] == data[base_offset:base_offset + csize]
                                    memory_read += csize

                                    assert c.includes_ofs(base_offset)
                                    assert c.includes_ofs(base_offset + csize - 1)
                                    assert not c.includes_ofs(base_offset + csize)
                            # END while we should do an access
                            elapsed = max(time() - st, 0.001)  # prevent zero divison errors on windows
                            mb = float(1000 * 1000)
                            print("%s: Read %i mb of memory with %i random on cursor "
                                  "initialized with %s accesses in %fs (%f mb/s)\n"
                                  % (mtype, memory_read / mb, max_random_accesses, type(item),
                                     elapsed, (memory_read / mb) / elapsed),
                                  file=sys.stderr)

                            # an offset as large as the size doesn't work !
                            self.assertRaises(ValueError, c.make_cursor, fc.size, size)

                            # collection - it should be able to collect all
                            assert man.num_open_regions
                            assert man.collect()
                            assert man.num_open_regions == 0
                        # END for each item
                    # END for each manager type
            finally:
                os.close(fd)

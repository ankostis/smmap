from __future__ import print_function

import os
from random import randint
import sys
from time import time

from smmap.mman import _MapWindow, align_to_mmap, TilingMemmapManager, GreedyMemmapManager

from smmap.mwindow import FixedWindowCursor, SlidingWindowCursor

from .lib import TestBase, FileCreator


try:
    from unittest import skipIf
except ImportError:
    from unittest2 import skipIf  # @UnusedImport


try:
    bytes
except NameError:
    bytes = str  # @ReservedAssignment

man_optimal = (TilingMemmapManager,)
man_worst_case = (TilingMemmapManager,
                  TestBase.k_window_test_size // 100,
                  TestBase.k_window_test_size // 3,
                  15)
static_man = (GreedyMemmapManager,)


def make_mman(mman, *args):
    return mman(*args)


class TestMWindow(TestBase):

    def test_region(self):
        with TilingMemmapManager() as mman:
            with FileCreator(self.k_window_test_size, "window_test") as fc:
                half_size = fc.size // 2
                rofs = align_to_mmap(4200, False)
                finfo = mman._get_or_create_finfo(fc.path)
                rfull = mman._open_region(finfo, ofs=0, size=fc.size)
                rhalfofs = mman._open_region(finfo, ofs=rofs, size=fc.size)
                rhalfsize = mman._open_region(finfo, ofs=0, size=half_size)

                # offsets
                assert rfull.ofs == 0 and rfull.size == fc.size
                assert rfull.ofs_end == fc.size   # if this method works, it works always

                assert rhalfofs.ofs == rofs and rhalfofs.size == fc.size - rofs
                assert rhalfsize.ofs == 0 and rhalfsize.size == half_size

                assert rfull.includes_ofs(0)
                assert rfull.includes_ofs(fc.size - 1)
                assert rfull.includes_ofs(half_size)
                assert not rfull.includes_ofs(-1)
                assert not rfull.includes_ofs(sys.maxsize)

            # auto-refcount
            assert len(rfull.cursors()) == 0

            # window constructor
            w = _MapWindow.from_region(rfull)
            assert w.ofs == rfull.ofs and w.ofs_end == rfull.ofs_end

    def test_fixed_cursor(self):
        with TilingMemmapManager() as mman:
            with FileCreator(self.k_window_test_size, "cursor_test") as fc:
                self.assertRaises(TypeError, FixedWindowCursor, mman)  # missing args

                cv = mman.make_cursor(fc.path)
                assert not cv.closed
                assert cv.file_size == fc.size
                assert cv.path == fc.path

            # unuse non-existing region manually is fine
            cv.release()
            # but not 2nd time
            self.assertRaises(Exception, cv.release)
            # yet close() ok many time
            cv.close()


class TestSliding(TestBase):

    def test_basics(self):
        with FileCreator(self.k_window_test_size, "buffer_test") as fc:
            # invalid paths fail upon construction
            with make_mman(*man_optimal) as mman:
                buf = mman.make_cursor(fc.path, size=fc.size, sliding=True)

                ## Born open, stays open...
                assert not buf.closed
                buf.close()
                buf.close()
                assert not buf.closed

                offset = 100
                buf = mman.make_cursor(fc.path, offset, fc.size, sliding=True)
                assert len(buf) == fc.size  # Actually we are extending file by `offset` bytes!
                assert not buf.closed

                ## Better let mman decide size.
                buf = mman.make_cursor(fc.path, offset, 0, sliding=True)
                assert len(buf) == fc.size - offset
                assert not buf.closed


                # simple access
                with open(fc.path, 'rb') as fp:
                    data = fp.read()
                assert data[offset] == buf[0]
                assert data[offset:offset * 2] == buf[0:offset]

                # negative indices, partial slices
                # Just check contextmanaging ok
                with buf:
                    assert buf[-1] == buf[len(buf) - 1]
                    assert buf[-10:] == buf[len(buf) - 10:len(buf)]

                ## Born open, stays open...
                buf.close()
                assert not buf.closed


                # Killing it is ok?
                del(buf)

                assert mman.num_open_regions == 1, mman.num_open_regions

    def test_win_size(self):
        fsize = 15
        winsize = fsize // 3

        with FileCreator(fsize, "winsize") as fc:
            with open(fc.path, 'rb') as fp:
                fdata = fp.read()

            with TilingMemmapManager(window_size=winsize) as mman:
                c = mman.make_cursor(fc.path, sliding=True)
                assert c[0] == fdata[0]
                assert mman.num_open_regions == 1
                assert mman.num_used_regions == 0
                region = mman.regions_for_finfo(c.finfo)[0]
                assert region.size == winsize
                assert region.size == winsize

                ## Make a dissjoined region.
                ofs = 2 * winsize   # == 10
                assert c[ofs] == fdata[ofs]
                assert mman.num_open_regions == 2
                assert mman.num_used_regions == 0
                region = mman.regions_for_finfo(c.finfo)[1]
                ## OK, region grows from 0 (aligned) to include offset (10).
                assert region.size == ofs + 1

    def test_performance(self):
        # PERFORMANCE
        # blast away with random access and a full mapping - we don't want to
        # exaggerate the manager's overhead, but measure the buffer overhead
        # We do it once with an optimal setting, and with a worse manager which
        # will produce small mappings only !
        with FileCreator(self.k_window_test_size, "buffer_test") as fc:
            with open(fc.path, 'rb') as fp:
                data = fp.read()
            max_num_accesses = 100
            fd = os.open(fc.path, os.O_RDONLY)
            for item in (fc.path, fd):
                fsize = fc.size
                for mman, man_id in ((man_optimal, 'optimal'),
                                     (man_worst_case, 'worst case'),
                                     ):
                    with make_mman(*mman) as manager:
                        buf = manager.make_cursor(item, sliding=True)
                        assert manager.num_open_regions == 0, manager
                        for access_mode in range(2):    # single, multi
                            num_accesses_left = max_num_accesses
                            num_bytes = 0

                            st = time()
                            while num_accesses_left:
                                num_accesses_left -= 1
                                if access_mode:  # multi
                                    ofs_start = randint(0, fsize)
                                    ofs_end = randint(ofs_start, fsize)
                                    d = buf[ofs_start:ofs_end]
                                    assert len(d) == ofs_end - ofs_start
                                    assert d == data[ofs_start:ofs_end]
                                    num_bytes += len(d)
                                    del d
                                else:
                                    pos = randint(0, fsize)
                                    assert buf[pos] == data[pos]
                                    num_bytes += 1
                                # END handle mode
                            # END handle num accesses

                        assert manager.num_open_regions
                        if isinstance(manager, TilingMemmapManager):
                            assert manager.num_open_regions >= 1
                        else:
                            assert manager.num_open_regions == 1
                        assert manager.num_used_regions == 0
                        if isinstance(manager, TilingMemmapManager):
                            assert manager.collect() >= 0
                        else:
                            assert manager.collect() == 0    # all regions currently used by buf

                        assert manager.num_open_regions == 0
                        assert manager.num_used_regions == 0
                        assert manager.collect() == 0
                        elapsed = max(time() - st, 0.001)  # prevent zero division errors on windows
                        mb = float(1000 * 1000)
                        mode_str = (access_mode and "slice") or "single byte"
                        print("%s: Made %i random %s accesses to buffer created from %s "
                              "reading a total of %f mb in %f s (%f mb/s)"
                              % (man_id, max_num_accesses, mode_str, type(item),
                                 num_bytes / mb, elapsed, (num_bytes / mb) / elapsed),
                              file=sys.stderr)
                        # END handle access mode
                        del buf
                        assert manager.collect() == 0
                # END for each manager
            # END for each input
            os.close(fd)

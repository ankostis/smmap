from __future__ import print_function

import os
from random import randint
import sys
from time import time

from smmap.mman import _MapWindow, align_to_mmap, TilingMemmapManager, GreedyMemmapManager
from smmap.util import PY3

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
                rfull = mman._make_region(finfo, ofs=0, size=fc.size)
                rhalfofs = mman._make_region(finfo, ofs=rofs, size=fc.size)
                rhalfsize = mman._make_region(finfo, ofs=0, size=half_size)

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

    @skipIf(not PY3, "mmap is not a buffer, so memoryview fails")
    def test_cursor_hangs(self):
        with FileCreator(1024 * 1024 * 8) as fc:
            #with self.assertRaisesRegex(ValueError, "cannot close exported pointers exist"):
                with TilingMemmapManager() as mman:
                    with mman.make_cursor(fc.path) as c:
                        data = memoryview(c.map())
                        assert data[0:5] == b'\x00\x00\x00\x00\x00'
                assert data[3] == 0
            #data.release()
            #mman.close()


class TestSliding(TestBase):

    def test_basics(self):
        with FileCreator(self.k_window_test_size, "buffer_test") as fc:
            # invalid paths fail upon construction
            with make_mman(*man_optimal) as mman:
                buf = SlidingWindowCursor(mman, fc.path, size=fc.size)
                assert buf.closed
                assert not buf.cursor
                buf.close()
                buf.close()

                with buf:
                    assert not buf.closed
                    assert buf.cursor
                    assert len(buf) == fc.size

                offset = 100
                with SlidingWindowCursor(mman, fc.path, offset) as buf:
                    assert len(buf) == fc.size - offset
                    assert not buf.closed

                # empty begin access keeps it valid on the same path, but alters the offset
                with buf:
                    assert len(buf) == fc.size - offset, (len(buf), fc.size)
                    assert not buf.closed
                    assert not buf.cursor.closed

                    # simple access
                    with open(fc.path, 'rb') as fp:
                        data = fp.read()
                    assert data[offset] == buf[0]
                    assert data[offset:offset * 2] == buf[0:offset]

                    # negative indices, partial slices
                    assert buf[-1] == buf[len(buf) - 1]
                    assert buf[-10:] == buf[len(buf) - 10:len(buf)]

                # Double-release screams
                #self.assertRaises(Exception, buf.release) TODO: renenable when
                # but double-close() not.
                buf.close()
                buf.close()
                assert buf.closed

                # an empty begin access fixes it up again
                with buf:
                    assert not buf.closed
                del(buf)        # ends access automatically

                assert mman.num_open_regions == 1, mman.num_open_regions

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
                for mman, man_id in ((man_optimal, 'optimal'),
                                     (man_worst_case, 'worst case'),
                                     (static_man, 'static optimal')):
                    with make_mman(*mman) as manager:
                        with SlidingWindowCursor(manager, item) as buf:
                            assert manager.num_open_regions == 1
                            for access_mode in range(2):    # single, multi
                                num_accesses_left = max_num_accesses
                                num_bytes = 0
                                fsize = fc.size

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
                            assert manager.num_used_regions == 1
                            if isinstance(manager, TilingMemmapManager):
                                assert manager.collect() >= 0
                            else:
                                assert manager.collect() == 0    # all regions currently used by buf

                        assert manager.num_open_regions
                        assert manager.num_open_regions == 1
                        assert manager.num_used_regions == 0
                        assert manager.collect() == 1
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

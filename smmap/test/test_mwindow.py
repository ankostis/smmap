from copy import copy
from mmap import ALLOCATIONGRANULARITY
import os
import sys
from unittest.case import skipIf

from smmap.mman import _MapWindow, align_to_mmap, SlidingWindowMapManager, _RegionList
from smmap.mwindow import MapRegion, WindowCursor
from smmap.util import is_64_bit, PY3

from .lib import TestBase, FileCreator


class TestMWindow(TestBase):

    def test_cursor(self):
        with SlidingWindowMapManager() as mman:
            with FileCreator(self.k_window_test_size, "cursor_test") as fc:
                self.assertRaises(TypeError, WindowCursor, mman)  # missing args

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

    def test_region(self):
        with SlidingWindowMapManager() as mman:
            with FileCreator(self.k_window_test_size, "window_test") as fc:
                half_size = fc.size // 2
                rofs = align_to_mmap(4200, False)
                rlist = mman.get_or_create_rlist(fc.path)
                rfull = mman._make_region(rlist, ofs=0, size=fc.size)
                rhalfofs = mman._make_region(rlist, ofs=rofs, size=fc.size)
                rhalfsize = mman._make_region(rlist, ofs=0, size=half_size)

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

    def test_region_list(self):
        with FileCreator(100, "sample_file") as fc:
            fd = os.open(fc.path, os.O_RDONLY)
            try:
                for item in (fc.path, fd):
                    ml = _RegionList(item)

                    assert len(ml) == 0
                    assert ml.path_or_fd == item
                    assert ml.file_size == fc.size
            finally:
                os.close(fd)

    def test_util(self):
        assert isinstance(is_64_bit, bool)    # just call it
        assert align_to_mmap(1, False) == 0
        assert align_to_mmap(1, True) == ALLOCATIONGRANULARITY

    @skipIf(not PY3, "mmap is not a buffer, so memoryview fails")
    def test_cursor_hangs(self):
        with FileCreator(1024 * 1024 * 8) as fc:
            #with self.assertRaisesRegex(ValueError, "cannot close exported pointers exist"):
                with SlidingWindowMapManager() as mman:
                    with mman.make_cursor(fc.path) as c:
                        data = memoryview(c.map())
                        assert data[0:5] == b'\x00\x00\x00\x00\x00'
                assert data[3] == 0
            #data.release()
            #mman.close()

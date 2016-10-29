from copy import copy
from mmap import ALLOCATIONGRANULARITY
import os
import sys

from smmap.mman import _MapWindow, align_to_mmap, SlidingWindowMapManager, WindowCursor
from smmap.util import is_64_bit

from smmap.mwindow import MapRegion, MapRegionList

from .lib import TestBase, FileCreator


class TestMWindow(TestBase):

    def test_cursor(self):
        with FileCreator(self.k_window_test_size, "cursor_test") as fc:
            man = SlidingWindowMapManager()
            ci = WindowCursor(man)  # invalid cursor
            assert not ci.is_valid()
            assert not ci.is_associated()
            assert ci.size() == 0       # this is cached, so we can query it in invalid state

            cv = man.make_cursor(fc.path)
            assert not cv.is_valid()    # no region mapped yet
            assert cv.is_associated()  # but it know where to map it from
            assert cv.file_size() == fc.size
            assert cv.path() == fc.path

        # copy module
        cio = copy(cv)
        assert not cio.is_valid() and cio.is_associated()

        # unuse non-existing region is fine
        cv.unuse_region()
        cv.unuse_region()

        # destruction is fine (even multiple times)
        cv._destroy()
        WindowCursor(man)._destroy()

    def test_region(self):
        with FileCreator(self.k_window_test_size, "window_test") as fc:
            half_size = fc.size // 2
            rofs = align_to_mmap(4200, False)
            rfull = MapRegion(fc.path, 0, fc.size)
            rhalfofs = MapRegion(fc.path, rofs, fc.size)
            rhalfsize = MapRegion(fc.path, 0, half_size)

            # offsets
            assert rfull.ofs_begin() == 0 and rfull.size() == fc.size
            assert rfull.ofs_end() == fc.size   # if this method works, it works always

            assert rhalfofs.ofs_begin() == rofs and rhalfofs.size() == fc.size - rofs
            assert rhalfsize.ofs_begin() == 0 and rhalfsize.size() == half_size

            assert rfull.includes_ofs(0)
            assert rfull.includes_ofs(fc.size - 1)
            assert rfull.includes_ofs(half_size)
            assert not rfull.includes_ofs(-1)
            assert not rfull.includes_ofs(sys.maxsize)

        # auto-refcount
        assert rfull.client_count() == 1
        _ = rfull
        assert rfull.client_count() == 1, "no auto-counting"

        # window constructor
        w = _MapWindow.from_region(rfull)
        assert w.ofs == rfull.ofs_begin() and w.ofs_end() == rfull.ofs_end()

    def test_region_list(self):
        with FileCreator(100, "sample_file") as fc:
            fd = os.open(fc.path, os.O_RDONLY)
            try:
                for item in (fc.path, fd):
                    ml = MapRegionList(item)

                    assert len(ml) == 0
                    assert ml.path_or_fd() == item
                    assert ml.file_size() == fc.size
            finally:
                os.close(fd)

    def test_util(self):
        assert isinstance(is_64_bit, bool)    # just call it
        assert align_to_mmap(1, False) == 0
        assert align_to_mmap(1, True) == ALLOCATIONGRANULARITY

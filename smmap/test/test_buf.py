from __future__ import print_function

from .lib import TestBase, FileCreator

from smmap.mman import (
    SlidingWindowMapManager,
    StaticWindowMapManager,
)
from smmap.buf import SlidingWindowMapBuffer

from random import randint
from time import time
import sys
import os


man_optimal = SlidingWindowMapManager()
man_worst_case = SlidingWindowMapManager(
    window_size=TestBase.k_window_test_size // 100,
    max_memory_size=TestBase.k_window_test_size // 3,
    max_open_handles=15)
static_man = StaticWindowMapManager()


class TestBuf(TestBase):

    def test_basics(self):
        with FileCreator(self.k_window_test_size, "buffer_test") as fc:
            # invalid paths fail upon construction
            with man_optimal as mman:
                buf = SlidingWindowMapBuffer(mman, fc.path, size=fc.size)
                assert buf.closed
                assert not buf.cursor
                buf.close()
                buf.close()

                with buf:
                    assert not buf.closed
                    assert buf.cursor
                    assert len(buf) == fc.size

                offset = 100
                with SlidingWindowMapBuffer(mman, fc.path, offset) as buf:
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
                self.assertRaises(Exception, buf.release)
                # but double-close() not.
                buf.close()
                buf.close()
                assert buf.closed

                # an empty begin access fixes it up again
                with buf:
                    assert not buf.closed
                del(buf)        # ends access automatically

                assert man_optimal.num_open_regions == 1, man_optimal.num_open_regions

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
                for manager, man_id in ((man_optimal, 'optimal'),
                                        (man_worst_case, 'worst case'),
                                        (static_man, 'static optimal')):

                    with SlidingWindowMapBuffer(manager, item) as buf:
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
                        assert manager.num_open_regions == 1
                        assert manager.num_used_regions == 1
                        assert manager.collect() == 0  # all regions currently used by buf

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

"""Module with a simple buffer implementation using the memory manager"""
import sys
from smmap.mwindow import _WindowHandle

__all__ = ["SlidingWindowMapBuffer"]


try:
    bytes
except NameError:
    bytes = str  # @ReservedAssignment


class SlidingWindowMapBuffer(_WindowHandle):

    """A buffer like object which allows direct byte-wise object and slicing into
    memory of a mapped file. The mapping is controlled by the slicing

    The buffer is relative, that is if you map an offset, index 0 will map to the
    first byte at the offset you used during initialization.

    **Note:** Although this type effectively hides the fact that there are mapped windows
    underneath, it can unfortunately not be used in any non-pure python method which
    needs a buffer or string"""
    __slots__ = (
        '_c',           # our cursor
        'flags',        # flags for cursor crearion
    )

    def __init__(self, mman, path_or_fd, offset=0, size=0, flags=0):
        # TODO: create sliding-buf from mman
        """Initalize the instance to operate on the given cursor.

        :param cursor: the cursor to the file you want to access
        :param offset: absolute offset in bytes
        :param size: the total size of the mapping. non-positives mean, as big possible.
            From that point on, the __len__ of the buffer will be the given size or the file size.
            If the size is larger than the mappable area, you can only access the actually available
            area, although the length of the buffer is reported to be your given size.
            Hence it is in your own interest to provide a proper size !
        :param flags: Additional flags to be passed to os.open
        :raise ValueError: if the buffer could not achieve a valid state"""
        self.flags = flags
        finfo = mman._get_or_create_finfo(path_or_fd)
        avail_size = finfo.file_size - offset
        if 0 < size < avail_size:
            avail_size = size
        super(SlidingWindowMapBuffer, self).__init__(mman, finfo, offset, avail_size)
        self._c = None

    def __enter__(self):
        self._c = self.mman.make_cursor(self.path_or_fd, self.ofs, self.size, self.flags)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def __getitem__(self, i):
        if isinstance(i, slice):
            return self.__getslice__(i.start or 0, i.stop or self.size)
        c = self._c
        assert not c.closed
        if i < 0:
            i = self.size + i
        if not c.includes_ofs(i):
            c.release()
            self._c = c = c.make_cursor(i, 1)
        # END handle region usage
        return c.buffer()[i - c.ofs]

    def __getslice__(self, i, j):
        c = self._c
        # fast path, slice fully included - safes a concatenate operation and
        # should be the default
        assert not c.closed
        if i < 0:
            i = self.size + i
        if j == sys.maxsize:
            j = self.size
        if j < 0:
            j = self.size + j
        if (c.ofs <= i) and (j < c.ofs_end):
            b = c.ofs
            return c.buffer()[i - b:j - b]
        else:
            l = j - i                 # total length
            ofs = i
            # It's fastest to keep tokens and join later, especially in py3, which was 7 times slower
            # in the previous iteration of this code
            pyvers = sys.version_info[:2]
            if (3, 0) <= pyvers <= (3, 3):
                # Memory view cannot be joined below python 3.4 ...
                out = bytes()
                while l:
                    c.release()
                    self._c = c = c.make_cursor(ofs, l)
                    d = c.buffer()[:l]
                    ofs += len(d)
                    l -= len(d)
                    # This is slower than the join ... but what can we do ...
                    out += d
                    del(d)
                # END while there are bytes to read
                return out
            else:
                md = []
                while l:
                    c.release()
                    self._c = c = c.make_cursor(ofs, l)
                    d = c.buffer()[:l]
                    ofs += len(d)
                    l -= len(d)
                    # Make sure we don't keep references, as c.use_region() might attempt
                    # to free resources, but can't unless we use pure bytes
                    if hasattr(d, 'tobytes'):
                        d = d.tobytes()
                    md.append(d)
                # END while there are bytes to read
                return bytes().join(md)
        # END fast or slow path
    #{ Interface

    @property
    def closed(self):
        assert not self._c or not self._c.closed
        return not bool(self._c)

    def release(self):
        """Call this method once you are done using the instance. It is automatically
        destroys cursor, and should be called just in time to allow system
        resources to be freed.
        """
        ## TODO: Buf inherit cursor.
        #  For now, have to break the "fail" rul becuase is not implemented
        #          #  an a "one off" context-manager.
        if self._c:
            self._c.release()
            self._c = None

    @property
    def cursor(self):
        """:return: the current cursor providing access to the data, which is None initially"""
        return self._c

    #}END interface

.. _tutorial-label:

###########
Usage Guide
###########
This text briefly introduces you to the basic design decisions and accompanying classes.


Design
======
The main task of this library is to manage "windows" into memory-mapped files.
There are 2 types of window-handles inheriting from :class:`smmap.mman.MemmapWindow`:

- *regions*, cached internally to encapsulate :data:`mmap.mmap` mapped *N-1* into files, and
- *cursors*, the client-facing windows that match *N-1* into *regions*.

To use *cursors*, you first need to configure and hold a *memap-manager*
(:class`:smmap.mman.MemmapManager`) throughout the application, and close it, to release resources
(mostly file-pointers and/or :class:`memoryview` instances).


Memory Manager
==============
There are two types of memory managers, whereas both allocate and manage "fixed" *regions*
of files mapped into memory (:class:`smmap.mman.MemmapRegion`):

1. :class:`smmap.mman.GreedyMemmapManager`: the *greedy* mem-manager always maps the whole file,
   or fail, keeping a single region mapping per file.  These choices allow for making
   some assumptions to simplify data access and increase performance.
   On the other hand, it has reduced limits on 32bit systems, or may exhaust memory on 64bit
   for giant files.

2. :class:`smmap.mman.TilingMemmapManager`: the *tiling* memmap-manager allocates possibly multiple,
   configurably small regions for each file.

The *tiling* memory manager therefore should be the default manager when preparing an application
for handling huge amounts of data on both 32bit and 64bit systems::

    >>> import smmap
    >>> mman = smmap.TilingMemmapManager()

The manager provides much useful information about its current state
like the amount of open file handles or the amount of mapped memory::

    >>> mman.num_open_regions
    0
    >>> mman.num_used_regions
    0
    >>> mman.num_open_cursors
    0
    >>> mman.mapped_memory_size
    0

You have to **remember always to close it at the end**::

    >>> mman.close()

.. Tip::

   The *memory-managers* are (optionally) re-entrant, but not thread-safe, context-manager(s),
   to be used within a ``with ...:`` block, ensuring any left-overs cursors are cleaned up.

   You may use :class:`contextlib.ExitStack()` to store them for longer-term lifetime.


Cursors
=======
*Cursors* are handles onto a *region* of a file mapped into memory.  You obtain *cursors*
also from the *memmap-manager*, and you them as "buffers" to access the underlying file-bytes.
There are also 2 types of cursors:

1. *fixed-cursor:* it implements a one-off buffer;
1. *sliding-cursor:* it manages region allocation behind its simple buffer like interface.

Note that as long as a *cursor* points into a *region*, the later is considered "used",
and cannot been collected, even if resources are falling short, so you must release them
asap.

Let's make a sample file with random bytes (remember to delete it later with ``del fc``)::

    >>> import smmap.test.lib
    >>> fc = smmap.test.lib.FileCreator(20, "test_file", final_byte=b'\xee')
    >>> with open(fc.path, 'rb') as fp:
    ...     fdata = fp.read()
    >>> fdata[-1:] == b'\xee'
    True


and asked as much data as possible starting, from offset 0::

    >>> mman = smmap.TilingMemmapManager()      # Remember to close it
    >>> c = mman.make_cursor(fc.path)
    >>> c.ofs == 0
    True
    >>> c.size == fc.size
    True

Since cursors hold open files for memory mapping, you must explicitly call :meth:`c.close()`
or the more "strict" :meth:`c.release()` (only once invocation allowed)::

    >>> c.release()
    >>> assert c.closed

But it is safer to include their access within a ``with ...:`` blocks::

    >>> with mman.make_cursor(fc.path) as c:
    ...     assert not c.closed
    ...     assert c.size == fc.size
    ...     data = c.buffer()
    ...     assert data[0] == fdata[0]
    ...     assert data[-1] == data[c.size - 1] == ord(b'\xee')

    >>> assert c.closed         # Cursor closed on context-exit .

Notice that you cannot interrogate the data from a "closed" cursor::

    >>> c.buffer()[0]
    Traceback (most recent call last):
    AttributeError: 'NoneType' object has no attribute 'buffer'

You can still query absolute offsets, and check whether an offset is included
in the cursor's data::

    >>> c.ofs < c.ofs_end
    True
    >>> c.includes_ofs(19)
    True
    >>> c.includes_ofs(20)
    False

If you ask for a cursor beyond the file-size (20 in this example), it will fail::

    >>> c.make_cursor(offset=21)
    Traceback (most recent call last):
    ValueError: Offset(21) beyond file-size(20) for file:
        ...

    >>> assert c.closed         # Previous cursor remains closed anyhow.


Its recommended not to create big slices when feeding the buffer
into consumers (e.g. struct or zlib).
Instead, either give the buffer directly, or on PY2 use python's buffer command::

    >>> buffer(c.buffer(), 1, 9)    # first 9 bytes without copying them # doctest: +SKIP

Once a cursor has been closed, you may still obtain a new cursor bound
on another region of the file with :meth:`c.make_cursor()` or :meth:`c.next_cursor()`::

    >>> with c.make_cursor(10, 5) as c2:
    ...     assert c2 is not c          # a new cursor indeed
    ...     data = c2.buffer()
    ...     assert data[0:5] == fdata[10:15]

    >>> with c2.next_cursor() as c:     # start re-using the `c` var
    ...     assert c.ofs == 15
    ...     assert c.buffer()[0:5] == fdata[15:]

    >>> with c.next_cursor() as c:
    ...     """Got pas the end of the file..."""
    Traceback (most recent call last):
    ValueError: Offset(20) beyond file-size(20) for file: ...




Now you would have to write your algorithms around this interface to properly slide through
huge amounts of data.  Alternatively you can use the "sliding-buffer" convenience interface.


Sliding cursors
---------------
To facilitate usability at the expense of performance, the :class:`smmap.mwindow.SlidingWindowCursor`
uses multiple regions internally.  That way you can access all data in a possibly huge file
with a single *cursor*.

And actually you don't have to tediously acquire and release cursors,
acquiring the *mmemp-manager* is enough.

.. Note::
   Only *tiling-memmap-managers* can create *sliding-cursors*.

::

    >>> with smmap.TilingMemmapManager() as mman:
    ...     c = mman.make_cursor(fc.path, sliding=True)
    ...     assert c.size == fc.size                        # A cursore till the end of the file.
    ...     assert c[0] == fdata[0]                         # access the first byte
    ...     assert c[-1] == ord(b'\xee')                    # access the last ten bytes on the file
    ...     assert c[-5:] == fdata[fc.size - 5:fc.size]     # access the last five bytes
    ...
    ...     assert not c.closed                             # Note that the cursor is born open ...
    ...     c.close()                                       # and stays open even if ...
    ...     assert not c.closed                             # told to close.
    >>> c.closed                                            # It closes only if it's memmap-manager has closed.
    True

Let's artificially limit the ``window_size`` of the *memmap-manager* to have it
generate multiple regions::

    >>> win_size = 5
    >>> with smmap.TilingMemmapManager(window_size=win_size) as mman:
    ...     c = mman.make_cursor(fc.path, sliding=True)     # NOTE: you must re-create the cursor for the new mmanager.
    ...     assert mman.num_open_regions == 0               # Initially, no resources allocated.
    ...     assert c[0] == fdata[0]                         # Create a region for the 1st-byte...
    ...     assert mman.num_open_regions == 1               # and a region has been created ...
    ...     assert mman.num_used_regions == 0               # but is now "cached".
    ...     region = mman.regions_for_finfo(c.finfo)[0]
    ...     assert region.size == win_size
    ...     assert c[5] == fdata[5]                         # Access a byte beyond the region and ...
    ...     assert mman.num_open_regions == 2               # a 2nd "cached" region gets created...

If you need different initial offsets/size/flags, then you have to create a new instance.


Disadvantages
-------------
- *Sliding-cursors* cannot be used in place of strings or maps, hence you have to slice them
  to have valid input for the sorts of *struct* and *zlib* libraries.
- A slice means a lot of data handling overhead which makes *sliding* cursors slower
  compared to *fixed* ones.


.. Tip::
    Remember to close the memory-manager ans delete the sample-file::

        >>> mman.close()
        >>> del fc

#########
Changelog
#########

2.1.1
======

BREAKING CHANGES, actually a new project!

- Class hierarchy is comprised of two kinds of "objects": 
  - the **memap-managers** (class:`smmap.mman.MemmapManager`), which are  
    both creating and managing the *window-handles* (see below).  These are:
    - the :class:`smmap.mman.GreadyMemmapManager` (the old ``StaticWindowMapManager``)
      which produces cursors that always map the whole file, and
    - the :class:`smmap.mman.TilingMemmapManager` (the old ``SlidingWindowMapManager``)
      which allocates possibly multiple, configurably small regions for each file.
    
  - the immutable **window-handles** (class:`smmap.mwindow.WindowHandle`) 
    further divided into:
    -  the **regions** (class:`smmap.mwindow.MemmapRegion`), that represent actual 
       os-level :class:`mmap.mmap`, and ...
    -  the **cursors**, the client-facing handles into memory mapped files, 
       which are further subdivided into:
       - the :class:`smmap.mwindow.FixedWindowCursor`, (the old cursor), and
       - the :class:`smmap.mwindow.SlidingWindowCursor`, (the old ``SlidingWindowMapBuffer``).

- All state is handled by *memmap-managers* using :class:`smmap.util.Relation` indexes;
  the *window-handles* are immutable.
- All objects have been retrofitted as context-managers, to release resources deterministically.
- Use :class:`weakref.finalize` instead of ``__del__()`` to release leaked resources.


v0.9.0
========
- Fixed issue with resources never being freed as mmaps were never closed.
- Client counting is now done manually, instead of relying on pyton's reference count


v0.8.5
========
- Fixed Python 3.0-3.3 regression, which also causes smmap to become about 3 times slower depending on the code path. It's related to this bug (http://bugs.python.org/issue15958), which was fixed in python 3.4


v0.8.4
========
- Fixed Python 3 performance regression


v0.8.3
========
- Cleaned up code and assured it works sufficiently well with python 3


v0.8.1
========
- A single bugfix


v0.8.0
========

- Initial Release

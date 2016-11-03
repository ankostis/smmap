"""Intialize the smmap package"""

# make everything available in root package for convenience
from .mman import *     # noqa F401
from .mwindow import *  # noqa F401

__author__ = "Sebastian Thiel"
__contact__ = "byronimo@gmail.com"
__homepage__ = "https://github.com/Byron/smmap"
version_info = (2, 1, 1, 'dev4')
__version__ = '.'.join(str(i) for i in version_info)

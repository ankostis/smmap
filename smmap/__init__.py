"""Intialize the smmap package"""

__author__ = "Sebastian Thiel"
__contact__ = "byronimo@gmail.com"
__homepage__ = "https://github.com/Byron/smmap"
version_info = (2, 1, 1, 'dev2')
__version__ = '.'.join(str(i) for i in version_info)

# make everything available in root package for convenience
from .mman import *     # @IgnorePep8
from .mwindow import *  # @IgnorePep8

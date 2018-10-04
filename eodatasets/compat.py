# coding=utf-8
# We're using references that don't exist in python 3 (unicode, long):
# pylint: skip-file
"""
Compatibility helpers for Python 2 and 3.

See: http://lucumr.pocoo.org/2013/5/21/porting-to-python-3-redux/

"""
import sys

PY2 = sys.version_info[0] == 2

if not PY2:
    text_type = str
    string_types = (str,)
    integer_types = (int,)
    unicode_to_char = chr
    long_int = int
else:
    raise RuntimeError("Python 2 is no longer supported")

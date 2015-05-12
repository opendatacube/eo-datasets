# coding=utf-8
# We're using references that don't exist in python 3 (unicode, long):
# pylint: disable=undefined-variable
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
    unichr = chr
    long_int = int
else:
    text_type = unicode
    string_types = (str, unicode)
    integer_types = (int, long)
    unichr = unichr
    long_int = long

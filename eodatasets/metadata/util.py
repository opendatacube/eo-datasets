# -*- coding: utf-8 -*-


import logging

import datetime

_LOG = logging.getLogger(__name__)


def parse_type(s):
    """Parse the string `s` and return a native python object.

    >>> parse_type('01:40:54.7722350Z')
    datetime.time(1, 40, 54, 772235)
    >>> parse_type('2015-03-29')
    datetime.date(2015, 3, 29)
    >>> # Some have added quotes
    >>> parse_type('"01:40:54.7722350Z"')
    datetime.time(1, 40, 54, 772235)
    >>> parse_type("NONE")
    >>> parse_type("Y")
    True
    >>> parse_type("N")
    False
    >>> parse_type('Plain String')
    'Plain String'
    >>> parse_type('"Quoted String"')
    'Quoted String'
    >>> parse_type('"Quoted String"')
    'Quoted String'
    >>> parse_type('1.0')
    1.0
    >>> parse_type('31')
    31
    >>> parse_type('-4.1')
    -4.1
    >>> parse_type('yellow')
    'yellow'
    """

    strptime = datetime.datetime.strptime

    def yesno(s):
        """Parse Y/N"""
        if len(s) == 1:
            if s == 'Y':
                return True
            if s == 'N':
                return False
        raise ValueError

    def none(s):
        """Parse a NONE"""
        if len(s) == 4 and s == 'NONE':
            return None
        raise ValueError

    parsers = [int,
               float,
               lambda x: strptime(x, '%Y-%m-%dT%H:%M:%SZ'),
               lambda x: strptime(x, '%Y-%m-%d').date(),
               lambda x: strptime(x[0:15], '%H:%M:%S.%f').time(),
               yesno,
               none,
               str]

    for parser in parsers:
        try:
            return parser(s.strip('"'))
        except ValueError:
            pass
    raise ValueError

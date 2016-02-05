# coding=utf-8
from __future__ import absolute_import

import hashlib
import binascii
import logging

# PyLint doesn't recognise many distutils functions when in virtualenv. Not worth the effort.
# pylint: disable=no-name-in-module
from distutils import spawn
from pathlib import Path

_LOG = logging.getLogger(__name__)


def find_exe(name):
    """
    Find the location of the given executable.

    :return: the absolute path to the executable.
    :rtype: str
    """
    executable = spawn.find_executable(name)
    if not executable:
        raise Exception('No %s command found.' % (name,))

    return executable


def calculate_file_hash(filename, hash_fn=hashlib.sha1, block_size=4096):
    """
    Calculate the hash of the contents of a given file path.
    :type filename: str or Path
    :param block_size: Number of bytes to read at a time. (for performance: doesn't affect result)
    :param hash_fn: hashlib function to use. (typically sha1 or md5)
    :return: String of hex characters.
    :rtype: str
    """
    m = hash_fn()
    with Path(filename).open('rb') as f:
        while True:
            d = f.read(block_size)
            if not d:
                break
            m.update(d)

    return binascii.hexlify(m.digest()).decode('ascii')


# 16K seems to be the sweet spot in performance on my machine.
def calculate_file_crc32(filename, block_size=1024 * 16):
    """
    Calculate the crc32 of the contents of a given file path.
    :type filename: str or Path
    :param block_size: Number of bytes to read at a time. (for performance: doesn't affect result)
    :return: String of hex characters.
    :rtype: str
    """
    m = 0
    with Path(filename).open('rb') as f:
        while True:
            d = f.read(block_size)
            if not d:
                break
            m = binascii.crc32(d, m)

    return "%08x" % (m & 0xFFFFFFFF)


class PackageChecksum(object):
    """
    Incrementally build a checksum file for a package.

    (By building incrementally we can better take advantage of filesystem caching)
    """

    def __init__(self):
        self.file_hashes = []

    def add_file(self, file_path):
        """
        Add a file to the checksum list.
        :type file_path: Path
        :rtype: None
        """
        _LOG.info('Checksumming %r', file_path)
        hash_ = calculate_file_hash(file_path)
        _LOG.debug('%r -> %r', file_path, hash_)
        self.file_hashes.append((Path(file_path), hash_))

    def write(self, output_file):
        """
        Write checksums to the given file.
        :type output_file: Path or str
        """
        output_file = Path(output_file)
        with output_file.open('w') as f:
            f.writelines((u'{0}\t{1}\n'.format(str(hash_), str(filename.relative_to(output_file.parent)))
                          for filename, hash_ in sorted(self.file_hashes)))

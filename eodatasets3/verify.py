# coding=utf-8
from __future__ import absolute_import

import binascii
import hashlib
import logging

import typing

from distutils import spawn
from pathlib import Path

_LOG = logging.getLogger(__name__)


def find_exe(name: str):
    """
    Find the location of the given executable.

    :return: the absolute path to the executable.
    :rtype: str
    """
    executable = spawn.find_executable(name)
    if not executable:
        raise Exception(f"No {name!r} command found.")

    return executable


def calculate_file_sha1(filename):
    """
    :type filename: str or Path
    :rtype: str
    """
    return calculate_file_hash(filename, hash_fn=hashlib.sha1)


def calculate_file_hash(filename, hash_fn=hashlib.sha1, block_size=4096):
    """
    Calculate the hash of the contents of a given file path.
    :type filename: str or Path
    :param block_size: Number of bytes to read at a time. (for performance: doesn't affect result)
    :param hash_fn: hashlib function to use. (typically sha1 or md5)
    :return: String of hex characters.
    :rtype: str
    """
    with Path(filename).open("rb") as f:
        return calculate_hash(f, hash_fn, block_size)


def calculate_hash(f, hash_fn=hashlib.sha1, block_size=4096):
    m = hash_fn()

    while True:
        d = f.read(block_size)
        if not d:
            break
        m.update(d)

    return binascii.hexlify(m.digest()).decode("ascii")


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
    with Path(filename).open("rb") as f:
        while True:
            d = f.read(block_size)
            if not d:
                break
            m = binascii.crc32(d, m)

    return f"{m & 0xFFFFFFFF:08x}"


class PackageChecksum(object):
    """
    Incrementally build a checksum file for a package.

    (By building incrementally we can better take advantage of filesystem caching)
    """

    def __init__(self):
        self._file_hashes = {}

    def add_file(self, file_path):
        """
        Add files to the checksum list (recursing into directories.)
        :type file_path: Path
        :rtype: None
        """
        if file_path.is_dir():
            self.add_files(file_path.iterdir())
        else:
            hash_ = self._checksum(file_path)
            self._append_hash(file_path, hash_)

    def add(self, fd: typing.IO, name=None):
        """
        Add a checksum, reading the data from an open file descriptor.
        """
        name = name or fd.name
        if not name:
            raise ValueError("No usable name for checksummed file descriptor")

        _LOG.info("Checksumming %r", name)
        hash_ = calculate_hash(fd)
        _LOG.debug("%r -> %r", name, hash_)
        self._append_hash(name, hash_)

    def _checksum(self, file_path):
        _LOG.info("Checksumming %r", file_path)
        hash_ = calculate_file_hash(file_path)
        _LOG.debug("%r -> %r", file_path, hash_)
        return hash_

    def _append_hash(self, file_path, hash_):
        self._file_hashes[Path(file_path).absolute()] = hash_

    def add_files(self, file_paths):
        for path in file_paths:
            self.add_file(path)

    def write(self, output_file: typing.Union[Path, str]):
        """
        Write checksums to the given file.
        :type output_file: Path or str
        """
        output_file = Path(output_file)
        with output_file.open("wb") as f:
            f.writelines(
                (
                    "{0}\t{1}\n".format(
                        str(hash_), str(filename.relative_to(output_file.parent))
                    ).encode("utf-8")
                    for filename, hash_ in sorted(self._file_hashes.items())
                )
            )

    def read(self, checksum_path):
        """
        Read checksum values from the given checksum file
        :type checksum_path: Path or str
        """
        checksum_path = Path(checksum_path)
        with checksum_path.open("r") as f:
            for line in f.readlines():
                hash_, path = str(line).strip().split("\t")
                self._append_hash(
                    checksum_path.parent.joinpath(*path.split("/")), hash_
                )

    def items(self):
        return self._file_hashes.items()

    def __len__(self):
        return len(self._file_hashes)

    def iteratively_verify(self):
        """
        Lazily yield each file and whether it matches the known checksum.

        :rtype: [(Path, bool)]
        """
        for path, hash_ in self.items():
            calculated_hash = self._checksum(path)
            yield path, calculated_hash == hash_

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            # pylint 1.6.4 isn't smart enough to know that this is protected access of the same class
            # pylint: disable=protected-access
            return self._file_hashes == other._file_hashes

        return False

    def __hash__(self) -> int:
        return hash(self._file_hashes)

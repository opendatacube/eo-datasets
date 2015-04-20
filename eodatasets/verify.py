import hashlib
import binascii
from pathlib import Path


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

    return m.digest().encode('hex')


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
        hash = calculate_file_hash(file_path)

        self.file_hashes.append((Path(file_path), hash))

    def write(self, output_file):
        output_file = Path(output_file)
        with output_file.open('w') as f:
            f.writelines((u'{0}\t{1}\n'.format(str(hash_), str(filename.relative_to(output_file.parent)))
                          for filename, hash_ in self.file_hashes))

# coding=utf-8
from __future__ import absolute_import

import hashlib
import unittest

from eodatasets3 import verify
from tests import write_files


class VerifyTests(unittest.TestCase):
    def test_checksum(self):
        d = write_files({"test1.txt": "test"})

        test_file = d.joinpath("test1.txt")

        sha1_hash = verify.calculate_file_hash(test_file)
        self.assertEqual(sha1_hash, "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3")

        md5_hash = verify.calculate_file_hash(test_file, hash_fn=hashlib.md5)
        self.assertEqual(md5_hash, "098f6bcd4621d373cade4e832627b4f6")

        crc32_checksum = verify.calculate_file_crc32(test_file)
        self.assertEqual(crc32_checksum, "d87f7e0c")

    def test_package_checksum(self):
        d = write_files(
            {
                "test1.txt": "test",
                "package": {"test2.txt": "test2", "test3.txt": "test3"},
            }
        )

        c = verify.PackageChecksum()

        c.add_file(d.joinpath("test1.txt"))
        c.add_file(d.joinpath("package", "test3.txt"))
        c.add_file(d.joinpath("package", "test2.txt").absolute())

        checksums_file = d.joinpath("package.sha1")
        c.write(checksums_file)

        with checksums_file.open("r") as f:
            doc = f.read()

        # One (hash, file) per line separated by a tab.
        #  - File paths must be relative to the checksum file.
        #  - Output in filename alphabetical order.
        self.assertEqual(
            """109f4b3c50d7b0df729d299bc6f8e9ef9066971f\tpackage/test2.txt
3ebfa301dc59196f18593c45e519287a23297589\tpackage/test3.txt
a94a8fe5ccb19ba61c4c0873d391e987982fbbd3\ttest1.txt
""",
            doc,
        )

        # After dumping to a file, read()'ing from the file should give us identical values.
        c2 = verify.PackageChecksum()
        c2.read(checksums_file)
        original_items = set(c.items())
        loaded_items = set(c2.items())
        assert original_items == loaded_items
        assert c == c2
        # ... and a sanity check of our equals method:
        assert c != verify.PackageChecksum()

        # Verification should succeed:
        verification_results = set(c2.iteratively_verify())
        expected_verification = {
            (d.joinpath("test1.txt").absolute(), True),
            (d.joinpath("package", "test3.txt").absolute(), True),
            (d.joinpath("package", "test2.txt").absolute(), True),
        }
        assert expected_verification == verification_results

        # Corrupt a file, and expect it to fail verification.
        with d.joinpath("package", "test3.txt").open("w") as f:
            f.write("Deliberate corruption!")

        expected_verification = {
            (d.joinpath("test1.txt").absolute(), True),
            (d.joinpath("package", "test3.txt").absolute(), False),
            (d.joinpath("package", "test2.txt").absolute(), True),
        }
        verification_results = set(c2.iteratively_verify())
        assert expected_verification == verification_results

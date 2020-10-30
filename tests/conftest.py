from tests.common import format_doc_diffs


def pytest_assertrepr_compare(op, left, right):
    """
    Custom pytest error messages for large documents.

    The default pytest dict==dict error messages are unreadable for
    nested document-like dicts. (Such as our json and yaml docs!)

    We just want to know which fields differ.

    (this function is stolen from Datacube Explorer because it's neat)
    """

    def is_a_doc(o: object):
        """
        Is it a dict that's not printable on one line?
        """
        return isinstance(o, dict) and len(repr(o)) > 88

    if is_a_doc(left) and is_a_doc(right) and op == "==":
        return format_doc_diffs(left, right)

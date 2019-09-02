from eodatasets3.utils import SimpleUrl


def test_simpleurls():
    base = SimpleUrl("s3://foo/bar/baz")

    assert base.parent == "s3://foo/bar"

    assert (base / "hello.txt") == "s3://foo/bar/baz/hello.txt"
    assert (base / "/hello.txt") == "s3://foo/bar/baz/hello.txt"
    assert (
        SimpleUrl("s3://foo/bar/baz/") / "/hello.txt"
    ) == "s3://foo/bar/baz/hello.txt"
    assert (base / "hello.txt").parent == base

import tempfile
from pathlib import Path

from eodatasets3.images import FileWrite


def test_thumbnail_bitflag(input_uint8_tif: Path):
    writer = FileWrite()

    outfile = Path(tempfile.gettempdir()) / "test-new.jpg"

    writer.create_thumbnail_bitflag(
        input_uint8_tif,
        Path(outfile),
        128
        )

    assert(Path(outfile).is_file())

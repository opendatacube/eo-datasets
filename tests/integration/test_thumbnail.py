import tempfile
from pathlib import Path

from eodatasets3.images import FileWrite

from . import assert_image


def test_thumbnail_bitflag(input_uint8_tif: Path):
    writer = FileWrite()

    outfile = Path(tempfile.gettempdir()) / "test-bitflag.jpg"

    water = 128

    writer.create_thumbnail_singleband(input_uint8_tif, Path(outfile), bit=water)

    assert_image(outfile, bands=3)


def test_thumbnail_lookuptable(input_uint8_tif_2: Path):
    writer = FileWrite()

    outfile = Path(tempfile.gettempdir()) / "test-lookuptable.jpg"

    wofs_lookup = {
        0: [150, 150, 110],  # dry
        1: [255, 255, 255],  # nodata,
        16: [119, 104, 87],  # terrain
        32: [89, 88, 86],  # cloud_shadow
        64: [216, 215, 214],  # cloud
        80: [242, 220, 180],  # cloudy terrain
        128: [79, 129, 189],  # water
        160: [51, 82, 119],  # shady water
        192: [186, 211, 242],  # cloudy water
    }

    writer.create_thumbnail_singleband(
        input_uint8_tif_2, Path(outfile), lookup_table=wofs_lookup
    )

    assert_image(outfile, bands=3)

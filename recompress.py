#!/usr/bin/env python
import tarfile
import tempfile
from os.path import join as pjoin
from pathlib import Path
from typing import List

import click
import rasterio

from wagl.data import write_img
from wagl.geobox import GriddedGeoBox

CHUNKS = (512, 512)


def _do_conversion(tar_path: Path, output_tar_path: Path, aggression=9):
    """
    Convert a USGS Collection-1 *.tar.gz to a .tar
    The supplied data comes stream compressed, meaning a file
    member within a tar needs to be fully decompressed in order
    to then access the data.
    The proposal here is to compress the imagery internally using
    GDAL, thus allowing better compression ratios and read speed.
    The imagery will also be tiled (compressed in blocks).
    Sample tests indicated that (256, 256) compressed better than
    (512, 512) or (1024, 1024).

    Compression settings:
        * Deflate
        * Aggression 9
        * Chunks (256, 256)
    """
    write_options = {
        'compress': 'deflate',
        'zlevel': aggression,
        'blockxsize': CHUNKS[1],
        'blockysize': CHUNKS[0],
        'tiled': 'yes'
    }

    outdir: Path = output_tar_path.parent
    if not outdir.exists():
        outdir.mkdir(parents=True)

    with tempfile.TemporaryDirectory(prefix='.extract-', dir=str(outdir)) as tmpdir:
        tmp_out_tar = Path(tmpdir).joinpath(output_tar_path.name)

        with tarfile.open(str(tar_path), 'r') as targz:
            with tarfile.open(tmp_out_tar, 'w') as out_tar:
                members: List[tarfile.TarInfo] = targz.getmembers()
                with click.progressbar(label=tar_path.name,
                                       length=sum(member.size for member in members)) as progress:
                    fileno = 0
                    for member in members:
                        fileno += 1
                        progress.label = f"{tar_path.name} ({fileno:2d}/{len(members)})"

                        tmp_fname = pjoin(tmpdir, member.name)

                        # Recompress any TIFs, copy other files verbatum.
                        if member.name.upper().endswith('.TIF'):
                            with rasterio.open(targz.extractfile(member)) as ds:
                                geobox = GriddedGeoBox.from_dataset(ds)
                                write_img(
                                    ds.read(1),
                                    tmp_fname,
                                    geobox=geobox,
                                    options=write_options,
                                )
                        else:
                            # process the text files
                            targz.extract(member, tmpdir)

                        # add the file to the new tar
                        out_tar.add(tmp_fname, member.name)
                        progress.update(member.size)
            # Match the lower r/w permission bits to the output folder. Temp directories default to 700 otherwise.
            tmp_out_tar.chmod(outdir.stat().st_mode & 0o777)
            # Our output tar is complete. Move it into place.
            tmp_out_tar.rename(output_tar_path)


@click.command()
@click.option("--outdir", type=click.Path(file_okay=False, writable=True),
              help="The base output directory.", required=True)
@click.option("--aggression", type=click.IntRange(0, 9), default=9,
              help="Deflate aggression value.")
@click.argument("files", nargs=-1, type=click.Path(exists=True, readable=True))
def main(files: List[str], outdir: str, aggression: int):
    """
    Main level.
    """
    output_path = Path(outdir)
    for tar_file in files:
        tar_path = Path(tar_file)
        output_tar_path = _calculate_out_path(output_path, tar_path)
        _do_conversion(tar_path, output_tar_path, aggression)


def _calculate_out_path(out_path: Path, path: Path) -> Path:
    """
    >>> i = Path('/test/in/l1-data/USGS/L1/C1/092_091/LT50920911991126/LT05_L1GS_092091_19910506_20170126_01_T2.tar.gz')
    >>> o = Path('/test/dir/out')
    >>> _calculate_out_path(o, i).as_posix()
    Path('/test/dir/out/L1/Landsat/C1/092_091/LT50920911991126/LT05_L1GS_092091_19910506_20170126_01_T2.tar')
    """
    if 'USGS' not in path.parts:
        raise ValueError(
            "Expected AODH input path structure, "
            "eg: /AODH/USGS/L1/Landsat/C1/092_091/LT50920911991126/LT05_L1GS_092091_19910506_20170126_01_T2.tar.gz"
        )
    return out_path.joinpath(*path.parts[path.parts.index('USGS') + 1:-1], path.stem)


if __name__ == '__main__':
    main()

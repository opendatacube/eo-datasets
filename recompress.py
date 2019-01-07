#!/usr/bin/env python

from os.path import join as pjoin
from pathlib import Path
import tempfile
import tarfile
import zipfile
import click
import rasterio

from wagl.data import write_img
from wagl.geobox import GriddedGeoBox

PREFIX = 'tar:{}!'
CHUNKS = (512, 512)


def conversion(fname, out_fname, aggression=9):
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
    options = {
        'compress': 'deflate',
        'zlevel': aggression,
        'blockxsize': CHUNKS[1],
        'blockysize': CHUNKS[0],
        'tiled': 'yes'
    }

    outdir = out_fname.parent
    if not outdir.exists():
        outdir.mkdir(parents=True)

    with tempfile.TemporaryDirectory(prefix='extract-', dir=str(outdir)) as tmpdir:
        with tarfile.open(str(fname), 'r') as targz:
            with tarfile.open(str(out_fname), 'w') as out_tar:
                # with tarfile.open(str(out_fname), 'w:gz', compresslevel=0) as out_tar:
                # with zipfile.ZipFile(str(out_fname), 'w') as out_zip:
                for member in targz.getmembers():
                    tmp_fname = pjoin(tmpdir, member.name)

                    if member.name.endswith('.TIF'):
                        # process imagery
                        with rasterio.open(pjoin(PREFIX.format(fname), member.name)) as ds:
                            geobox = GriddedGeoBox.from_dataset(ds)
                            write_img(ds.read(1), tmp_fname, geobox=geobox,
                                      options=options)
                    else:
                        # process the text files
                        targz.extract(member, tmpdir)

                    # add the file to the new tar
                    out_tar.add(tmp_fname, member.name)
                    # out_zip.write(tmp_fname, member.name)


@click.command()
@click.option("--filename", type=click.Path(exists=True, readable=True),
              help="The input file to convert.")
@click.option("--outdir", type=click.Path(file_okay=False, writable=True),
              help="The base output directory.")
@click.option("--aggression", type=click.IntRange(0, 9), default=9,
              help="Deflate aggression value.")
def main(filename, outdir, aggression):
    """
    Main level.
    """
    filename = Path(filename)
    outdir = Path(outdir)
    parts = filename.parts
    idx = parts.index('USGS') + 1
    # tar fname
    out_fname = outdir.joinpath(*parts[idx:-1], filename.stem)
    # tar.gz fname
    # out_fname = outdir.joinpath(*parts[idx:-1], filename.name)
    # zip fname
    # out_fname = outdir.joinpath(*parts[idx:-1],
    #                             Path(Path(filename.stem).stem).with_suffix('.zip'))
    conversion(filename, out_fname, aggression)


if __name__ == '__main__':
    main()

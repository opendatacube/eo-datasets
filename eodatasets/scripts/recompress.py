#!/usr/bin/env python
import tarfile
import tempfile
from pathlib import Path
from typing import List
import numpy

import click
import rasterio

_PREDICTOR_TABLE = {
    'int8': 2,
    'uint8': 2,
    'int16': 2,
    'uint16': 2,
    'int32': 2,
    'uint32': 2,
    'int64': 2,
    'uint64': 2,
    'float32': 3,
    'float64': 3
}


def repackage_tar(tar_path: Path,
                  output_tar_path: Path,
                  zlevel=9,
                  block_size=(512, 512)):
    """
    Repackage a USGS Collection-1 tar for faster read access.

    It comes as a *.tar.gz with uncompressed tifs, which Josh's tests have found to be too slow to read.
    We compress the inner tifs and store as an uncompressed tar.

    The imagery will also be tiled (compressed in blocks), at size 512x512: a size agreed to
    between Kirill and Josh.
    """
    blocksize_y, blocksize_x = block_size
    write_options = {
        'compress': 'deflate',
        'zlevel': zlevel,
        'blockxsize': blocksize_x,
        'blockysize': blocksize_y,
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

                        tmp_fname = Path(tmpdir) / member.name

                        # Recompress any TIFs, copy other files verbatim.
                        if member.name.lower().endswith('.tif'):
                            with rasterio.open(targz.extractfile(member)) as ds:
                                write_img(
                                    ds,
                                    tmp_fname,
                                    options=write_options,
                                )
                        else:
                            # Copy unchanged into target (typically the text/metadata files).
                            targz.extract(member, tmpdir)

                        # add the file to the new tar
                        out_tar.add(tmp_fname, member.name)
                        tmp_fname.unlink()
                        progress.update(member.size)
            # Match the lower r/w permission bits to the output folder.
            # (Temp directories default to 700 otherwise.)
            tmp_out_tar.chmod(outdir.stat().st_mode & 0o777)
            # Our output tar is complete. Move it into place.
            tmp_out_tar.rename(output_tar_path)


def write_img(ds: rasterio.DatasetReader,
              path: Path,
              options=None,
              ):
    """
    Writes a 2D/3D image to disk using rasterio.
    """
    array = ds.read(1)
    # Get the datatype of the array
    dtype = array.dtype.name

    # Check for excluded datatypes
    if dtype in ('int64', 'int8', 'uint64'):
        msg = "Datatype not supported: {dt}".format(dt=dtype)
        raise TypeError(msg)

    # convert any bools to uin8
    if dtype == 'bool':
        array = numpy.uint8(array)
        dtype = 'uint8'

    ndims = array.ndim
    dims = array.shape

    # Get the (z, y, x) dimensions (assuming BSQ interleave)
    if ndims == 2:
        samples = dims[1]
        lines = dims[0]
        bands = 1
    elif ndims == 3:
        samples = dims[2]
        lines = dims[1]
        bands = dims[0]
    else:
        raise RuntimeError(f'Input array is {ndims} dimensions (2 or 3 is supported)')

    # If we have a geobox, then retrieve the geotransform and projection
    transform = ds.transform
    projection = ds.crs

    # compression predictor choices
    kwargs = {
        'count': bands,
        'width': samples,
        'height': lines,
        'crs': projection,
        'transform': transform,
        'dtype': dtype,
        'driver': 'GTiff',
        'predictor': _PREDICTOR_TABLE[dtype]
    }

    # the user can override any derived blocksizes by supplying `options`
    if options is not None:
        for key in options:
            kwargs[key] = options[key]

    with rasterio.open(path, 'w', **kwargs) as outds:
        if bands == 1:
            outds.write(array, 1)
        else:
            for i in range(bands):
                outds.write(array[i], i + 1)


@click.command()
@click.option("--outbase", type=click.Path(file_okay=False, writable=True),
              help="The base output directory.", required=True)
@click.option("--zlevel", type=click.IntRange(0, 9), default=9,
              help="Deflate compression level.")
@click.argument("files", nargs=-1, type=click.Path(exists=True, readable=True))
def main(files: List[str], outbase: str, zlevel: int):
    """
    Repackage USGS L1 tar files for faster read access.
    """
    base_output = Path(outbase)
    for tar_file in files:
        tar_path = Path(tar_file)
        output_tar_path = _calculate_out_path(base_output, tar_path)
        repackage_tar(tar_path, output_tar_path, zlevel=zlevel)


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

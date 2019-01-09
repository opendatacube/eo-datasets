#!/usr/bin/env python
import tarfile
import tempfile
from pathlib import Path
from typing import List

import click
import numpy
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


def repackage_tar(
        tar_path: Path,
        output_tar_path: Path,
        **compress_args,
):
    """
    Repackage a USGS Collection-1 tar for faster read access.

    It comes as a *.tar.gz with uncompressed tifs, which Josh's tests have found to be too slow to read.
    We compress the inner tifs and store as an uncompressed tar.

    """

    outdir: Path = output_tar_path.parent
    outdir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix='.extract-', dir=str(outdir)) as tmpdir:
        tmp_out_tar = Path(tmpdir).joinpath(output_tar_path.name)

        with tarfile.open(str(tar_path), 'r') as in_tar, tarfile.open(tmp_out_tar, 'w') as out_tar:
            members: List[tarfile.TarInfo] = in_tar.getmembers()

            _reorder_tar_members(members, tar_path.name)

            with click.progressbar(label=tar_path.name,
                                   length=sum(member.size for member in members)) as progress:
                fileno = 0
                for member in members:
                    fileno += 1
                    progress.label = f"{tar_path.name} ({fileno:2d}/{len(members)})"

                    tmp_fname = Path(tmpdir) / member.name

                    # Recompress any TIFs, copy other files verbatim.
                    if member.name.lower().endswith('.tif'):
                        _recompress_image(in_tar.extractfile(member), tmp_fname, **compress_args)
                    else:
                        # Copy unchanged into target (typically the text/metadata files).
                        in_tar.extract(member, tmpdir)

                    # add the file to the new tar
                    out_tar.add(tmp_fname, member.name)
                    tmp_fname.unlink()
                    progress.update(member.size)
            # Match the lower r/w permission bits to the output folder.
            # (Temp directories default to 700 otherwise.)
            tmp_out_tar.chmod(outdir.stat().st_mode & 0o777)
            # Our output tar is complete. Move it into place.
            tmp_out_tar.rename(output_tar_path)


def _reorder_tar_members(members: List[tarfile.TarInfo], identifier: str):
    """
    Put the (tiny) MTL file at the beginning of the tar so that it's quick to access.
    """
    # Find MTL
    for i, member in enumerate(members):
        if '_MTL' in member.path:
            mtl_index = i
            break
    else:
        formatted_members = '\n\t'.join(m.name for m in members)
        raise ValueError(f"No MTL file found in package {identifier}. Have:\n\t{formatted_members}")

    # Move to front
    mtl_item = members.pop(mtl_index)
    members.insert(0, mtl_item)


def _recompress_image(
        fp,
        output_path: Path,
        zlevel=9,
        block_size=(512, 512),
):
    """
    Read an image from given file pointer, and write as compressed GTIFF.
    """
    with rasterio.open(fp) as ds:
        ds: rasterio.DatasetReader
        blocksize_y, blocksize_x = block_size

        if len(ds.indexes) != 1:
            raise ValueError(f"Expecting one-band-per-tif input (USGS packages). "
                             f"Input has multiple layers {repr(ds.indexes)}")
        array: numpy.ndarray = ds.read(1)

        profile = ds.profile
        profile.update(
            driver='GTiff',
            predictor=_PREDICTOR_TABLE[array.dtype.name],
            compress='deflate',
            zlevel=zlevel,
            blockxsize=blocksize_x,
            blockysize=blocksize_y,
            tiled='yes',
        )

        with rasterio.open(output_path, 'w', **profile) as outds:
            outds.write(array, 1)
            # Copy gdal metadata
            outds.update_tags(**ds.tags())
            outds.update_tags(1, **ds.tags(1))


@click.command()
@click.option("--outbase", type=click.Path(file_okay=False, writable=True),
              help="The base output directory.", required=True)
@click.option("--zlevel", type=click.IntRange(0, 9), default=9,
              help="Deflate compression level.")
@click.option("--block-size", type=int, default=512,
              help="Compression block size (both x and y)")
@click.argument("files", nargs=-1, type=click.Path(exists=True, readable=True))
def main(files: List[str], outbase: str, zlevel: int, block_size: int):
    """
    Repackage USGS L1 tar files for faster read access.
    """
    base_output = Path(outbase)
    for tar_file in files:
        tar_path = Path(tar_file)
        output_tar_path = _calculate_out_path(base_output, tar_path)
        repackage_tar(
            tar_path,
            output_tar_path,
            zlevel=zlevel,
            block_size=(block_size, block_size),
        )


def _calculate_out_path(out_path: Path, path: Path) -> Path:
    """
    >>> i = Path('/test/in/l1-data/USGS/L1/C1/092_091/LT50920911991126/LT05_L1GS_092091_19910506_20170126_01_T2.tar.gz')
    >>> o = Path('/test/dir/out')
    >>> _calculate_out_path(o, i).as_posix()
    '/test/dir/out/L1/C1/092_091/LT50920911991126/LT05_L1GS_092091_19910506_20170126_01_T2.tar'
    """
    if 'USGS' not in path.parts:
        raise ValueError(
            "Expected AODH input path structure, "
            "eg: /AODH/USGS/L1/Landsat/C1/092_091/LT50920911991126/LT05_L1GS_092091_19910506_20170126_01_T2.tar.gz"
        )
    return out_path.joinpath(*path.parts[path.parts.index('USGS') + 1:-1], path.stem)


if __name__ == '__main__':
    main()

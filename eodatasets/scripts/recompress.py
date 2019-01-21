#!/usr/bin/env python
import copy
import io
import stat
import tarfile
import tempfile
from functools import partial
from pathlib import Path
from typing import List, Iterable, Tuple, Callable, IO

import click
import numpy
import rasterio
from click import secho

from eodatasets.verify import PackageChecksum

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

# The info of a file, and a method to open the file for reading.
ReadableMember = Tuple[tarfile.TarInfo, Callable[[], IO]]


def _create_tarinfo(path: Path, name=None) -> tarfile.TarInfo:
    """
    Create a TarInfo for the given path.

    This is based on TarFile.gettarinfo(), but doesn't need an existing tar file.
    """
    # We're avoiding convenience methods like `path.is_file()`, to minimise repeated `stat()` calls on lustre.
    s = path.stat()
    info = tarfile.TarInfo(name or path.name)

    if stat.S_ISREG(s.st_mode):
        info.size = s.st_size
        info.type = tarfile.REGTYPE
    elif stat.S_ISDIR(s.st_mode):
        info.type = tarfile.DIRTYPE
        info.size = 0
    else:
        raise NotImplementedError(
            f"Only regular files and directories are supported for extracted datasets. "
            f"({path.name} in {path.absolute().parent})"
        )

    info.mode = s.st_mode
    info.uid = s.st_uid
    info.gid = s.st_gid
    info.mtime = s.st_mtime

    if tarfile.pwd:
        try:
            info.uname = tarfile.pwd.getpwuid(info.uid)[0]
        except KeyError:
            pass
    if tarfile.grp:
        try:
            info.gname = tarfile.grp.getgrgid(info.gid)[0]
        except KeyError:
            pass
    return info


def _tar_members(in_tar: tarfile.TarFile) -> Iterable[ReadableMember]:
    """Get readable files (members) from a tar"""
    members: List[tarfile.TarInfo] = in_tar.getmembers()

    for member in members:
        # We return a lambda/callable so that the file isn't opened until it's needed.
        yield member, partial(in_tar.extractfile, member)


def _folder_members(path: Path, base_path: Path = None) -> Iterable[ReadableMember]:
    """
    Get readable files (presented as tar members) from a directory.
    """
    if not base_path:
        base_path = path

    for item in path.iterdir():
        member = _create_tarinfo(
            item,
            name=str(item.relative_to(base_path)),
        )
        if member.type == tarfile.DIRTYPE:
            yield from _folder_members(item, base_path=path)
        else:
            # We return a lambda/callable so that the file isn't opened until it's needed.
            yield member, partial(item.open, 'rb')


def repackage_tar(
        label: str,
        input_files: Iterable[ReadableMember],
        output_tar_path: Path,
        **compress_args,
):
    """
    Repackage a USGS Collection-1 tar for faster read access.

    It comes as a *.tar.gz with uncompressed tiffs, which Josh's tests have found to be too slow to read.
    We compress the inner tiffs and store as an uncompressed tar.

    """

    out_dir: Path = output_tar_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    verify = PackageChecksum()

    with tempfile.TemporaryDirectory(prefix='.extract-', dir=str(out_dir)) as tmpdir:
        tmpdir = Path(tmpdir).absolute()
        tmp_out_tar = tmpdir.joinpath(output_tar_path.name)

        with tarfile.open(tmp_out_tar, 'w') as out_tar:
            members = list(input_files)

            # Add the MTL file to the beginning of the output tar, so it can be accessed faster.
            # This slows down this repackage a little, as we're seeking/decompressing the input stream an extra time.
            _reorder_tar_members(members, label)

            with click.progressbar(label=label,
                                   length=sum(member.size for member, _ in members)) as progress:
                file_number = 0
                for member, open_member in members:
                    file_number += 1
                    progress.label = f"{label} ({file_number:2d}/{len(members)})"

                    # Recompress any TIFs, copy other files verbatim.
                    if member.name.lower().endswith('.tif'):
                        with rasterio.MemoryFile(filename=member.name) as memory_file:
                            _recompress_image(open_member(), memory_file, **compress_args)
                            new_member = copy.copy(member)
                            new_member.size = memory_file.getbuffer().nbytes
                            out_tar.addfile(new_member, memory_file)

                            # Image has been written. Seek to beginning to take a checksum.
                            memory_file.seek(0)
                            verify.add(memory_file, tmpdir / new_member.name)
                    else:
                        # Copy unchanged into target (typically the text/metadata files).
                        file_contents = open_member().read()
                        out_tar.addfile(member, io.BytesIO(file_contents))
                        verify.add(io.BytesIO(file_contents), tmpdir / member.name)
                        del file_contents

                    progress.update(member.size)

            # Append sha1 checksum file
            checksum_path = tmpdir / 'package.sha1'
            verify.write(checksum_path)
            checksum_path.chmod(0o664)
            out_tar.add(checksum_path, checksum_path.name)

            # Match the lower r/w permission bits to the output folder.
            # (Temp directories default to 700 otherwise.)
            tmp_out_tar.chmod(out_dir.stat().st_mode & 0o777)
            # Our output tar is complete. Move it into place.
            tmp_out_tar.rename(output_tar_path)


def _reorder_tar_members(members: List[ReadableMember], identifier: str):
    """
    Put the (tiny) MTL file at the beginning of the tar so that it's quick to read.
    """
    # Find MTL
    for i, (member, _) in enumerate(members):
        if '_MTL' in member.path:
            mtl_index = i
            break
    else:
        formatted_members = '\n\t'.join(m.name for m, _ in members)
        raise ValueError(f"No MTL file found in package {identifier}. Have:\n\t{formatted_members}")

    # Move to front
    mtl_item = members.pop(mtl_index)
    members.insert(0, mtl_item)


def _recompress_image(
        input_image_fp: IO,
        output_fp: rasterio.MemoryFile,
        zlevel=9,
        block_size=(512, 512),
):
    """
    Read an image from given file pointer, and write as compressed GeoTIFF.
    """
    # noinspection PyUnusedLocal
    with rasterio.open(input_image_fp) as ds:
        ds: rasterio.DatasetReader
        block_size_y, block_size_x = block_size

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
            blockxsize=block_size_x,
            blockysize=block_size_y,
            tiled=True,
        )

        with output_fp.open(**profile) as output_dataset:
            output_dataset.write(array, 1)
            # Copy gdal metadata
            output_dataset.update_tags(**ds.tags())
            output_dataset.update_tags(1, **ds.tags(1))


@click.command()
@click.option("--output-base", type=click.Path(file_okay=False, writable=True),
              help="The base output directory.", required=True)
@click.option("--zlevel", type=click.IntRange(0, 9), default=5,
              help="Deflate compression level.")
@click.option("--block-size", type=int, default=512,
              help="Compression block size (both x and y)")
@click.argument("paths", nargs=-1, type=click.Path(exists=True, readable=True))
def main(paths: List[str], output_base: str, zlevel: int, block_size: int):
    """
    Repackage USGS L1 tar files for faster read access.
    """
    base_output_path = Path(output_base)
    with rasterio.Env():
        for path in paths:
            path = Path(path)

            # Input is either a tar.gz file, or a directory containing an MTL (already extracted)
            if path.suffix.lower() == '.gz':
                with tarfile.open(str(path), 'r') as in_tar:
                    repackage_tar(
                        path.name,
                        _tar_members(in_tar),
                        _output_tar_path(base_output_path, path),
                        zlevel=zlevel,
                        block_size=(block_size, block_size),
                    )
            elif path.is_dir():
                repackage_tar(
                    path.name,
                    _folder_members(path),
                    _output_tar_path_from_directory(base_output_path, path),
                    zlevel=zlevel,
                    block_size=(block_size, block_size),
                )
            else:
                raise ValueError(f"Expected either tar.gz or a dataset folder. Got: {path}")


def _output_tar_path(base_output, input_path):
    out_path = _calculate_out_base_path(base_output, input_path)
    return out_path.with_name(out_path.stem)


def _output_tar_path_from_directory(base_output, input_path):
    mtl_files = list(input_path.glob('*_MTL.txt'))
    if not mtl_files:
        raise ValueError(f"Dataset has no mtl: {input_path}")
    if len(mtl_files) > 1:
        secho(f"WARNING: multiple MTL files in {input_path}", bold=True, color='red', err=True)
    mtl_file = mtl_files[0]
    dataset_name = mtl_file.name.replace('_MTL.txt', '')
    out_tar_path = _calculate_out_base_path(base_output, input_path) / f'{dataset_name}.tar'
    return out_tar_path


def _calculate_out_base_path(out_base: Path, path: Path) -> Path:
    if 'USGS' not in path.parts:
        raise ValueError(
            "Expected AODH input path structure, "
            "eg: /AODH/USGS/L1/Landsat/C1/092_091/LT50920911991126/LT05_L1GS_092091_19910506_20170126_01_T2.tar.gz"
        )
    # The directory structure after the "USGS" folder is recreated onto the output base folder.
    return out_base.joinpath(*path.parts[path.parts.index('USGS') + 1:-1], path.name)


def test_calculate_out_path(tmp_path: Path):
    out_base = tmp_path / 'out'

    # When input is a tar file, use the same name on output.
    path = Path('/test/in/l1-data/USGS/L1/C1/092_091/LT50920911991126/LT05_L1GS_092091_19910506_20170126_01_T2.tar.gz')
    assert_path_eq(
        out_base.joinpath('L1/C1/092_091/LT50920911991126/LT05_L1GS_092091_19910506_20170126_01_T2.tar'),
        _output_tar_path(out_base, path),
    )

    # When input is a directory, use the MTL file's name for the output.
    path = tmp_path / 'USGS/L1/092_091/LT50920911991126'
    path.mkdir(parents=True)
    mtl = path / 'LT05_L1GS_092091_19910506_20170126_01_T2_MTL.txt'
    mtl.write_text('fake mtl')
    assert_path_eq(
        out_base.joinpath('L1/092_091/LT50920911991126/LT05_L1GS_092091_19910506_20170126_01_T2.tar'),
        _output_tar_path_from_directory(out_base, path),
    )


def assert_path_eq(p1: Path, p2: Path):
    """Assert two pathlib paths are equal, with reasonable error output."""
    __tracebackhide__ = True
    # Pytest's error messages are far better for strings than Paths. It shows you the difference between them.
    s1, s2 = str(p1), str(p2)
    # And we use extra s1/s2 variables so that pytest doesn't print the expression "str()" as part of its output.
    assert s1 == s2


if __name__ == '__main__':
    main()

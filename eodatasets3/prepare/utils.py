import tarfile

import re
from pathlib import Path
from typing import Tuple, Dict, Generator, Iterable, Union, Callable

MTL_PAIRS_RE = re.compile(r"(\w+)\s=\s(.*)")


def get_mtl_content(
    acquisition_path: Path, root_element="l1_metadata_file"
) -> Tuple[Dict, str]:
    """
    Find MTL file for the given path. It could be a directory or a tar file.

    It will return the MTL parsed as a dict and its filename.
    """

    def iter_tar_members(tp: tarfile.TarFile) -> Generator[tarfile.TarInfo, None, None]:
        """
        This is a lazy alternative to TarInfo.getmembers() that only reads one tar item at a time.

        We're reading the MTL file, which is almost always the first entry in the tar, and then
        closing it, so we're avoiding skipping through the entirety of the tar.
        """
        member = tp.next()
        while member is not None:
            yield member
            member = tp.next()

    if not acquisition_path.exists():
        raise RuntimeError("Missing path '{}'".format(acquisition_path))

    if acquisition_path.is_file() and tarfile.is_tarfile(str(acquisition_path)):
        with tarfile.open(str(acquisition_path), "r") as tp:
            for member in iter_tar_members(tp):
                if "_MTL" in member.name:
                    with tp.extractfile(member) as fp:
                        return read_mtl(fp), member.name
            else:
                raise RuntimeError(
                    "MTL file not found in {}".format(str(acquisition_path))
                )

    else:
        paths = list(acquisition_path.rglob("*_MTL.txt"))
        if not paths:
            raise RuntimeError("No MTL file")
        if len(paths) > 1:
            raise RuntimeError(
                f"Multiple MTL files found in given acq path {acquisition_path}"
            )
        [path] = paths
        with path.open("r") as fp:
            return read_mtl(fp, root_element), path.name


def read_mtl(fp: Iterable[Union[str, bytes]], root_element="l1_metadata_file") -> Dict:
    def _parse_value(s: str) -> Union[int, float, str]:
        """
        >>> _parse_value("asdf")
        'asdf'
        >>> _parse_value("123")
        123
        >>> _parse_value("3.14")
        3.14
        """
        s = s.strip('"')
        for parser in [int, float]:
            try:
                return parser(s)
            except ValueError:
                pass
        return s

    def _parse_group(
        lines: Iterable[Union[str, bytes]],
        key_transform: Callable[[str], str] = lambda s: s.lower(),
    ) -> dict:

        tree = {}

        for line in lines:
            # If line is bytes-like convert to str
            if isinstance(line, bytes):
                line = line.decode("utf-8")
            match = MTL_PAIRS_RE.findall(line)
            if match:
                key, value = match[0]
                if key == "GROUP":
                    tree[key_transform(value)] = _parse_group(lines)
                elif key == "END_GROUP":
                    break
                else:
                    tree[key_transform(key)] = _parse_value(value)
        return tree

    tree = _parse_group(fp)
    return tree[root_element]


def _iter_bands_paths(mtl_doc: Dict) -> Generator[Tuple[str, str], None, None]:
    prefix = "file_name_band_"
    for name, filepath in mtl_doc["product_metadata"].items():
        if not name.startswith(prefix):
            continue
        usgs_band_id = name[len(prefix) :]
        yield usgs_band_id, filepath

"""
Prepare eo3 metadata for USGS Landsat Level 1 data.

Input dataset paths can be directories or tar files.
"""

import logging
import os
import re
import tarfile
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union, Iterable, Dict, Tuple, Callable, Generator

import click
import rasterio
from eodatasets3 import serialise, utils, DatasetAssembler, IfExists
from eodatasets3.model import FileFormat
from eodatasets3.ui import PathPath

_COPYABLE_MTL_FIELDS = [
    (
        "metadata_file_info",
        (
            "landsat_scene_id",
            "landsat_product_id",
            "station_id",
            "processing_software_version",
        ),
    ),
    (
        "product_metadata",
        ("data_type", "ephemeris_type", "wrs_path", "wrs_row", "collection_category"),
    ),
    (
        "image_attributes",
        (
            "ground_control_points_version",
            "ground_control_points_model",
            "geometric_rmse_model_x",
            "geometric_rmse_model_y",
            "ground_control_points_verify",
            "geometric_rmse_verify",
        ),
    ),
    ("projection_parameters", ("scan_gap_interpolation",)),
]

# Static namespace to generate uuids for datacube indexing
USGS_UUID_NAMESPACE = uuid.UUID("276af61d-99f8-4aa3-b2fb-d7df68c5e28f")

LANDSAT_OLI_TIRS_BAND_ALIASES = {
    "1": "coastal_aerosol",
    "2": "blue",
    "3": "green",
    "4": "red",
    "5": "nir",
    "6": "swir_1",
    "7": "swir_2",
    "8": "panchromatic",
    "9": "cirrus",
    "10": "lwir_1",
    "11": "lwir_2",
    "quality": "quality",
}

LANDSAT_xTM_BAND_ALIASES = {
    "1": "blue",
    "2": "green",
    "3": "red",
    "4": "nir",
    "5": "swir_1",
    "6": "tir",
    "6_vcid_1": "tir_1",
    "6_vcid_2": "tir_2",
    "7": "swir_2",
    "8": "panchromatic",
    "quality": "quality",
}

MTL_PAIRS_RE = re.compile(r"(\w+)\s=\s(.*)")


def get_band_alias_mappings(sat: str, instrument: str) -> Dict[str, str]:
    """
    To load the band_names for referencing either LANDSAT8 or LANDSAT7 or LANDSAT5 bands
    Landsat7 and Landsat5 have same band names

    >>> get_band_alias_mappings('landsat-8', 'OLI_TIRS') == LANDSAT_OLI_TIRS_BAND_ALIASES
    True
    >>> get_band_alias_mappings('landsat-8', 'OLI') == LANDSAT_OLI_TIRS_BAND_ALIASES
    True
    >>> get_band_alias_mappings('landsat-5', 'TM') == LANDSAT_xTM_BAND_ALIASES
    True
    >>> get_band_alias_mappings('landsat-5', 'TM') == LANDSAT_xTM_BAND_ALIASES
    True
    >>> get_band_alias_mappings('aqua', 'MODIS') == LANDSAT_xTM_BAND_ALIASES
    Traceback (most recent call last):
    ...
    NotImplementedError: Unexpected satellite. Only landsat handled currently. Got 'aqua'
    >>> get_band_alias_mappings('landsat-5', 'MSS') == LANDSAT_xTM_BAND_ALIASES
    Traceback (most recent call last):
    ...
    NotImplementedError: Landsat version not yet supported: 'landsat-5', 'MSS'
    """

    if not sat.startswith("landsat-"):
        raise NotImplementedError(
            f"Unexpected satellite. Only landsat handled currently. Got {sat!r}"
        )
    landsat_number = int(sat.split("-")[1])

    if landsat_number == 8:
        return LANDSAT_OLI_TIRS_BAND_ALIASES
    if landsat_number in (4, 5, 7) and instrument.endswith("TM"):
        return LANDSAT_xTM_BAND_ALIASES

    raise NotImplementedError(
        f"Landsat version not yet supported: {sat!r}, {instrument!r}"
    )


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


def prepare_and_write(
    ds_path: Path,
    output_yaml_path: Path,
    source_telemetry: Path = None,
    # TODO: Can we infer producer automatically? This is bound to cause mistakes othewise
    producer="usgs.gov",
) -> Tuple[uuid.UUID, Path]:
    """
    Prepare an eo3 metadata file for a Level1 dataset.

    Input dataset path can be a folder or a tar file.
    """
    mtl_doc, mtl_filename = get_mtl_content(ds_path)
    if not mtl_doc:
        raise ValueError(f"No MTL file found for {ds_path}")

    usgs_collection_number = mtl_doc["metadata_file_info"].get("collection_number")
    if usgs_collection_number is None:
        raise NotImplementedError(
            "Dataset has no collection number: pre-collection data is not supported."
        )

    data_format = mtl_doc["product_metadata"]["output_format"]
    if data_format.upper() != "GEOTIFF":
        raise NotImplementedError(f"Only GTiff currently supported, got {data_format}")
    file_format = FileFormat.GeoTIFF

    # Assumed below.
    projection_params = mtl_doc["projection_parameters"]
    if (
        "grid_cell_size_thermal" in projection_params
        and "grid_cell_size_reflective" in projection_params
        and (
            projection_params["grid_cell_size_reflective"]
            != projection_params["grid_cell_size_thermal"]
        )
    ):
        raise NotImplementedError("reflective and thermal have different cell sizes")
    ground_sample_distance = min(
        value
        for name, value in projection_params.items()
        if name.startswith("grid_cell_size_")
    )

    with DatasetAssembler(
        metadata_path=output_yaml_path,
        dataset_location=ds_path,
        # Detministic ID based on USGS's product id (which changes when the scene is reprocessed by them)
        dataset_id=uuid.uuid5(
            USGS_UUID_NAMESPACE, mtl_doc["metadata_file_info"]["landsat_product_id"]
        ),
        naming_conventions="dea",
        if_exists=IfExists.Overwrite,
    ) as p:
        if source_telemetry:
            # Only GA's data has source telemetry...
            assert producer == "ga.gov.au"
            p.add_source_path(source_telemetry)

        p.platform = mtl_doc["product_metadata"]["spacecraft_id"]
        p.instrument = mtl_doc["product_metadata"]["sensor_id"]
        p.product_family = "level1"
        p.producer = producer
        p.datetime = "{}T{}".format(
            mtl_doc["product_metadata"]["date_acquired"],
            mtl_doc["product_metadata"]["scene_center_time"],
        )
        p.processed = mtl_doc["metadata_file_info"]["file_date"]
        p.properties["odc:file_format"] = file_format
        p.properties["eo:gsd"] = ground_sample_distance
        cloud_cover = mtl_doc["image_attributes"]["cloud_cover"]
        # Cloud cover is -1 when missing (such as TIRS-only data)
        if cloud_cover != -1:
            p.properties["eo:cloud_cover"] = cloud_cover
        p.properties["eo:sun_azimuth"] = mtl_doc["image_attributes"]["sun_azimuth"]
        p.properties["eo:sun_elevation"] = mtl_doc["image_attributes"]["sun_elevation"]
        p.properties["landsat:collection_number"] = usgs_collection_number
        for section, fields in _COPYABLE_MTL_FIELDS:
            for field in fields:
                value = mtl_doc[section].get(field)
                if value is not None:
                    p.properties[f"landsat:{field}"] = value

        p.region_code = f"{p.properties['landsat:wrs_path']:03d}{p.properties['landsat:wrs_row']:03d}"
        org_collection_number = utils.get_collection_number(
            p.producer, p.properties["landsat:collection_number"]
        )
        p.dataset_version = f"{org_collection_number}.0.{p.processed:%Y%m%d}"

        # NRT product?
        # Category is one of: T1, T2 or RT ('real time')
        if p.properties["landsat:collection_category"] == "RT":
            p.properties["odc:dataset_maturity"] = "nrt"

        band_aliases = get_band_alias_mappings(p.platform, p.instrument)
        for usgs_band_id, file_location in _iter_bands_paths(mtl_doc):
            p.note_measurement(
                band_aliases[usgs_band_id],
                file_location,
                relative_to_dataset_location=True,
            )

        p.add_accessory_file("metadata:landsat_mtl", Path(mtl_filename))

        return p.done()


@click.command(help=__doc__)
@click.option(
    "--output-base",
    help="Write metadata files into a directory instead of alongside each dataset",
    required=False,
    type=PathPath(exists=True, writable=True, dir_okay=True, file_okay=False),
)
@click.option(
    "--source",
    "source_telemetry",
    help="Path to the source telemetry data for all of the provided datasets"
    "(either the folder or metadata file)",
    required=False,
    type=PathPath(exists=True),
)
@click.option(
    "--producer",
    help="Organisation that produced the data: probably either 'ga.gov.au' or 'usgs.gov'.",
    required=False,
    default="usgs.gov",
)
@click.argument(
    "datasets", type=PathPath(exists=True, readable=True, writable=False), nargs=-1
)
@click.option(
    "--overwrite-existing/--skip-existing",
    is_flag=True,
    default=False,
    help="Overwrite if exists (otherwise skip)",
)
@click.option(
    "--newer-than",
    type=serialise.ClickDatetime(),
    default=None,
    help="Only process files newer than this date",
)
def main(
    output_base: Optional[Path],
    datasets: List[Path],
    overwrite_existing: bool,
    producer: str,
    source_telemetry: Optional[Path],
    newer_than: datetime,
):
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO
    )
    with rasterio.Env():
        for ds in datasets:
            if output_base:
                output = output_base.joinpath(
                    *utils.subfolderise(_dataset_region_code(ds))
                )
                output.mkdir(parents=True, exist_ok=True)
            else:
                # Alongside the dataset itself.
                output = ds.absolute().parent

            ds_path = _normalise_dataset_path(Path(ds).absolute())
            (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(ds)
            create_date = datetime.utcfromtimestamp(ctime)
            if newer_than and (create_date <= newer_than):
                logging.info(
                    "Creation time {} older than start date {:%Y-%m-%d %H:%M} ...SKIPPING {}".format(
                        newer_than - create_date, newer_than, ds_path.name
                    )
                )
                continue

            logging.info("Processing %s", ds_path)
            output_yaml = output / "{}.odc-metadata.yaml".format(_dataset_name(ds_path))

            if output_yaml.exists():
                if not overwrite_existing:
                    logging.info("Output exists: skipping. %s", output_yaml)
                    continue

                logging.info("Output exists: overwriting %s", output_yaml)

            output_uuid, output_path = prepare_and_write(
                ds_path,
                output_yaml,
                producer=producer,
                source_telemetry=source_telemetry,
            )
            logging.info("Wrote dataset %s to %s", output_uuid, output_path)


def _normalise_dataset_path(input_path: Path) -> Path:
    """
    Dataset path should be either the direct imagery folder (mtl+bands) or a tar path.

    Translate other inputs (example: the MTL path) to one of the two.

    >>> tmppath = Path(tempfile.mkdtemp())
    >>> ds_path = tmppath.joinpath('LE07_L1GT_104078_20131209_20161119_01_T1')
    >>> ds_path.mkdir()
    >>> mtl_path = ds_path / 'LC08_L1TP_090084_20160121_20170405_01_T1_MTL.txt'
    >>> mtl_path.write_text('<mtl content>')
    13
    >>> _normalise_dataset_path(ds_path).relative_to(tmppath).as_posix()
    'LE07_L1GT_104078_20131209_20161119_01_T1'
    >>> _normalise_dataset_path(mtl_path).relative_to(tmppath).as_posix()
    'LE07_L1GT_104078_20131209_20161119_01_T1'
    >>> tar_path = tmppath / 'LS_L1GT.tar.gz'
    >>> tar_path.write_text('fake tar')
    8
    >>> _normalise_dataset_path(tar_path).relative_to(tmppath).as_posix()
    'LS_L1GT.tar.gz'
    >>> _normalise_dataset_path(Path(tempfile.mkdtemp()))
    Traceback (most recent call last):
    ...
    ValueError: No MTL files within input path .... Not a dataset?
    """
    input_path = normalise_nci_symlinks(input_path)
    if input_path.is_file():
        if ".tar" in input_path.suffixes:
            return input_path
        input_path = input_path.parent

    mtl_files = list(input_path.rglob("*_MTL*"))
    if not mtl_files:
        raise ValueError(
            "No MTL files within input path '{}'. Not a dataset?".format(input_path)
        )
    if len(mtl_files) > 1:
        raise ValueError(
            "Multiple MTL files in a single dataset (got path: {})".format(input_path)
        )
    return input_path


def normalise_nci_symlinks(input_path: Path) -> Path:
    """
    If it's an NCI lustre path, always use the symlink (`/g/data`) rather than specific drives (eg. `/g/data2`).

    >>> normalise_nci_symlinks(Path('/g/data2/v10/some/dataset.tar')).as_posix()
    '/g/data/v10/some/dataset.tar'
    >>> normalise_nci_symlinks(Path('/g/data1a/v10/some/dataset.tar')).as_posix()
    '/g/data/v10/some/dataset.tar'
    >>> # Don't change other paths!
    >>> normalise_nci_symlinks(Path('/g/data/v10/some/dataset.tar')).as_posix()
    '/g/data/v10/some/dataset.tar'
    >>> normalise_nci_symlinks(Path('/Users/testuser/unrelated-path.yaml')).as_posix()
    '/Users/testuser/unrelated-path.yaml'
    """
    match = re.match(r"^/g/data[0-9a-z]+/(.*)", str(input_path))
    if not match:
        return input_path

    [offset] = match.groups()
    return Path("/g/data/" + offset)


def _dataset_name(ds_path: Path) -> str:
    """
    >>> _dataset_name(Path("example/LE07_L1GT_104078_20131209_20161119_01_T1.tar.gz"))
    'LE07_L1GT_104078_20131209_20161119_01_T1'
    >>> _dataset_name(Path("example/LE07_L1GT_104078_20131209_20161119_01_T1.tar"))
    'LE07_L1GT_104078_20131209_20161119_01_T1'
    >>> _dataset_name(Path("example/LE07_L1GT_104078_20131209_20161119_01_T2"))
    'LE07_L1GT_104078_20131209_20161119_01_T2'
    """
    # This is a little simpler than before :)
    return ds_path.stem.split(".")[0]


def _dataset_region_code(ds_path: Path) -> str:
    """
    >>> _dataset_region_code(Path("example/LE07_L1GT_104078_20131209_20161119_01_T1.tar.gz"))
    '104078'
    >>> _dataset_region_code(Path("example/LE07_L1GT_104078_20131209_20161119_01_T1.tar"))
    '104078'
    >>> _dataset_region_code(Path("example/LE07_L1GT_104078_20131209_20161119_01_T2"))
    '104078'
    """
    return _dataset_name(ds_path).split("_")[2]


if __name__ == "__main__":
    main()

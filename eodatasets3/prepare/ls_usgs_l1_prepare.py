"""
Ingest data from the command-line.
"""

import hashlib
import logging
import os
import re
import tarfile
import tempfile
import uuid
from datetime import datetime
from functools import partial
from pathlib import Path

import click
import pyproj
import ruamel.yaml
from osgeo import osr
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform

from eodatasets3 import serialise, utils
from eodatasets3.assemble import DatasetAssembler
from eodatasets3.model import FileFormat, resolve_absolute_offset
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
]

try:
    # flake8 doesn't recognise type hints as usage
    from typing import (
        List,
        Optional,
        Union,
        Iterable,
        Dict,
        Tuple,
        Callable,
    )  # noqa: F401
except ImportError:
    pass

# Static namespace to generate uuids for datacube indexing
USGS_UUID_NAMESPACE = uuid.UUID("276af61d-99f8-4aa3-b2fb-d7df68c5e28f")

LANDSAT_8_BANDS = [
    ("1", "coastal_aerosol"),
    ("2", "blue"),
    ("3", "green"),
    ("4", "red"),
    ("5", "nir"),
    ("6", "swir_1"),
    ("7", "swir_2"),
    ("8", "panchromatic"),
    ("9", "cirrus"),
    ("10", "lwir_1"),
    ("11", "lwir_2"),
    ("QUALITY", "quality"),
]

TIRS_ONLY = LANDSAT_8_BANDS[9:12]
OLI_ONLY = [*LANDSAT_8_BANDS[0:9], LANDSAT_8_BANDS[11]]

LANDSAT_BANDS = [
    ("1", "blue"),
    ("2", "green"),
    ("3", "red"),
    ("4", "nir"),
    ("5", "swir_1"),
    ("7", "swir_2"),
    ("QUALITY", "quality"),
]

MTL_PAIRS_RE = re.compile(r"(\w+)\s=\s(.*)")


def _parse_value(s):
    # type: (str) -> Union[int, float, str]
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


def find_in(path, s, suffix="txt"):
    # type: (Path, str, str) -> Optional[Path]
    """Recursively find any file with a certain string in its name

    Search through `path` and its children for the first occurance of a
    file with `s` in its name. Returns the path of the file or `None`.
    """

    def matches(p):
        # type: (Path) -> bool
        return s in p.name and p.name.endswith(suffix)

    if path.is_file():
        return path if matches(path) else None

    for root, _, files in os.walk(str(path)):
        for f in files:
            p = Path(root) / f
            if matches(p):
                return p
    return None


def _parse_group(lines, key_transform=lambda s: s.lower()):
    # type: (Iterable[Union[str, bytes]], Callable[[str], str]) -> dict
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


def get_coords(geo_ref_points, epsg_code):
    # type: (Dict, int) -> Dict

    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromEPSG(epsg_code)
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        lon, lat, z = t.TransformPoint(p["x"], p["y"])
        return {"lon": lon, "lat": lat}

    return {key: transform(p) for key, p in geo_ref_points.items()}


def get_satellite_band_names(sat, instrument, file_name):
    # type: (str, str, str) -> List[Tuple[str, str]]
    """
    To load the band_names for referencing either LANDSAT8 or LANDSAT7 or LANDSAT5 bands
    Landsat7 and Landsat5 have same band names
    """

    name_len = file_name.split("_")
    if sat == "LANDSAT_8":
        if instrument == "TIRS":
            sat_img = TIRS_ONLY
        elif instrument == "OLI":
            sat_img = OLI_ONLY
        else:
            sat_img = LANDSAT_8_BANDS
    elif len(name_len) > 7:
        sat_img = LANDSAT_BANDS
    else:
        sat_img = LANDSAT_BANDS[:6]
    return sat_img


def get_mtl_content(acquisition_path: Path) -> Tuple[Dict, str]:
    """
    Find MTL file; return it parsed as a dict with its filename relative to the acquisition path.
    """
    if not acquisition_path.exists():
        raise RuntimeError("Missing path '{}'".format(acquisition_path))

    if acquisition_path.is_file() and tarfile.is_tarfile(str(acquisition_path)):
        with tarfile.open(str(acquisition_path), "r") as tp:
            try:
                internal_file = next(
                    filter(lambda memb: "_MTL" in memb.name, tp.getmembers())
                )
                with tp.extractfile(internal_file) as fp:
                    return read_mtl(fp), internal_file.name
            except StopIteration:
                raise RuntimeError(
                    "MTL file not found in {}".format(str(acquisition_path))
                )
    else:
        path = find_in(acquisition_path, "MTL")
        if not path:
            raise RuntimeError("No MTL file")

        with path.open("r") as fp:
            return read_mtl(fp), path.name


def read_mtl(fp):
    return _parse_group(fp)["l1_metadata_file"]


def _file_size_bytes(path: Path) -> int:
    """
    Total file size for the given file/directory.

    >>> import tempfile
    >>> test_dir = Path(tempfile.mkdtemp())
    >>> inner_dir = test_dir.joinpath('inner')
    >>> inner_dir.mkdir()
    >>> test_file = Path(inner_dir / 'test.txt')
    >>> test_file.write_text('asdf\\n')
    5
    >>> Path(inner_dir / 'other.txt').write_text('secondary\\n')
    10
    >>> _file_size_bytes(test_file)
    5
    >>> _file_size_bytes(inner_dir)
    15
    >>> _file_size_bytes(test_dir)
    15
    """
    if path.is_file():
        return path.stat().st_size

    return sum(_file_size_bytes(p) for p in path.iterdir())


def prepare_and_write(
    ds_path: Path,
    output_yaml_path: Path,
    use_absolute_paths=False,
    write_checksum=False,
) -> Tuple[uuid.UUID, Path]:
    mtl_doc, mtl_filename = get_mtl_content(ds_path)
    if not mtl_doc:
        raise ValueError(f"No MTL file found for {ds_path}")

    collection_number = mtl_doc["metadata_file_info"].get("collection_number")

    if collection_number is None:
        raise NotImplementedError(
            "Dataset has no collection number: pre-collection data is not supported."
        )

    data_format = mtl_doc["product_metadata"]["output_format"]
    if data_format.upper() != "GEOTIFF":
        raise NotImplementedError(f"Only GTiff currently supported, got {data_format}")
    file_format = FileFormat.GeoTIFF

    # epsg_code = 32600 + mtl_doc["projection_parameters"]["utm_zone"]

    platform_id = mtl_doc["product_metadata"]["spacecraft_id"]
    sensor_id = mtl_doc["product_metadata"]["sensor_id"]
    band_mappings = get_satellite_band_names(platform_id, sensor_id, mtl_filename)
    product_id = mtl_doc["metadata_file_info"]["landsat_product_id"]

    # Assumed below.
    if (
        mtl_doc["projection_parameters"]["grid_cell_size_reflective"]
        != mtl_doc["projection_parameters"]["grid_cell_size_thermal"]
    ):
        raise NotImplementedError("reflective and thermal have different cell sizes")
    ground_sample_distance = min(
        value
        for name, value in mtl_doc["projection_parameters"].items()
        if name.startswith("grid_cell_size_")
    )

    with DatasetAssembler(
        metadata_file=output_yaml_path,
        # Detministic ID based on USGS's product id (which changes when the scene is reprocessed by them)
        dataset_id=uuid.uuid5(USGS_UUID_NAMESPACE, product_id),
        naming_conventions="dea",
    ) as p:
        p.platform = platform_id
        p.instrument = sensor_id
        p.product_family = "level1"
        p.producer = "usgs.gov"
        p.datetime = "{}T{}".format(
            mtl_doc["product_metadata"]["date_acquired"],
            mtl_doc["product_metadata"]["scene_center_time"],
        )
        p.processed = mtl_doc["metadata_file_info"]["file_date"]
        p.properties["odc:file_format"] = file_format
        p.properties["eo:gsd"] = ground_sample_distance
        p.properties["eo:cloud_cover"] = mtl_doc["image_attributes"]["cloud_cover"]
        p.properties["eo:sun_azimuth"] = mtl_doc["image_attributes"]["sun_azimuth"]
        p.properties["eo:sun_elevation"] = mtl_doc["image_attributes"]["sun_elevation"]
        p.properties["landsat:collection_number"] = collection_number

        for section, fields in _COPYABLE_MTL_FIELDS:
            s = mtl_doc[section]
            for field in fields:
                p.properties[f"landsat:{field}"] = s.get(field)

        p.region_code = f"{p.properties['landsat:wrs_path']:03d}{p.properties['landsat:wrs_row']:03d}"

        p.dataset_version = ".".join(
            (str(collection_number), "0", mtl_doc["metadata_file_info"]["file_date"])
        )

        for usgs_band_id, band_alias in band_mappings:
            p.note_measurement(
                band_alias,
                resolve_absolute_offset(
                    ds_path,
                    mtl_doc["product_metadata"][
                        "file_name_band_" + usgs_band_id.lower()
                    ],
                ),
            )

        p.add_accessory_file("metadata:landsat_mtl", Path(mtl_filename))

        return p.done()


def bbox(shape: BaseGeometry, crs: str) -> Tuple[float, float, float, float]:
    tranform_wrs84 = partial(
        pyproj.transform, pyproj.Proj(init=crs), pyproj.Proj(init="epsg:4326")
    )
    return transform(tranform_wrs84, shape).buffer(0).bounds


def _remove_nones(d: Dict) -> Dict:
    return {k: v for (k, v) in d.items() if v is not None}


def _checksum_path(base_path: Path) -> Path:
    """
    Get the checksum file path for the given dataset.

    If it's a file, we add a sibling file with '.sha1' extension

    If it's a directory, we add a 'package.sha1' file inside (existing
    dataset management scripts like dea-sync expect this)
    """
    if base_path.is_file():
        return base_path.parent / f"{base_path.name}.sha1"
    else:
        return base_path / "package.sha1"


def relative_path(basepath: Path, offset: Path) -> Path:
    """
    Get relative path (similar to web browser conventions)
    """
    # pathlib doesn't like relative-to-a-file.
    if basepath.is_file():
        basepath = basepath.parent
    return offset.relative_to(basepath)


def yaml_checkums_correctly(output_yaml: Path, data_path: Path) -> bool:
    with output_yaml.open() as yaml_f:
        logging.info("Running checksum comparison")
        # It can match any dataset in the yaml.
        for doc in ruamel.yaml.safe_load_all(yaml_f):
            yaml_sha1 = doc["checksum_sha1"]
            checksum_sha1 = hashlib.sha1(data_path.open("rb").read()).hexdigest()
            if checksum_sha1 == yaml_sha1:
                return True

    return False


@click.command(
    help="""\b
                    Prepare USGS Landsat Collection 1 data for ingestion into the Data Cube.
                    This prepare script supports only for MTL.txt metadata file
                    To Set the Path for referring the datasets -
                    Download the  Landsat scene data from Earth Explorer or GloVis into
                    'some_space_available_folder' and unpack the file.
                    For example: yourscript.py --output [Yaml- which writes datasets into this file for indexing]
                    [Path for dataset as : /home/some_space_available_folder/]"""
)
@click.option(
    "--output-base",
    help="Write output into this directory instead of with the dataset",
    required=False,
    type=PathPath(exists=True, writable=True, dir_okay=True, file_okay=False),
)
@click.argument(
    "datasets", type=PathPath(exists=True, readable=True, writable=False), nargs=-1
)
@click.option(
    "--newer-than",
    type=serialise.ClickDatetime(),
    default=None,
    help="Only prepare files newer than this date",
)
@click.option(
    "--checksum/--no-checksum",
    "check_checksum",
    help="Checksum the input dataset to confirm match",
    default=False,
)
@click.option(
    "--absolute-paths/--relative-paths",
    "force_absolute_paths",
    help="Embed absolute paths in the metadata document (not recommended)",
    default=False,
)
def main(
    output_base: Optional[Path],
    datasets: List[Path],
    check_checksum: bool,
    force_absolute_paths: bool,
    newer_than: datetime,
):
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO
    )

    for ds in datasets:
        if output_base:
            output = output_base / utils.subfolderise(_dataset_region_code(ds))
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

        logging.info("Output %s", output_yaml)
        if output_yaml.exists():
            logging.info("Output already exists %s", output_yaml)
            if check_checksum and yaml_checkums_correctly(output_yaml, ds_path):
                logging.info(
                    "Dataset preparation already done...SKIPPING %s", ds_path.name
                )
                continue

        prepare_and_write(ds_path, output_yaml, use_absolute_paths=force_absolute_paths)


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

"""
Ingest data from the command-line.
"""
from __future__ import absolute_import

import hashlib
import logging
import os
import re
import tarfile
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

import ciso8601
import click

import yaml
from osgeo import osr
from shapely.geometry import Polygon
import urllib.parse
from eodatasets import verify
from eodatasets.prepare.model import (
    Dataset,
    Product,
    FileFormat,
    Measurement,
    valid_region,
    resolve_absolute_offset,
)
from . import serialise

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
    ("6", "swir1"),
    ("7", "swir2"),
    ("8", "panchromatic"),
    ("9", "cirrus"),
    ("10", "lwir1"),
    ("11", "lwir2"),
    ("QUALITY", "quality"),
]

TIRS_ONLY = LANDSAT_8_BANDS[9:12]
OLI_ONLY = [*LANDSAT_8_BANDS[0:9], LANDSAT_8_BANDS[11]]

LANDSAT_BANDS = [
    ("1", "blue"),
    ("2", "green"),
    ("3", "red"),
    ("4", "nir"),
    ("5", "swir1"),
    ("7", "swir2"),
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


def get_mtl_content(acquisition_path):
    # type: (Path) -> Tuple[Dict, str]
    """
    Find MTL file; return it parsed as a dict with its filename.
    """
    if not acquisition_path.exists():
        raise RuntimeError("Missing path '{}'".format(acquisition_path))

    if acquisition_path.is_file() and tarfile.is_tarfile(str(acquisition_path)):
        with tarfile.open(str(acquisition_path), "r") as tp:
            try:
                internal_file = next(
                    filter(lambda memb: "_MTL" in memb.name, tp.getmembers())
                )
                filename = Path(internal_file.name).stem
                with tp.extractfile(internal_file) as fp:
                    return read_mtl(fp), filename
            except StopIteration:
                raise RuntimeError(
                    "MTL file not found in {}".format(str(acquisition_path))
                )
    else:
        path = find_in(acquisition_path, "MTL")
        if not path:
            raise RuntimeError("No MTL file")

        filename = Path(path).stem
        with path.open("r") as fp:
            return read_mtl(fp), filename


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


def prepare_dataset(base_path: Path, write_checksum: bool = True) -> Optional[Dict]:
    mtl_doc, mtl_filename = get_mtl_content(base_path)

    if not mtl_doc:
        return None

    if write_checksum:
        checksum_path = _checksum_path(base_path)
        if checksum_path.exists():
            logging.warning("Checksum path exists. Not touching it. %r", checksum_path)
        else:
            checksum = verify.PackageChecksum()
            checksum.add_file(base_path)
            checksum.write(checksum_path)

    return prepare_dataset_from_mtl(
        _file_size_bytes(base_path), mtl_doc, mtl_filename, base_path=base_path
    )


def prepare_dataset_from_mtl(
    total_size: int, mtl_doc: dict, mtl_filename: str, base_path: Optional[Path] = None
) -> dict:
    return _prepare(mtl_doc, mtl_filename, base_path).to_doc()


def _prepare(
    mtl_doc: dict, mtl_filename: str, base_path: Optional[Path] = None
) -> Dataset:
    data_format = mtl_doc["product_metadata"]["output_format"]
    if data_format.upper() != "GEOTIFF":
        raise NotImplementedError(f"Only GTiff currently supported, got {data_format}")
    file_format = FileFormat.GeoTIFF

    epsg_code = 32600 + mtl_doc["projection_parameters"]["utm_zone"]

    platform_id = mtl_doc["product_metadata"]["spacecraft_id"]
    sensor_id = mtl_doc["product_metadata"]["sensor_id"]
    band_mappings = get_satellite_band_names(platform_id, sensor_id, mtl_filename)

    product_id = mtl_doc["metadata_file_info"]["landsat_product_id"]

    bands = [
        Measurement(
            mtl_doc["product_metadata"]["file_name_band_" + band_fname.lower()],
            band_name,
            layer="1",
        )
        for band_fname, band_name in band_mappings
    ]

    # If we have a filesystem path, we can store the actual
    if base_path:
        # Mask value?
        geometry, grids = valid_region(base_path, bands, mask_value=None)
    else:
        info = mtl_doc["product_metadata"]
        geometry = Polygon(
            (
                (
                    info["corner_ul_projection_x_product"],
                    info["corner_ul_projection_y_product"],
                ),
                (
                    info["corner_ur_projection_x_product"],
                    info["corner_ur_projection_y_product"],
                ),
                (
                    info["corner_lr_projection_x_product"],
                    info["corner_lr_projection_y_product"],
                ),
                (
                    info["corner_ll_projection_x_product"],
                    info["corner_ll_projection_y_product"],
                ),
            )
        )
        grids = None

    def get_all(section: str, keys):
        return {k: mtl_doc[section][k] for k in keys if k in mtl_doc[section]}

    user_data = {
        **get_all(
            "metadata_file_info",
            (
                "landsat_scene_id",
                "landsat_product_id",
                "collection_number",
                "station_id",
                "processing_software_version",
            ),
        ),
        **get_all("product_metadata", ("data_type", "ephemeris_type")),
    }

    # Assumed below.
    if (
        mtl_doc["projection_parameters"]["grid_cell_size_reflective"]
        != mtl_doc["projection_parameters"]["grid_cell_size_thermal"]
    ):
        raise NotImplementedError("reflective and thermal have different cell sizes")

    # Generate a deterministic UUID for the level 1 dataset
    d = Dataset(
        id=uuid.uuid5(USGS_UUID_NAMESPACE, product_id),
        product=Product(
            "usgs_ls{}-{}_level1_3".format(
                platform_id[-1].lower(), sensor_id[0].lower()
            )
        ),
        datetime=ciso8601.parse_datetime(
            "{}T{}".format(
                mtl_doc["product_metadata"]["date_acquired"],
                mtl_doc["product_metadata"]["scene_center_time"],
            )
        ),
        file_format=file_format,
        crs="epsg:%s" % epsg_code,
        geometry=geometry,
        grids=grids,
        measurements={band.band: band for band in bands},
        lineage={},
        properties=_remove_nones({
            "eo:platform": platform_id.lower().replace("_", "-"),
            "eo:instrument": sensor_id,
            "eo:gsd": mtl_doc["projection_parameters"]["grid_cell_size_reflective"],
            "eo:cloud_cover": mtl_doc["image_attributes"]["cloud_cover"],
            "eo:sun_azimuth": mtl_doc["image_attributes"]["sun_azimuth"],
            "eo:sun_elevation": mtl_doc["image_attributes"]["sun_elevation"],
            'landsat:wrs_path': mtl_doc['product_metadata']['wrs_path'],
            'landsat:wrs_row': mtl_doc['product_metadata']['wrs_row'],
            "landsat:ground_control_points_model": mtl_doc["image_attributes"].get("ground_control_points_model"),
            "landsat:ground_control_points_version": mtl_doc["image_attributes"].get("ground_control_points_version"),
            "landsat:ground_control_points_verify": mtl_doc["image_attributes"].get("ground_control_points_verify"),
            "landsat:geometric_rmse_model_x": mtl_doc["image_attributes"].get("geometric_rmse_model_x"),
            "landsat:geometric_rmse_model_y": mtl_doc["image_attributes"].get("geometric_rmse_model_y"),
            "landsat:geometric_rmse_verify": mtl_doc["image_attributes"].get("geometric_rmse_verify"),
        }),
        user_data=user_data,
    )
    return d


def _remove_nones(d: Dict) -> Dict:
    return {k: v for (k, v) in d.items() if v is not None}


def _checksum_path(base_path):
    # type: (Path) -> Path
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


def relative_path(basepath, offset):
    # type: (Path, Path) -> Path
    """
    Get relative path (similar to web browser conventions)
    """
    # pathlib doesn't like relative-to-a-file.
    if basepath.is_file():
        basepath = basepath.parent
    return offset.relative_to(basepath)


def yaml_checkums_correctly(output_yaml, data_path):
    with output_yaml.open() as yaml_f:
        logging.info("Running checksum comparison")
        # It can match any dataset in the yaml.
        for doc in yaml.safe_load_all(yaml_f):
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
    "--output",
    help="Write output into this directory",
    required=True,
    type=click.Path(exists=True, writable=True, dir_okay=True, file_okay=False),
)
@click.argument(
    "datasets", type=click.Path(exists=True, readable=True, writable=False), nargs=-1
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
def main(output, datasets, check_checksum, force_absolute_paths, newer_than):
    # type: (str, List[str], bool, bool, datetime) -> None

    output = Path(output)

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO
    )

    for ds in datasets:
        ds_path = _normalise_dataset_path(Path(ds))
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
        output_yaml = output / "{}.yaml".format(_dataset_name(ds_path))

        logging.info("Output %s", output_yaml)
        if output_yaml.exists():
            logging.info("Output already exists %s", output_yaml)
            if check_checksum and yaml_checkums_correctly(output_yaml, ds_path):
                logging.info(
                    "Dataset preparation already done...SKIPPING %s", ds_path.name
                )
                continue

        prepare_and_write(ds_path, output_yaml, use_absolute_paths=force_absolute_paths)

    # delete intermediate MTL files for archive datasets in output folder
    output_mtls = list(output.rglob("*MTL.txt"))
    for mtl_path in output_mtls:
        try:
            mtl_path.unlink()
        except OSError:
            pass


def prepare_and_write(
    ds_path, output_yaml_path, use_absolute_paths=False, write_checksum=True
):
    # type: (Path, Path, bool, bool) -> None

    doc = prepare_dataset(ds_path, write_checksum=write_checksum)

    if use_absolute_paths:
        for band in doc["measurements"].values():
            band["path"] = resolve_absolute_offset(
                ds_path, band["path"], target_path=output_yaml_path
            )

    serialise.dump_yaml(output_yaml_path, doc)


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


def _dataset_name(ds_path):
    # type: (Path) -> str
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


def register_scheme(*schemes):
    """
    Register additional uri schemes as supporting relative offsets (etc), so that band/measurement paths can be
    calculated relative to the base uri.
    """
    urllib.parse.uses_netloc.extend(schemes)
    urllib.parse.uses_relative.extend(schemes)
    urllib.parse.uses_params.extend(schemes)


register_scheme('tar')

if __name__ == "__main__":
    main()

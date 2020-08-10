"""
Package WAGL HDF5 Outputs

This will convert the HDF5 file (and sibling fmask/gqa files) into
GeoTIFFS (COGs) with datacube metadata using the DEA naming conventions
for files.
"""
import contextlib
import os
import re
import sys
from datetime import timedelta, datetime
from pathlib import Path
from typing import List, Sequence, Optional, Iterable, Any, Tuple, Dict, Mapping
from uuid import UUID

import attr
import numpy
import rasterio
from affine import Affine
from boltons.iterutils import get_path, PathAccessError
from click import secho
from rasterio import DatasetReader
from rasterio.crs import CRS
from rasterio.enums import Resampling

from eodatasets3 import serialise, utils, images, DatasetAssembler
from eodatasets3.images import GridSpec
from eodatasets3.model import DatasetDoc
from eodatasets3.serialise import loads_yaml
from eodatasets3.ui import bool_style
from eodatasets3.utils import default_utc

try:
    import h5py
except ImportError:
    sys.stderr.write(
        "eodatasets3 has not been installed with the wagl extras. \n"
        "    Try `pip install eodatasets3[wagl]\n"
    )
    raise

POSSIBLE_PRODUCTS = ("nbar", "nbart", "lambertian", "sbt")
DEFAULT_PRODUCTS = ("nbar", "nbart")

_THUMBNAILS = {
    ("landsat-5", "nbar"): ("nbar:red", "nbar:green", "nbar:blue"),
    ("landsat-5", "nbart"): ("nbart:red", "nbart:green", "nbart:blue"),
    ("landsat-7", "nbar"): ("nbar:red", "nbar:green", "nbar:blue"),
    ("landsat-7", "nbart"): ("nbart:red", "nbart:green", "nbart:blue"),
    ("landsat-8", "nbar"): ("nbar:red", "nbar:green", "nbar:blue"),
    ("landsat-8", "nbart"): ("nbart:red", "nbart:green", "nbart:blue"),
}

os.environ["CPL_ZIP_ENCODING"] = "UTF-8"

FILENAME_TIF_BAND = re.compile(
    r"(?P<prefix>(?:.*_)?)(?P<band_name>B[0-9][A0-9]|B[0-9]*|B[0-9a-zA-z]*)"
    r"(?P<extension>\....)"
)
PRODUCT_SUITE_FROM_GRANULE = re.compile("(L1[GTPCS]{1,2})")


def _find_h5_paths(h5_obj: h5py.Group, dataset_class: str = "") -> List[str]:
    """
    Find all objects in a h5 of the given class, returning their path.

    (class examples: IMAGE, TABLE. SCALAR)
    """
    items = []

    def _find(name, obj):
        if obj.attrs.get("CLASS") == dataset_class:
            items.append(name)

    h5_obj.visititems(_find)
    return items


def _unpack_products(
    p: DatasetAssembler, product_list: Iterable[str], h5group: h5py.Group
) -> None:
    """
    Unpack and package the NBAR and NBART products.
    """
    # listing of all datasets of IMAGE CLASS type
    img_paths = _find_h5_paths(h5group, "IMAGE")

    for product in product_list:
        with do(f"Starting {product}", heading=True):
            for pathname in [
                p for p in img_paths if "/{}/".format(product.upper()) in p
            ]:
                with do(f"Path {pathname!r}"):
                    dataset = h5group[pathname]
                    band_name = utils.normalise_band_name(dataset.attrs["alias"])
                    write_measurement_h5(
                        p,
                        f"{product}:{band_name}",
                        dataset,
                        overview_resampling=Resampling.average,
                        file_id=_file_id(dataset),
                    )

            if (p.platform, product) in _THUMBNAILS:
                red, green, blue = _THUMBNAILS[(p.platform, product)]
                with do(f"Thumbnailing {product}"):
                    p.write_thumbnail(
                        red, green, blue, kind=product, static_stretch=(1, 3000)
                    )


def write_measurement_h5(
    p: DatasetAssembler,
    name: str,
    g: h5py.Dataset,
    overviews=images.DEFAULT_OVERVIEWS,
    overview_resampling=Resampling.nearest,
    expand_valid_data=True,
    file_id: str = None,
):
    """
    Write a measurement by copying it from a hdf5 dataset.
    """
    if hasattr(g, "chunks"):
        data = g[:]
    else:
        data = g

    p.write_measurement_numpy(
        name=name,
        array=data,
        grid_spec=images.GridSpec(
            shape=g.shape,
            transform=Affine.from_gdal(*g.attrs["geotransform"]),
            crs=CRS.from_wkt(g.attrs["crs_wkt"]),
        ),
        nodata=(g.attrs.get("no_data_value")),
        overviews=overviews,
        overview_resampling=overview_resampling,
        expand_valid_data=expand_valid_data,
        file_id=file_id,
    )


def _file_id(dataset: h5py.Dataset) -> str:
    """
    Devise a file id for the given dataset (using its attributes)

    Eg. 'band01'
    """
    # What we have to work with:
    # >>> print(repr((dataset.attrs["band_id"], dataset.attrs["band_name"], dataset.attrs["alias"])))
    # ('1', 'BAND-1', 'Blue')

    band_name = dataset.attrs["band_id"]

    # A purely numeric id needs to be formatted 'band01' according to naming conventions.
    return utils.normalise_band_name(band_name)


def _unpack_observation_attributes(
    p: DatasetAssembler,
    product_list: Iterable[str],
    h5group: h5py.Group,
    infer_datetime_range=False,
):
    """
    Unpack the angles + other supplementary datasets produced by wagl.
    Currently only the mode resolution group gets extracted.
    """
    resolution_groups = sorted(g for g in h5group.keys() if g.startswith("RES-GROUP-"))
    # Use the highest resolution as the ground sample distance.
    del p.properties["eo:gsd"]
    p.properties["eo:gsd"] = min(
        min(h5group[grp].attrs["resolution"]) for grp in resolution_groups
    )

    if len(resolution_groups) not in (1, 2):
        raise NotImplementedError(
            f"Unexpected set of res-groups. "
            f"Expected either two (with pan) or one (without pan), "
            f"got {resolution_groups!r}"
        )
    # Res groups are ordered in descending resolution, so res-group-0 is the highest resolution.
    # (ie. res-group-0 in landsat 7/8 is Panchromatic)
    # We only care about packaging OA data for the "common" bands: not panchromatic.
    # So we always pick the lowest resolution: the last (or only) group.
    res_grp = h5group[resolution_groups[-1]]

    def _write(section: str, dataset_names: Sequence[str]):
        """
        Write supplementary attributes as measurement.
        """
        for dataset_name in dataset_names:
            o = f"{section}/{dataset_name}"
            with do(f"Path {o!r} "):
                measurement_name = utils.normalise_band_name(dataset_name)
                write_measurement_h5(
                    p,
                    f"oa:{measurement_name}",
                    res_grp[o],
                    # We only use the product bands for valid data calc, not supplementary.
                    # According to Josh: Supplementary pixels outside of the product bounds are implicitly invalid.
                    expand_valid_data=False,
                    overviews=None,
                )

    _write(
        "SATELLITE-SOLAR",
        [
            "SATELLITE-VIEW",
            "SATELLITE-AZIMUTH",
            "SOLAR-ZENITH",
            "SOLAR-AZIMUTH",
            "RELATIVE-AZIMUTH",
            "TIME-DELTA",
        ],
    )
    _write("INCIDENT-ANGLES", ["INCIDENT-ANGLE", "AZIMUTHAL-INCIDENT"])
    _write("EXITING-ANGLES", ["EXITING-ANGLE", "AZIMUTHAL-EXITING"])
    _write("RELATIVE-SLOPE", ["RELATIVE-SLOPE"])
    _write("SHADOW-MASKS", ["COMBINED-TERRAIN-SHADOW"])

    timedelta_data = (
        res_grp["SATELLITE-SOLAR/TIME-DELTA"] if infer_datetime_range else None
    )
    with do("Contiguity", timedelta=bool(timedelta_data)):
        _create_contiguity(
            p,
            product_list,
            resolution_yx=tuple(res_grp.attrs["resolution"]),
            timedelta_data=timedelta_data,
        )


def _create_contiguity(
    p: DatasetAssembler,
    product_list: Iterable[str],
    resolution_yx: Tuple[float, float],
    timedelta_product: str = "nbar",
    timedelta_data: numpy.ndarray = None,
):
    """
    Create the contiguity (all pixels valid) dataset.

    Write a contiguity mask file based on the intersection of valid data pixels across all
    bands from the input files.
    """
    for product in product_list:
        contiguity = None
        for grid, band_name, path in p.iter_measurement_paths():
            if not band_name.startswith(f"{product.lower()}:"):
                continue
            # Only our given res group (no pan band in Landsat)
            if grid.resolution_yx != resolution_yx:
                continue

            with rasterio.open(path) as ds:
                ds: DatasetReader
                if contiguity is None:
                    contiguity = numpy.ones((ds.height, ds.width), dtype="uint8")
                    geobox = GridSpec.from_rio(ds)
                elif ds.shape != contiguity.shape:
                    raise NotImplementedError(
                        "Contiguity from measurements of different shape"
                    )

                for band in ds.indexes:
                    contiguity &= ds.read(band) > 0

        if contiguity is None:
            secho(f"No images found for requested product {product}", fg="red")
            continue

        p.write_measurement_numpy(
            f"oa:{product.lower()}_contiguity",
            contiguity,
            geobox,
            nodata=255,
            overviews=None,
            expand_valid_data=False,
        )

        # masking the timedelta_data with contiguity mask to get max and min timedelta within the NBAR product
        # footprint for Landsat sensor. For Sentinel sensor, it inherits from level 1 yaml file
        if timedelta_data is not None and product.lower() == timedelta_product:
            valid_timedelta_data = numpy.ma.masked_where(
                contiguity == 0, timedelta_data
            )

            def offset_from_center(v: numpy.datetime64):
                return p.datetime + timedelta(
                    microseconds=v.astype(float) * 1_000_000.0
                )

            p.datetime_range = (
                offset_from_center(numpy.ma.min(valid_timedelta_data)),
                offset_from_center(numpy.ma.max(valid_timedelta_data)),
            )


@contextlib.contextmanager
def do(name: str, heading=False, **fields):
    """
    Informational logging.

    TODO: move this to the cli. It shouldn't be part of library usage.
    """
    single_line = not heading

    def val(v: Any):
        if isinstance(v, bool):
            return bool_style(v)
        if isinstance(v, Path):
            return repr(str(v))
        return repr(v)

    if heading:
        name = f"\n{name}"
    fields = " ".join(f"{k}:{val(v)}" for k, v in fields.items())
    secho(f"{name} {fields} ", nl=not single_line, fg="blue" if heading else None)
    yield
    if single_line:
        secho("(done)")


def _extract_reference_code(p: DatasetAssembler, granule: str) -> Optional[str]:
    matches = None
    if p.platform.startswith("landsat"):
        matches = re.match(r"L\w\d(?P<reference_code>\d{6}).*", granule)
    elif p.platform.startswith("sentinel-2"):
        matches = re.match(r".*_T(?P<reference_code>\d{1,2}[A-Z]{3})_.*", granule)

    if matches:
        [reference_code] = matches.groups()
        # TODO name properly
        return reference_code
    return None


@attr.s(auto_attribs=True)
class Granule:
    """
    A single granule in a hdf5 file, with optional corresponding fmask/gqa/etc files.

    You probably want to make one by using `Granule.for_path()`
    """

    name: str
    wagl_hdf5: Path
    wagl_metadata: Dict
    source_level1_metadata: DatasetDoc

    fmask_doc: Optional[Dict] = None
    fmask_image: Optional[Path] = None
    gqa_doc: Optional[Dict] = None
    tesp_doc: Optional[Dict] = None

    @classmethod
    def for_path(
        cls,
        wagl_hdf5: Path,
        granule_names: Optional[Sequence[str]] = None,
        level1_metadata_path: Optional[Path] = None,
        fmask_image_path: Optional[Path] = None,
        fmask_doc_path: Optional[Path] = None,
        gqa_doc_path: Optional[Path] = None,
        tesp_doc_path: Optional[Path] = None,
    ):
        """
        Create granules by scanning the given hdf5 file.

        Optionally specify additional files and level1 path.

        If they are not specified it look for them using WAGL's output naming conventions.
        """
        if not wagl_hdf5.exists():
            raise ValueError(f"Input hdf5 doesn't exist {wagl_hdf5}")

        with h5py.File(wagl_hdf5, "r") as fid:
            granule_names = granule_names or fid.keys()

            for granule_name in granule_names:
                if granule_name not in fid:
                    raise ValueError(
                        f"Granule {granule_name!r} not found in file {wagl_hdf5}"
                    )

                wagl_doc_field = get_path(fid, (granule_name, "METADATA", "CURRENT"))
                if not wagl_doc_field:
                    raise ValueError(
                        f"Granule contains no wagl metadata: {granule_name} in {wagl_hdf5}"
                    )

                [wagl_doc] = loads_yaml(wagl_doc_field[()])

                if not level1_metadata_path:
                    level1_tar_path = Path(
                        get_path(wagl_doc, ("source_datasets", "source_level1"))
                    )
                    level1_metadata_path = level1_tar_path.with_suffix(
                        ".odc-metadata.yaml"
                    )
                if not level1_metadata_path.exists():
                    raise ValueError(
                        f"No level1 metadata found at {level1_metadata_path}"
                    )

                level1 = serialise.from_path(level1_metadata_path)

                fmask_image_path = fmask_image_path or wagl_hdf5.with_name(
                    f"{granule_name}.fmask.img"
                )
                if not fmask_image_path.exists():
                    raise ValueError(f"No fmask image found at {fmask_image_path}")

                fmask_doc_path = fmask_doc_path or fmask_image_path.with_suffix(".yaml")
                if not fmask_doc_path.exists():
                    raise ValueError(f"No fmask found at {fmask_doc_path}")
                with fmask_doc_path.open("r") as fl:
                    [fmask_doc] = loads_yaml(fl)

                gqa_doc_path = gqa_doc_path or wagl_hdf5.with_name(
                    f"{granule_name}.gqa.yaml"
                )
                if not gqa_doc_path.exists():
                    raise ValueError(f"No gqa found at {gqa_doc_path}")
                with gqa_doc_path.open("r") as fl:
                    [gqa_doc] = loads_yaml(fl)

                # Optional doc
                if tesp_doc_path:
                    # But if they gave us a path, we're strict about it existing.
                    if not tesp_doc_path.exists():
                        raise ValueError(
                            f"Supplied tesp doc path doesn't exist: {tesp_doc_path}"
                        )
                else:
                    tesp_doc_path = wagl_hdf5.with_name(f"{granule_name}.tesp.yaml")
                if tesp_doc_path.exists():
                    with tesp_doc_path.open("r") as fl:
                        [tesp_doc] = loads_yaml(fl)

                yield cls(
                    name=granule_name,
                    wagl_hdf5=wagl_hdf5,
                    wagl_metadata=wagl_doc,
                    source_level1_metadata=level1,
                    fmask_doc=fmask_doc,
                    fmask_image=fmask_image_path,
                    gqa_doc=gqa_doc,
                    tesp_doc=tesp_doc,
                )


def package_file(
    out_directory: Path,
    hdf_file: Path,
    included_products: Iterable[str] = DEFAULT_PRODUCTS,
    include_oa: bool = True,
) -> Dict[UUID, Path]:
    """
    Simple alternative to package().

    Takes a single HDF5 and infers other paths (gqa etc) via naming conventions.

    Returns a dictionary of the output datasets: Mapping UUID to the their metadata path.
    """

    out = {}
    for granule in Granule.for_path(hdf_file):
        dataset_id, metadata_path = package(
            out_directory,
            granule,
            included_products=included_products,
            include_oa=include_oa,
        )
        out[dataset_id] = metadata_path

    return out


def package(
    out_directory: Path,
    granule: Granule,
    included_products: Iterable[str] = DEFAULT_PRODUCTS,
    include_oa: bool = True,
) -> Tuple[UUID, Path]:
    """
    Package an L2 product.

    :param include_oa:

    :param out_directory:
        The base directory for output datasets. A DEA-naming-conventions folder hierarchy
        will be created inside this folder.

    :param granule:
        Granule information. You probably want to make one with Granule.from_path()

    :param included_products:
        A list of imagery products to include in the package.
        Defaults to all products.

    :return:
        The dataset UUID and output metadata path
    """
    included_products = tuple(s.lower() for s in included_products)

    with h5py.File(granule.wagl_hdf5, "r") as fid:
        granule_group = fid[granule.name]

        with DatasetAssembler(
            out_directory,
            # WAGL stamps a good, random ID already.
            dataset_id=granule.wagl_metadata.get("id"),
            naming_conventions="dea",
        ) as p:
            level1 = granule.source_level1_metadata
            p.add_source_dataset(level1, auto_inherit_properties=True)

            # It's a GA ARD product.
            p.producer = "ga.gov.au"
            p.product_family = "ard"

            _read_wagl_metadata(p, granule_group)

            org_collection_number = utils.get_collection_number(
                p.producer, p.properties["landsat:collection_number"]
            )

            # TODO: wagl's algorithm version should determine our dataset version number, right?
            # The '1' is after gadi software changes.
            p.dataset_version = f"{org_collection_number}.1.0"
            p.region_code = _extract_reference_code(p, granule.name)

            _read_gqa_doc(p, granule.gqa_doc)
            _read_fmask_doc(p, granule.fmask_doc)
            if granule.tesp_doc:
                _take_software_versions(p, granule.tesp_doc)

            _unpack_products(p, included_products, granule_group)

            if include_oa:
                with do("Starting OA", heading=True):
                    _unpack_observation_attributes(
                        p,
                        included_products,
                        granule_group,
                        infer_datetime_range=level1.platform.startswith("landsat"),
                    )
                if granule.fmask_image:
                    with do(f"Writing fmask from {granule.fmask_image} "):
                        p.write_measurement(
                            "oa:fmask",
                            granule.fmask_image,
                            expand_valid_data=False,
                            overview_resampling=Resampling.mode,
                        )

            with do("Finishing package"):
                return p.done()


def _flatten_dict(d: Mapping, prefix=None, separator=".") -> Iterable[Tuple[str, Any]]:
    """
    >>> dict(_flatten_dict({'a' : 1, 'b' : {'inner' : 2},'c' : 3}))
    {'a': 1, 'b.inner': 2, 'c': 3}
    >>> dict(_flatten_dict({'a' : 1, 'b' : {'inner' : {'core' : 2}}}, prefix='outside', separator=':'))
    {'outside:a': 1, 'outside:b:inner:core': 2}
    """
    for k, v in d.items():
        name = f"{prefix}{separator}{k}" if prefix else k
        if isinstance(v, Mapping):
            yield from _flatten_dict(v, prefix=name, separator=separator)
        else:
            yield name, v


def _read_gqa_doc(p: DatasetAssembler, doc: Dict):
    _take_software_versions(p, doc)
    p.extend_user_metadata("gqa", doc)

    # TODO: more of the GQA fields?
    for k, v in _flatten_dict(doc["residual"], separator="_"):
        p.properties[f"gqa:{k}"] = v


def _read_fmask_doc(p: DatasetAssembler, doc: Dict):
    for name, value in doc["percent_class_distribution"].items():
        # From Josh: fmask cloud cover trumps the L1 cloud cover.
        if name == "cloud":
            del p.properties["eo:cloud_cover"]
            p.properties["eo:cloud_cover"] = value

        p.properties[f"fmask:{name}"] = value

    _take_software_versions(p, doc)
    p.extend_user_metadata("fmask", doc)


def _take_software_versions(p: DatasetAssembler, doc: Dict):
    versions = doc.pop("software_versions", {})

    for name, o in versions.items():
        p.note_software_version(name, o.get("repo_url"), o.get("version"))


def find_a_granule_name(wagl_hdf5: Path) -> str:
    """
    Try to extract granule name from wagl filename,

    >>> find_a_granule_name(Path('LT50910841993188ASA00.wagl.h5'))
    'LT50910841993188ASA00'
    >>> find_a_granule_name(Path('my-test-granule.h5'))
    Traceback (most recent call last):
    ...
    ValueError: No granule specified, and cannot find it on input filename 'my-test-granule'.
    """
    granule_name = wagl_hdf5.stem.split(".")[0]
    if not granule_name.startswith("L"):
        raise ValueError(
            f"No granule specified, and cannot find it on input filename {wagl_hdf5.stem!r}."
        )
    return granule_name


def _read_wagl_metadata(p: DatasetAssembler, granule_group: h5py.Group):
    try:
        wagl_path, *ancil_paths = [
            pth
            for pth in (_find_h5_paths(granule_group, "SCALAR"))
            if "METADATA" in pth
        ]
    except ValueError:
        raise ValueError("No nbar metadata found in granule")

    [wagl_doc] = loads_yaml(granule_group[wagl_path][()])

    try:
        p.processed = get_path(wagl_doc, ("system_information", "time_processed"))
    except PathAccessError:
        raise ValueError(f"WAGL dataset contains no time processed. Path {wagl_path}")

    for i, path in enumerate(ancil_paths, start=2):
        wagl_doc.setdefault(f"wagl_{i}", {}).update(
            list(loads_yaml(granule_group[path][()]))[0]["ancillary"]
        )

    p.properties["dea:dataset_maturity"] = _determine_maturity(
        p.datetime, p.processed, wagl_doc
    )

    _take_software_versions(p, wagl_doc)
    p.extend_user_metadata("wagl", wagl_doc)


def _determine_maturity(acq_date: datetime, processed: datetime, wagl_doc: Dict):
    """
    Determine maturity field of a dataset.

    Based on the fallback logic in nbar pages of CMI, eg: https://cmi.ga.gov.au/ga_ls5t_nbart_3
    """
    ancillary_tiers = {
        key.lower(): o["tier"]
        for key, o in wagl_doc["ancillary"].items()
        if "tier" in o
    }

    if "water_vapour" not in ancillary_tiers:
        # Perhaps this should be a warning, but I'm being strict until told otherwise.
        # (a warning is easy to ignore)
        raise ValueError(
            f"No water vapour ancillary tier. Got {list(ancillary_tiers.keys())!r}"
        )

    water_vapour_is_definitive = ancillary_tiers["water_vapour"].lower() == "definitive"

    if (processed - acq_date) < timedelta(hours=48):
        return "nrt"

    if not water_vapour_is_definitive:
        return "interim"

    # For accurate BRDF, both Aqua and Terra need to be operating.
    # Aqua launched May 2002, and we add a ~2 month buffer of operation.
    if acq_date < default_utc(datetime(2002, 7, 1)):
        return "final"

    if "brdf" not in ancillary_tiers:
        # Perhaps this should be a warning, but I'm being strict until told otherwise.
        # (a warning is easy to ignore)
        raise ValueError(
            f"No brdf tier available. Got {list(ancillary_tiers.keys())!r}"
        )
    brdf_tier = ancillary_tiers["brdf"].lower()

    if "definitive" in brdf_tier:
        return "final"
    elif "fallback" in brdf_tier:
        return "interim"
    else:
        # This value should not occur for production data, only for experiments
        return "user"

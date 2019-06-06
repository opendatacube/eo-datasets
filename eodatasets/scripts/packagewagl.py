#!/usr/bin/env python

import os
import re
import tempfile
from os.path import basename
from pathlib import Path
from posixpath import join as ppjoin
from typing import Dict, Sequence, List

import h5py
import numpy
import numpy as np
import rasterio
import yaml
from click import secho
from yaml.representer import Representer

import eodatasets
from eodatasets.prepare import serialise, images
from eodatasets.prepare.assemble import DatasetAssembler
from eodatasets.prepare.images import GridSpec
from eodatasets.prepare.model import DatasetDoc

EUGL_VERSION = "DO_SOMETHING_HERE"

FMASK_VERSION = "DO_SOMETHING_HERE2"
FMASK_REPO_URL = "https://bitbucket.org/chchrsc/python-fmask"

TESP_VERSION = eodatasets.__version__
TESP_REPO_URL = "https://github.com/GeoscienceAustralia/eo-datasets"

os.environ["CPL_ZIP_ENCODING"] = "UTF-8"


FILENAME_TIF_BAND = re.compile(
    r"(?P<prefix>(?:.*_)?)(?P<band_name>B[0-9][A0-9]|B[0-9]*|B[0-9a-zA-z]*)"
    r"(?P<extension>\....)"
)
PRODUCT_SUITE_FROM_GRANULE = re.compile("(L1[GTPCS]{1,2})")
ARD = "ARD"
QA = "QA"
SUPPS = "SUPPLEMENTARY"


def find_h5_paths(h5_obj: h5py.Group, dataset_class: str = "") -> List[str]:
    """
    Given an h5py `Group`, `File` (opened file id; fid),
    recursively list all objects or optionally only list
    `h5py.Dataset` objects matching a given class, for example:

        * IMAGE
        * TABLE
        * SCALAR

    :param h5_obj:
        A h5py `Group` or `File` object to use as the
        entry point from which to start listing the contents.

    :param dataset_class:
        A `str` containing a CLASS name identifier, eg:

        * IMAGE
        * TABLE
        * SCALAR

        Default is an empty string `''`.

    :return:
        A `list` containing the pathname to all matching objects.
    """

    items = []

    def _find(name, obj):
        """
        An internal utility to find objects matching `dataset_class`.
        """
        if obj.attrs.get("CLASS") == dataset_class:
            items.append(name)

    h5_obj.visititems(_find)
    return items


def provider_reference_info(p: DatasetAssembler, granule: str):
    """
    Extracts provider reference metadata
    Supported platforms are:
        * LANDSAT
        * SENTINEL2
    :param granule:
        A string referring to the name of the capture

    :return:
        Dictionary; contains satellite reference if identified
    """
    matches = None
    if p.platform.startswith("landsat"):
        matches = re.match(r"L\w\d(?P<reference_code>\d{6}).*", granule)
    elif p.platform.startswith("sentinel-2"):
        matches = re.match(r".*_T(?P<reference_code>\d{1,2}[A-Z]{3})_.*", granule)

    if matches:
        [reference_code] = matches.groups()
        # TODO name properly
        p["odc:reference_code"] = reference_code


def _gls_version(ref_fname: str) -> str:
    # TODO a more appropriate method of version detection and/or population of metadata
    if "GLS2000_GCP_SCENE" in ref_fname:
        gls_version = "GLS_v1"
    else:
        gls_version = "GQA_v3"

    return gls_version


yaml.add_representer(numpy.int8, Representer.represent_int)
yaml.add_representer(numpy.uint8, Representer.represent_int)
yaml.add_representer(numpy.int16, Representer.represent_int)
yaml.add_representer(numpy.uint16, Representer.represent_int)
yaml.add_representer(numpy.int32, Representer.represent_int)
yaml.add_representer(numpy.uint32, Representer.represent_int)
yaml.add_representer(numpy.int, Representer.represent_int)
yaml.add_representer(numpy.int64, Representer.represent_int)
yaml.add_representer(numpy.uint64, Representer.represent_int)
yaml.add_representer(numpy.float, Representer.represent_float)
yaml.add_representer(numpy.float32, Representer.represent_float)
yaml.add_representer(numpy.float64, Representer.represent_float)
yaml.add_representer(numpy.ndarray, Representer.represent_list)


def _l1_to_ard(granule: str) -> str:
    return re.sub(PRODUCT_SUITE_FROM_GRANULE, ARD, granule)


def unpack_products(
    p: DatasetAssembler,
    product_list: Sequence[str],
    level1: DatasetDoc,
    h5group: h5py.Group,
    outdir: Path,
) -> None:
    """
    Unpack and package the NBAR and NBART products.
    """
    # listing of all datasets of IMAGE CLASS type
    img_paths = find_h5_paths(h5group, "IMAGE")

    # TODO pass products through from the scheduler rather than hard code
    for product in product_list:
        secho(f"\n\nStarting {product}", fg="blue")
        for pathname in [p for p in img_paths if "/{}/".format(product) in p]:
            secho(f"Path {pathname}", fg="blue")
            dataset = h5group[pathname]

            band_name = _clean_alias(dataset)
            base_fname = basename(level1.measurements[band_name.replace("_", "")].path)

            match_dict = FILENAME_TIF_BAND.match(base_fname).groupdict()
            out_fname = (
                outdir
                / product
                / _l1_to_ard(
                    "{}{}_{}{}".format(
                        match_dict.get("prefix"),
                        product,
                        match_dict.get("band_name"),
                        match_dict.get("extension"),
                    )
                )
            )
            # TODO: formal separation of 'product' groups?
            p.write_measurement_h5(f"{product}_{band_name}", dataset)

    pathnames = [
        pth for pth in (find_h5_paths(h5group, "SCALAR")) if "NBAR-METADATA" in pth
    ]

    tags = yaml.load(h5group[pathnames[0]][()])
    for path in pathnames[1:]:
        other = yaml.load(h5group[path][()])
        tags["ancillary"].update(other["ancillary"])

    p.extend_user_metadata("wagl", tags)


def _clean_alias(dataset: h5py.Dataset):
    return dataset.attrs["alias"].lower().replace("-", "_")


def unpack_supplementary(p: DatasetAssembler, h5group: h5py.Group):
    """
    Unpack the angles + other supplementary datasets produced by wagl.
    Currently only the mode resolution group gets extracted.
    """

    def _write(dataset_names: Sequence[str], h5_group: h5py.Group, basedir: str):
        """
        An internal util for serialising the supplementary
        H5Datasets to tif.
        """
        for dname in dataset_names:
            p.write_measurement_h5(f"{basedir}/{dname}", h5_group[dname])

    res_grps = [g for g in h5group.keys() if g.startswith("RES-GROUP-")]
    if len(res_grps) != 1:
        raise NotImplementedError(f"expected one res group, got {res_grps!r}")
    [res_grp] = res_grps

    grn_id = ""

    # satellite and solar angles
    grp = h5group[ppjoin(res_grp, "SATELLITE-SOLAR")]

    _write(
        [
            "SATELLITE-VIEW",
            "SATELLITE-AZIMUTH",
            "SOLAR-ZENITH",
            "SOLAR-AZIMUTH",
            "RELATIVE-AZIMUTH",
            "TIMEDELTA",
        ],
        grp,
        SUPPS,
    )

    # timedelta data
    timedelta_data = grp["TIMEDELTA"]

    # incident angles

    _write(
        ["INCIDENT", "AZIMUTHAL-INCIDENT"],
        h5group[ppjoin(res_grp, "INCIDENT-ANGLES")],
        SUPPS,
    )

    # exiting angles

    _write(
        ["EXITING", "AZIMUTHAL-EXITING"],
        h5group[ppjoin(res_grp, "EXITING-ANGLES")],
        SUPPS,
    )

    # relative slope

    _write(["RELATIVE-SLOPE"], h5group[ppjoin(res_grp, "RELATIVE-SLOPE")], SUPPS)

    # terrain shadow
    # TODO: this one had cogtif=True? (but was unused in `_write()`)

    _write(["COMBINED-TERRAIN-SHADOW"], h5group[ppjoin(res_grp, "SHADOW-MASKS")], QA)

    # TODO do we also include slope and aspect?

    return timedelta_data


def create_contiguity(
    p: DatasetAssembler,
    product_list: Sequence[str],
    level1: DatasetDoc,
    granule: str,
    outdir: Path,
    timedelta_data,
):
    """
    Create the contiguity (all pixels valid) dataset.
    """
    # TODO: Actual res?
    res = level1.properties["eo:gsd"]

    grn_id = _l1_to_ard(granule)

    with tempfile.TemporaryDirectory(dir=outdir, prefix="contiguity-") as tmpdir:
        for product in product_list:
            search_path = outdir / product
            fnames = [
                str(f) for f in search_path.glob("*.TIF") if "QUICKLOOK" not in str(f)
            ]

            # quick work around for products that aren't being packaged
            if not fnames:
                continue

            out_fname = outdir / QA / "{}_{}_CONTIGUITY.TIF".format(grn_id, product)
            out_fname.parent.mkdir(exist_ok=True)

            # Build a temp vrt
            # S2 bands are different resolutions. Make them appear the same when taking contiguity.
            tmp_fname = Path(tmpdir) / f"{product}.vrt"
            images.run_command(
                [
                    "gdalbuildvrt",
                    "-resolution",
                    "user",
                    "-tr",
                    res,
                    res,
                    "-separate",
                    tmp_fname,
                    *fnames,
                ],
                tmpdir,
            )

            # def contiguity(p: DatasetAssembler, product_name: str, fname: Path):
            """
            Write a contiguity mask file based on the intersection of valid data pixels across all
            bands from the input file and returns with the geobox of the source dataset
            """
            with rasterio.open(tmp_fname) as ds:
                geobox = GridSpec.from_rio(ds)
                ones = np.ones((ds.height, ds.width), dtype="uint8")
                for band in ds.indexes:
                    ones &= ds.read(band) > 0

                p.write_measurement_numpy(f"{product.lower()}_contiguity", ones, geobox)

            # masking the timedelta_data with contiguity mask to get max and min timedelta within the NBAR product
            # footprint for Landsat sensor. For Sentinel sensor, it inherits from level 1 yaml file
            if product.lower() == "nbar" and level1.properties[
                "eo:platform"
            ].startswith("landsat"):
                valid_timedelta_data = numpy.ma.masked_where(ones == 0, timedelta_data)

                center_dt = np.datetime64(level1.datetime)
                from_dt = center_dt + np.timedelta64(
                    int(float(numpy.ma.min(valid_timedelta_data)) * 1000000), "us"
                )
                to_dt = center_dt + np.timedelta64(
                    int(float(numpy.ma.max(valid_timedelta_data)) * 1000000), "us"
                )
                p.datetime_range = (from_dt, to_dt)


def package(
    l1_path: Path,
    antecedents: Dict[str, Path],
    outdir: Path,
    granule: str,
    products=("NBAR",),  # "NBART", "LAMBERTIAN", "SBT"),
):
    """
    Package an L2 product.

    :param l1_path:
        A string containing the full file pathname to the Level-1
        dataset.

    :param antecedents:
        A dictionary describing antecedent task outputs
        (currently supporting wagl, eugl-gqa, eugl-fmask)
        to package.

    :param outdir:
        A string containing the full file pathname to the directory
        that will contain the packaged Level-2 datasets.

    :param granule:
        The identifier for the granule

    :param products:
        A list of imagery products to include in the package.
        Defaults to all products.

    :return:
        None; The packages will be written to disk directly.
    """

    level1 = serialise.from_path(l1_path)

    with h5py.File(antecedents["wagl"], "r") as fid:
        out_path = outdir / _l1_to_ard(granule)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with DatasetAssembler(out_path) as p:
            p.add_source_dataset(level1, auto_inherit_properties=True)

            # TODO better software identifiers
            p.note_software_version("eugl", EUGL_VERSION)
            p.note_software_version(FMASK_REPO_URL, FMASK_VERSION)
            # p.note_software_version('gverify', gverify_version)
            p.note_software_version(TESP_REPO_URL, TESP_VERSION)

            # TODO there's probably a real one.
            p["dea:processing_level"] = "Level-2"
            provider_reference_info(p, granule)

            # GA's collection 3 processes USGS Collection 1
            if level1.properties["landsat:collection_number"] == 1:
                org_collection_number = 3
            else:
                raise NotImplementedError(f"Unsupported collection number.")

            p.properties["odc:product_family"] = "ard"

            # TODO: Move this into the naming api.
            p.product_name = "ga_{platform}{instrument}_{family}_{collection}".format(
                platform=p.platform_abbreviated,
                instrument=p.instrument[0].lower(),
                family=p.properties["odc:product_family"],
                collection=int(org_collection_number),
            )

            # TODO: pan band?
            # cogtif_args = get_cogtif_options(
            #     level1.grids[level1.measurements["blue"].grid].shape
            # )

            # unpack the standardised products produced by wagl
            unpack_products(p, products, level1, h5group=fid[granule], outdir=out_path)

            # unpack supplementary datasets produced by wagl
            timedelta_data = unpack_supplementary(p, fid[granule])

            # file based globbing, so can't have any other tifs on disk
            create_contiguity(p, products, level1, granule, out_path, timedelta_data)

            # fmask cogtif conversion
            if "fmask" in antecedents:

                # TODO: this one has different predictor settings?
                fmask_cogtif_args_predictor = 2

                p.write_measurement("qa/fmask", antecedents["fmask"])

                # The processing version should be supplied somewhere in their metadata.
                p.note_software_version("fmask_repo", "TODO")

            # merge all the yaml documents
            if "gqa" in antecedents:
                with antecedents["gqa"].open() as fl:
                    p.extend_user_metadata("gqa", yaml.safe_load(fl))

            p.finish()


def run():
    package(
        l1_path=Path(
            "/home/jez/dea/eo-datasets/wagltest/LT05_L1TP_091084_19930707_20170118_01_T1.yaml"
        ),
        antecedents={
            "wagl": Path(
                "/home/jez/dea/eo-datasets/wagltest/LT50910841993188ASA00.wagl.h5"
            ),
            # 'eugl-gqa',
            # 'eugl-fmask',
        },
        outdir=Path("./wagl-out").absolute(),
        granule="LT50910841993188ASA00",
    )


if __name__ == "__main__":
    run()

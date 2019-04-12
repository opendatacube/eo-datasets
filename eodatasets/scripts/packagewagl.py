#!/usr/bin/env python
# pylint: disable=too-many-locals

import copy
import os
import re
import tempfile
import uuid
from os.path import join as pjoin, basename, dirname, splitext, exists
from pathlib import Path
from posixpath import join as ppjoin
from subprocess import check_call

import h5py
import numpy as np
import yaml
from pkg_resources import resource_stream
from rasterio.enums import Resampling
from yaml.representer import Representer

from eodatasets.prepare.ls_usgs_l1_prepare import prepare_dataset as landsat_prepare
from eodatasets.prepare.s2_l1c_aws_pds_prepare import (
    prepare_dataset as sentinel_2_aws_pds_prepare,
)
from eodatasets.prepare.s2_prepare_cophub_zip import (
    prepare_dataset as sentinel_2_zip_prepare,
)
from wagl.acquisition import acquisitions, AcquisitionsContainer
from wagl.constants import DatasetName, GroupName
from wagl.data import write_img
from wagl.geobox import GriddedGeoBox
from wagl.hdf5 import find


import numpy
import rasterio
from skimage.exposure import rescale_intensity

from eodatasets.verify import PackageChecksum

import fmask

EUGL_VERSION = "DO_SOMETHING_HERE"

FMASK_REPO_URL = "https://bitbucket.org/chchrsc/python-fmask"

TESP_VERSION = "DO_SOMETHING_HERE"
TESP_REPO_URL = "https://github.com/OpenDataCubePipelines/tesp"

os.environ["CPL_ZIP_ENCODING"] = "UTF-8"


def provider_reference_info(granule, wagl_tags):
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
    provider_info = {}
    matches = None
    if "LANDSAT" in wagl_tags["source_datasets"]["platform_id"]:
        matches = re.match(r"L\w\d(?P<reference_code>\d{6}).*", granule)
    elif "SENTINEL_2" in wagl_tags["source_datasets"]["platform_id"]:
        matches = re.match(r".*_T(?P<reference_code>\d{1,2}[A-Z]{3})_.*", granule)

    if matches:
        provider_info.update(**matches.groupdict())
    return provider_info


def merge_metadata(
    level1_tags, wagl_tags, granule, image_paths, platform, **antecedent_tags
):
    """
    Combine the metadata from input sources and output
    into a single ARD metadata yaml.
    """
    # TODO have properly defined product types for the ARD product
    ptype = {
        "LANDSAT_5": "L5ARD",
        "LANDSAT_7": "L7ARD",
        "LANDSAT_8": "L8ARD",
        "SENTINEL_2A": "S2MSIARD",
        "SENTINEL_2B": "S2MSIARD",
    }

    # TODO: resolve common software version for fmask and gqa
    software_versions = wagl_tags["software_versions"]

    software_versions["tesp"] = {"repo_url": TESP_REPO_URL, "version": TESP_VERSION}

    # for Landsat, from_dt and to_dt in ARD-METADATA is populated from max and min timedelta values
    if platform == "LANDSAT":

        # pylint: disable=too-many-function-args
        def interpret_landsat_temporal_extent():
            """
            Landsat imagery only provides a center datetime; a time range can be derived
            from the timedelta dataset
            """

            center_dt = np.datetime64(level1_tags["extent"]["center_dt"])
            from_dt = center_dt + np.timedelta64(
                int(float(wagl_tags.pop("timedelta_min")) * 1000000), "us"
            )
            to_dt = center_dt + np.timedelta64(
                int(float(wagl_tags.pop("timedelta_max")) * 1000000), "us"
            )

            level2_extent = {
                "center_dt": "{}Z".format(center_dt),
                "coord": level1_tags["extent"]["coord"],
                "from_dt": "{}Z".format(from_dt),
                "to_dt": "{}Z".format(to_dt),
            }

            return level2_extent

        level2_extent = interpret_landsat_temporal_extent()
    else:
        level2_extent = level1_tags["extent"]

    # TODO: extend yaml document to include fmask and gqa yamls
    merged_yaml = {
        "algorithm_information": wagl_tags["algorithm_information"],
        "system_information": wagl_tags["system_information"],
        "id": str(uuid.uuid4()),
        "processing_level": "Level-2",
        "product_type": ptype[wagl_tags["source_datasets"]["platform_id"]],
        "platform": {"code": wagl_tags["source_datasets"]["platform_id"]},
        "instrument": {"name": wagl_tags["source_datasets"]["sensor_id"]},
        "format": {"name": "GeoTIFF"},
        "tile_id": granule,
        "extent": level2_extent,
        "grid_spatial": level1_tags["grid_spatial"],
        "image": {"bands": image_paths},
        "lineage": {
            "ancillary": wagl_tags["ancillary"],
            "source_datasets": {
                level1_tags["product_type"]: copy.deepcopy(level1_tags)
            },
        },
    }

    # Configured to handle gqa and fmask antecedent tasks
    for task_name, task_md in antecedent_tags.items():
        if "software_versions" in task_md:
            for key, value in task_md.pop("software_versions").items():
                software_versions[key] = value  # This fails on key conflicts

        # Check for valid metadata after merging the software versions
        if task_md:
            merged_yaml[task_name] = task_md

    provider_info = provider_reference_info(granule, wagl_tags)
    if provider_info:
        merged_yaml["provider"] = provider_info

    merged_yaml["software_versions"] = software_versions

    return merged_yaml


def extract_level1_metadata(acq):
    """
    Factory method for selecting a level1 metadata script

    """
    # Optional (not installed yet on Travis)
    # pytest: disable=import-error
    from wagl.acquisition.sentinel import (
        _Sentinel2SinergiseAcquisition,
        Sentinel2Acquisition,
    )
    from wagl.acquisition.landsat import LandsatAcquisition

    if isinstance(acq, _Sentinel2SinergiseAcquisition):
        return sentinel_2_aws_pds_prepare(Path(acq.pathname))
    elif isinstance(acq, Sentinel2Acquisition):
        return sentinel_2_zip_prepare(Path(acq.pathname))
    elif isinstance(acq, LandsatAcquisition):
        return landsat_prepare(Path(acq.pathname))

    raise NotImplementedError(
        "No level-1 YAML generation defined for target acquisition "
        "and no yaml_dir defined for level-1 metadata"
    )


def contiguity(fname):
    """
    Write a contiguity mask file based on the intersection of valid data pixels across all
    bands from the input file and returns with the geobox of the source dataset
    """
    with rasterio.open(fname) as ds:
        geobox = GriddedGeoBox.from_dataset(ds)
        yblock, xblock = ds.block_shapes[0]
        ones = np.ones((ds.height, ds.width), dtype="uint8")
        for band in ds.indexes:
            ones &= ds.read(band) > 0

    return ones, geobox


def quicklook(fname, out_fname, src_min, src_max, out_min=0, out_max=255):
    """
    Generate a quicklook image applying a linear contrast enhancement.
    Outputs will be converted to Uint8.
    If the input image has a valid no data value, the no data will
    be set to 0 in the output image.
    Any non-contiguous data across the colour domain, will be set to
    zero.
    The output is a tiled GeoTIFF with JPEG compression, utilising the
    YCBCR colour model, as well as a mask band.
    This routine attempts to minimise memory consumption, as such
    it reads data as needed on-the-fly, and doesn't retain all colour
    bands in memory.
    The same can't be said for writing to disk as this'll be in
    rasterio's code. The pixel interleaved nature of JPEG compression
    might require (on rasterio's end) to have all bands in memory.
    Extra code could be written to do I/O utilising the same output
    tile size in order to only have (y_size, x_size, bands) in memory
    at a time.

    :param fname:
        A `str` containing the file pathname to an image containing
        the relevant data to extract.

    :param out_fname:
        A `str` containing the file pathname to where the quicklook
        image will be saved.

    :param src_min:
        An integer/float containing the minimum data value to be
        used as input into contrast enhancement.

    :param src_max:
        An integer/float containing the maximum data value to be
        used as input into the contrast enhancement.

    :param out_min:
        An integer specifying the minimum output value that `src_min`
        will be scaled to. Default is 0.

    :param out_max:
        An integer specifying the maximum output value that `src_max`
        will be scaled to. Default is 255.

    :return:
        None; The output will be written directly to disk.
        The output datatype will be `UInt8`.
    """
    with rasterio.open(fname) as ds:

        # no data locations
        nulls = numpy.zeros((ds.height, ds.width), dtype="bool")
        for band in range(1, 4):
            nulls |= ds.read(band) == ds.nodata

        kwargs = {
            "driver": "GTiff",
            "height": ds.height,
            "width": ds.width,
            "count": 3,
            "dtype": "uint8",
            "crs": ds.crs,
            "transform": ds.transform,
            "nodata": 0,
            "compress": "jpeg",
            "photometric": "YCBCR",
            "tiled": "yes",
        }

        # Only set blocksize on larger imagery; enables reduced resolution processing
        if ds.height > 512 and ds.width > 512:
            kwargs["blockxsize"] = 512
            kwargs["blockysize"] = 512

        with rasterio.open(out_fname, "w", **kwargs) as out_ds:
            for band in range(1, 4):
                scaled = rescale_intensity(
                    ds.read(band),
                    in_range=(src_min, src_max),
                    out_range=(out_min, out_max),
                )
                scaled = scaled.astype("uint8")
                scaled[nulls] = 0

                out_ds.write(scaled, band)


def _get_eugl_metadata():
    return {
        "software_versions": {
            "eugl": {
                "version": EUGL_VERSION,
                "repo_url": "git@github.com:OpenDataCubePipelines/eugl.git",
            }
        }
    }


def get_fmask_metadata():
    base_info = _get_eugl_metadata()
    base_info["software_versions"]["fmask"] = {
        "version": fmask.__version__,
        "repo_url": FMASK_REPO_URL,
    }

    return base_info


def get_gqa_metadata(gverify_executable):
    """get_gqa_metadata: provides processing metadata for gqa_processing

    :param gverify_executable: GQA version is determined from executable
    :returns metadata dictionary:
    """

    gverify_version = gverify_executable.split("_")[-1]
    base_info = _get_eugl_metadata()
    base_info["software_versions"]["gverify"] = {"version": gverify_version}

    return base_info


def _gls_version(ref_fname):
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

ALIAS_FMT = {
    "LAMBERTIAN": "lambertian_{}",
    "NBAR": "nbar_{}",
    "NBART": "nbart_{}",
    "SBT": "sbt_{}",
}
LEVELS = [8, 16, 32]
PATTERN1 = re.compile(
    r"(?P<prefix>(?:.*_)?)(?P<band_name>B[0-9][A0-9]|B[0-9]*|B[0-9a-zA-z]*)"
    r"(?P<extension>\.TIF)"
)
PATTERN2 = re.compile("(L1[GTPCS]{1,2})")
ARD = "ARD"
QA = "QA"
SUPPS = "SUPPLEMENTARY"


def run_command(command, work_dir):
    """
    A simple utility to execute a subprocess command.
    """
    check_call(" ".join(command), shell=True, cwd=work_dir)


def _clean(alias):
    """
    A quick fix for cleaning json unfriendly alias strings.
    """
    replace = {"-": "_", "[": "", "]": ""}
    for k, v in replace.items():
        alias = alias.replace(k, v)

    return alias.lower()


def get_cogtif_options(dataset, overviews=True, blockxsize=None, blockysize=None):
    """ Returns write_img options according to the source imagery provided
    :param dataset:
        Numpy array or hdf5 dataset representing raster values of the tif
    :param overviews:
        (boolean) sets overview flags in gdal config options
    :param blockxsize:
        (int) override the derived base blockxsize in cogtif conversion
    :param blockysize:
        (int) override the derived base blockysize in cogtif conversion

    returns a dict {'options': {}, 'config_options': {}}
    """

    # TODO Standardizing the Sentinel-2's overview tile size with external inputs

    options = {"compress": "deflate", "zlevel": 4}
    config_options = {}

    # If blocksize and datasets has chunks configured set to chunk size
    # otherwise default to 512
    if blockxsize is None or blockysize is None:
        if hasattr(dataset, "chunks"):
            blockysize = blockysize or dataset.chunks[0]
            blockxsize = blockxsize or dataset.chunks[1]
        else:
            # Fallback to hardcoded 512 value
            blockysize = blockysize or 512
            blockxsize = blockxsize or 512

    if dataset.shape[0] <= 512 and dataset.shape[1] <= 512:
        # Do not set block sizes for small imagery
        pass
    elif dataset.shape[1] <= 512:
        options["blockysize"] = min(blockysize, 512)
        # Set blockxsize to power of 2 rounded down
        options["blockxsize"] = int(2 ** (blockxsize.bit_length() - 1))
        # gdal does not like a x blocksize the same as the whole dataset
        if options["blockxsize"] == blockxsize:
            options["blockxsize"] = int(options["blockxsize"] / 2)
    else:
        if dataset.shape[1] == blockxsize:
            # dataset does not have an internal tiling layout
            # set the layout to a 512 block size
            blockxsize = 512
            blockysize = 512
            if overviews:
                config_options["GDAL_TIFF_OVR_BLOCKSIZE"] = blockxsize

        options["blockxsize"] = blockxsize
        options["blockysize"] = blockysize
        options["tiled"] = "yes"

    if overviews:
        options["copy_src_overviews"] = "yes"

    return {"options": options, "config_options": config_options}


def write_tif_from_dataset(
    dataset,
    out_fname,
    options,
    config_options,
    overviews=True,
    nodata=None,
    geobox=None,
):
    """
    Method to write a h5 dataset or numpy array to a tif file
    :param dataset:
        h5 dataset containing a numpy array or numpy array
        Dataset will map to the raster data

    :param out_fname:
        destination of the tif

    :param options:
        dictionary of options provided to gdal

    :param config_options:
        dictionary of configurations provided to gdal

    :param overviews:
        boolean flag to create overviews
        default (True)

    returns the out_fname param
    """
    if hasattr(dataset, "chunks"):
        data = dataset[:]
    else:
        data = dataset

    if nodata is None and hasattr(dataset, "attrs"):
        nodata = dataset.attrs.get("no_data_value")
    if geobox is None:
        geobox = GriddedGeoBox.from_dataset(dataset)

    # path existence
    if not exists(dirname(out_fname)):
        os.makedirs(dirname(out_fname))

    write_img(
        data,
        out_fname,
        levels=LEVELS,
        nodata=nodata,
        geobox=geobox,
        resampling=Resampling.average,
        options=options,
        config_options=config_options,
    )

    return out_fname


def write_tif_from_file(
    dataset: str, out_fname, options, config_options, overviews=True
):
    """
    Compatible interface for writing (cog)tifs from a source file
    :param dataset:
        path to the source file

    :param out_fname:
        destination of the tif

    :param options:
        dictionary of options provided to gdal

    :param config_options:
        dictionary of configurations provided to gdal

    :param overviews:
        boolean flag to create overviews
        default (True)

    returns the out_fname param
    """

    with tempfile.TemporaryDirectory(
        dir=dirname(out_fname), prefix="cogtif-"
    ) as tmpdir:
        command = ["gdaladdo", "-clean", dataset]
        run_command(command, tmpdir)
        if overviews:
            command = ["gdaladdo", "-r", "mode", dataset]
            command.extend([str(l) for l in LEVELS])
            run_command(command, tmpdir)
        command = ["gdal_translate", "-of", "GTiff"]

        for key, value in options.items():
            command.extend(["-co", "{}={}".format(key, value)])

        if config_options:
            for key, value in config_options.items():
                command.extend(["--config", "{}".format(key), "{}".format(value)])

        command.extend([dataset, out_fname])

        run_command(command, dirname(dataset))

    return out_fname


def get_img_dataset_info(dataset, path, layer=1):
    """
    Returns metadata for raster datasets
    """
    geobox = GriddedGeoBox.from_dataset(dataset)
    return {
        "path": path,
        "layer": layer,
        "info": {
            "width": geobox.x_size(),
            "height": geobox.y_size(),
            "geotransform": list(geobox.transform.to_gdal()),
        },
    }


def get_platform(container: AcquisitionsContainer, granule):
    """
    retuns the satellite platform
    """
    acq = container.get_acquisitions(None, granule, False)[0]
    if "SENTINEL" in acq.platform_id:
        platform = "SENTINEL"
    elif "LANDSAT" in acq.platform_id:
        platform = "LANDSAT"
    else:
        msg = "Sensor not supported"
        raise Exception(msg)

    return platform


def unpack_products(product_list, container, granule, h5group, outdir):
    """
    Unpack and package the NBAR and NBART products.
    """
    # listing of all datasets of IMAGE CLASS type
    img_paths = find(h5group, "IMAGE")

    # relative paths of each dataset for ODC metadata doc
    rel_paths = {}

    # TODO pass products through from the scheduler rather than hard code
    for product in product_list:
        for pathname in [p for p in img_paths if "/{}/".format(product) in p]:

            dataset = h5group[pathname]

            acqs = container.get_acquisitions(
                group=pathname.split("/")[0], granule=granule
            )
            acq = [a for a in acqs if a.band_name == dataset.attrs["band_name"]][0]

            base_fname = "{}.TIF".format(splitext(basename(acq.uri))[0])
            match_dict = PATTERN1.match(base_fname).groupdict()
            fname = "{}{}_{}{}".format(
                match_dict.get("prefix"),
                product,
                match_dict.get("band_name"),
                match_dict.get("extension"),
            )
            rel_path = pjoin(product, re.sub(PATTERN2, ARD, fname))
            out_fname = pjoin(outdir, rel_path)

            _cogtif_args = get_cogtif_options(dataset, overviews=True)
            write_tif_from_dataset(dataset, out_fname, **_cogtif_args)

            # alias name for ODC metadata doc
            alias = _clean(ALIAS_FMT[product].format(dataset.attrs["alias"]))

            # Band Metadata
            rel_paths[alias] = get_img_dataset_info(dataset, rel_path)

    # retrieve metadata
    scalar_paths = find(h5group, "SCALAR")
    pathnames = [pth for pth in scalar_paths if "NBAR-METADATA" in pth]

    def tags():
        result = yaml.load(h5group[pathnames[0]][()])
        for path in pathnames[1:]:
            other = yaml.load(h5group[path][()])
            result["ancillary"].update(other["ancillary"])
        return result

    return tags(), rel_paths


def unpack_supplementary(container, granule, h5group, outdir):
    """
    Unpack the angles + other supplementary datasets produced by wagl.
    Currently only the mode resolution group gets extracted.
    """

    def _write(
        dataset_names, h5_group, granule_id, basedir, cogtif=False, cogtif_args=None
    ):
        """
        An internal util for serialising the supplementary
        H5Datasets to tif.
        """
        fmt = "{}_{}.TIF"
        paths = {}
        for dname in dataset_names:
            rel_path = pjoin(basedir, fmt.format(granule_id, dname.replace("-", "_")))
            out_fname = pjoin(outdir, rel_path)
            dset = h5_group[dname]
            alias = _clean(dset.attrs["alias"])
            paths[alias] = get_img_dataset_info(dset, rel_path)
            write_tif_from_dataset(dset, out_fname, **_cogtif_args)

        return paths

    acqs, res_grp = container.get_mode_resolution(granule)
    grn_id = re.sub(PATTERN2, ARD, granule)
    # Get tiling layout from mode resolution image, without overviews
    tileysize, tilexsize = acqs[0].tile_size
    _cogtif_args = get_cogtif_options(
        acqs[0].data(), overviews=False, blockxsize=tilexsize, blockysize=tileysize
    )
    del acqs

    # relative paths of each dataset for ODC metadata doc
    rel_paths = {}

    # satellite and solar angles
    grp = h5group[ppjoin(res_grp, GroupName.SAT_SOL_GROUP.value)]
    dnames = [
        DatasetName.SATELLITE_VIEW.value,
        DatasetName.SATELLITE_AZIMUTH.value,
        DatasetName.SOLAR_ZENITH.value,
        DatasetName.SOLAR_AZIMUTH.value,
        DatasetName.RELATIVE_AZIMUTH.value,
        DatasetName.TIME.value,
    ]
    paths = _write(dnames, grp, grn_id, SUPPS, cogtif=False, cogtif_args=_cogtif_args)
    for key in paths:
        rel_paths[key] = paths[key]

    # timedelta data
    timedelta_data = grp[DatasetName.TIME.value]

    # incident angles
    grp = h5group[ppjoin(res_grp, GroupName.INCIDENT_GROUP.value)]
    dnames = [DatasetName.INCIDENT.value, DatasetName.AZIMUTHAL_INCIDENT.value]
    paths = _write(dnames, grp, grn_id, SUPPS, cogtif=False, cogtif_args=_cogtif_args)
    for key in paths:
        rel_paths[key] = paths[key]

    # exiting angles
    grp = h5group[ppjoin(res_grp, GroupName.EXITING_GROUP.value)]
    dnames = [DatasetName.EXITING.value, DatasetName.AZIMUTHAL_EXITING.value]
    paths = _write(dnames, grp, grn_id, SUPPS, cogtif=False, cogtif_args=_cogtif_args)
    for key in paths:
        rel_paths[key] = paths[key]

    # relative slope
    grp = h5group[ppjoin(res_grp, GroupName.REL_SLP_GROUP.value)]
    dnames = [DatasetName.RELATIVE_SLOPE.value]
    paths = _write(dnames, grp, grn_id, SUPPS, cogtif=False, cogtif_args=_cogtif_args)
    for key in paths:
        rel_paths[key] = paths[key]

    # terrain shadow
    grp = h5group[ppjoin(res_grp, GroupName.SHADOW_GROUP.value)]
    dnames = [DatasetName.COMBINED_SHADOW.value]
    paths = _write(dnames, grp, grn_id, QA, cogtif=True, cogtif_args=_cogtif_args)
    for key in paths:
        rel_paths[key] = paths[key]

    # TODO do we also include slope and aspect?

    return rel_paths, timedelta_data


def create_contiguity(product_list, container, granule, outdir):
    """
    Create the contiguity (all pixels valid) dataset.
    """
    # quick decision to use the mode resolution to form contiguity
    # this rule is expected to change once more people get involved
    # in the decision making process
    acqs, _ = container.get_mode_resolution(granule)
    tileysize, tilexsize = acqs[0].tile_size
    _cogtif_args = get_cogtif_options(
        acqs[0].data(), blockxsize=tilexsize, blockysize=tileysize
    )
    _res = acqs[0].resolution
    del acqs

    grn_id = re.sub(PATTERN2, ARD, granule)

    nbar_contiguity = None
    # relative paths of each dataset for ODC metadata doc
    rel_paths = {}

    with tempfile.TemporaryDirectory(dir=outdir, prefix="contiguity-") as tmpdir:
        for product in product_list:
            search_path = pjoin(outdir, product)
            fnames = [
                str(f)
                for f in Path(search_path).glob("*.TIF")
                if "QUICKLOOK" not in str(f)
            ]

            # quick work around for products that aren't being packaged
            if not fnames:
                continue

            # output filename
            base_fname = "{}_{}_CONTIGUITY.TIF".format(grn_id, product)
            rel_path = pjoin(QA, base_fname)
            out_fname = pjoin(outdir, rel_path)

            if not exists(dirname(out_fname)):
                os.makedirs(dirname(out_fname))

            alias = ALIAS_FMT[product].format("contiguity")

            # temp vrt
            tmp_fname = pjoin(tmpdir, "{}.vrt".format(product))
            cmd = [
                "gdalbuildvrt",
                "-resolution",
                "user",
                "-tr",
                str(_res[1]),
                str(_res[0]),
                "-separate",
                tmp_fname,
            ]
            cmd.extend(fnames)
            run_command(cmd, tmpdir)

            # contiguity mask for nbar product
            contiguity_data, geobox = contiguity(tmp_fname)
            write_tif_from_dataset(
                contiguity_data, out_fname, geobox=geobox, **_cogtif_args
            )

            if base_fname.endswith("NBAR_CONTIGUITY.TIF"):
                nbar_contiguity = contiguity_data
            del contiguity_data

            with rasterio.open(out_fname) as ds:
                rel_paths[alias] = get_img_dataset_info(ds, rel_path)

    return rel_paths, nbar_contiguity


def create_quicklook(product_list, container: AcquisitionsContainer, outdir):
    """
    Create the quicklook and thumbnail images.
    """
    acq = container.get_mode_resolution()[0][0]
    tileysize, tilexsize = acq.tile_size
    gdal_settings = get_cogtif_options(
        acq.data(), overviews=True, blockxsize=tilexsize, blockysize=tileysize
    )

    # are quicklooks still needed?
    # this wildcard mechanism needs to change if quicklooks are to
    # persist
    band_wcards = {
        "LANDSAT_5": ["L*_B{}.TIF".format(i) for i in [3, 2, 1]],
        "LANDSAT_7": ["L*_B{}.TIF".format(i) for i in [3, 2, 1]],
        "LANDSAT_8": ["L*_B{}.TIF".format(i) for i in [4, 3, 2]],
        "SENTINEL_2A": ["*_B0{}.TIF".format(i) for i in [4, 3, 2]],
        "SENTINEL_2B": ["*_B0{}.TIF".format(i) for i in [4, 3, 2]],
    }

    # appropriate wildcards
    wcards = band_wcards[acq.platform_id]
    del acq

    def _process_quicklook(product, fnames, out_path, tmpdir):
        """
        Wrapper function to encapsulate gdal commands used to
        generate a quicklook for each product
        """
        # output filenames
        match = PATTERN1.match(fnames[0]).groupdict()
        out_fname1 = "{}{}{}".format(
            match.get("prefix"), "QUICKLOOK", match.get("extension")
        )
        out_fname2 = "{}{}{}".format(match.get("prefix"), "THUMBNAIL", ".JPG")

        # initial vrt of required rgb bands
        tmp_fname1 = pjoin(tmpdir, "{}.vrt".format(product))
        cmd = ["gdalbuildvrt", "-separate", "-overwrite", tmp_fname1]
        cmd.extend(fnames)
        run_command(cmd, tmpdir)

        # quicklook with contrast scaling
        tmp_fname2 = pjoin(tmpdir, "{}_{}.tif".format(product, "qlook"))
        quicklook(tmp_fname1, out_fname=tmp_fname2, src_min=1, src_max=3500, out_min=1)

        # warp to Lon/Lat WGS84
        tmp_fname3 = pjoin(tmpdir, "{}_{}.tif".format(product, "warp"))
        cmd = [
            "gdalwarp",
            "-t_srs",
            '"EPSG:4326"',
            "-co",
            "COMPRESS=JPEG",
            "-co",
            "PHOTOMETRIC=YCBCR",
            "-co",
            "TILED=YES",
            tmp_fname2,
            tmp_fname3,
        ]
        run_command(cmd, tmpdir)

        # build overviews/pyramids
        cmd = ["gdaladdo", "-r", "average", tmp_fname3]
        # Add levels
        cmd.extend([str(l) for l in LEVELS])
        run_command(cmd, tmpdir)

        # create the cogtif
        cmd = ["gdal_translate"]
        options_whitelist = ["blockxsize", "blockysize", "tiled", "copy_src_overviews"]
        for key, value in gdal_settings["options"].items():
            if key in options_whitelist:
                cmd.extend(["-co", "{}={}".format(key, value)])

        config_options_whitelist = ["GDAL_TIFF_OVR_BLOCKSIZE"]
        for key, value in gdal_settings["config_options"].items():
            if key in config_options_whitelist:
                cmd.extend(["--config", str(key), str(value)])

        cmd.extend(["-co", "COMPRESS=JPEG", "-co", "PHOTOMETRIC=YCBCR"])
        cmd.extend([tmp_fname3, out_fname1])

        run_command(cmd, tmpdir)

        # create the thumbnail
        cmd = [
            "gdal_translate",
            "-of",
            "JPEG",
            "-outsize",
            "10%",
            "10%",
            out_fname1,
            out_fname2,
        ]

        run_command(cmd, tmpdir)

    with tempfile.TemporaryDirectory(dir=outdir, prefix="quicklook-") as tmpdir:
        for product in product_list:
            if product == "SBT":
                # no sbt quicklook for the time being
                continue

            out_path = Path(pjoin(outdir, product))
            fnames = []
            for wcard in wcards:
                fnames.extend([str(f) for f in out_path.glob(wcard)])

            # quick work around for products that aren't being packaged
            if not fnames:
                continue
            _process_quicklook(product, fnames, out_path, tmpdir)


def create_readme(outdir):
    """
    Create the readme file.
    """
    with resource_stream("tesp", "_README.md") as src:
        with open(pjoin(outdir, "README.md"), "w") as out_src:
            out_src.writelines([l.decode("utf-8") for l in src.readlines()])


def create_checksum(outdir):
    """
    Create the checksum file.
    """
    out_fname = pjoin(outdir, "checksum.sha1")
    c = PackageChecksum()
    c.add(outdir)
    c.write(out_fname)
    return out_fname


def get_level1_tags(container, granule=None, yamls_path=None):
    _acq = container.get_all_acquisitions()[0]
    if yamls_path:
        # TODO define a consistent file structure where yaml metadata exists
        yaml_fname = pjoin(
            yamls_path,
            basename(dirname(_acq.pathname)),
            "{}.yaml".format(container.label),
        )

        # quick workaround if no source yaml
        if not exists(yaml_fname):
            raise IOError("yaml file not found: {}".format(yaml_fname))

        with open(yaml_fname, "r") as src:

            # TODO harmonise field names for different sensors

            l1_documents = {granule: doc for doc in yaml.load_all(src)}
            l1_tags = l1_documents[granule]
    else:
        docs = extract_level1_metadata(_acq)
        # Sentinel-2 may contain multiple scenes in a granule
        if isinstance(docs, list):
            l1_tags = [
                doc for doc in docs if doc.get("tile_id", doc.get("label")) == granule
            ][0]
        else:
            l1_tags = docs
    return l1_tags


def package(
    l1_path,
    antecedents,
    yamls_path,
    outdir,
    granule,
    products=("NBAR", "NBART", "LAMBERTIAN", "SBT"),
    acq_parser_hint=None,
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

    :param yamls_path:
        A string containing the full file pathname to the yaml
        documents for the indexed Level-1 datasets.

    :param outdir:
        A string containing the full file pathname to the directory
        that will contain the packaged Level-2 datasets.

    :param granule:
        The identifier for the granule

    :param products:
        A list of imagery products to include in the package.
        Defaults to all products.

    :param acq_parser_hint:
        A string that hints at which acquisition parser should be used.

    :return:
        None; The packages will be written to disk directly.
    """
    container: AcquisitionsContainer = acquisitions(l1_path, acq_parser_hint)
    l1_tags = get_level1_tags(container, granule, yamls_path)
    antecedent_metadata = {}

    # get sensor platform
    platform = get_platform(container, granule)

    with h5py.File(antecedents["wagl"], "r") as fid:
        grn_id = re.sub(PATTERN2, ARD, granule)
        out_path = pjoin(outdir, grn_id)

        if not exists(out_path):
            os.makedirs(out_path)

        # unpack the standardised products produced by wagl
        wagl_tags, img_paths = unpack_products(
            products, container, granule, fid[granule], out_path
        )

        # unpack supplementary datasets produced by wagl
        supp_paths, timedelta_data = unpack_supplementary(
            container, granule, fid[granule], out_path
        )

        # add in supplementary paths
        for key in supp_paths:
            img_paths[key] = supp_paths[key]

        # file based globbing, so can't have any other tifs on disk
        qa_paths, contiguity_ones_mask = create_contiguity(
            products, container, granule, out_path
        )

        # masking the timedelta_data with contiguity mask to get max and min timedelta within the NBAR product
        # footprint for Landsat sensor. For Sentinel sensor, it inherits from level 1 yaml file
        if platform == "LANDSAT":
            valid_timedelta_data = numpy.ma.masked_where(
                contiguity_ones_mask == 0, timedelta_data
            )
            wagl_tags["timedelta_min"] = numpy.ma.min(valid_timedelta_data)
            wagl_tags["timedelta_max"] = numpy.ma.max(valid_timedelta_data)

        # add in qa paths
        for key in qa_paths:
            img_paths[key] = qa_paths[key]

        # fmask cogtif conversion
        if "fmask" in antecedents:
            rel_path = pjoin(QA, "{}_FMASK.TIF".format(grn_id))
            fmask_cogtif_out = pjoin(out_path, rel_path)

            # Get cogtif args with overviews
            acq = container.get_mode_resolution(granule=granule)[0][0]
            tileysize, tilexsize = acq.tile_size
            fmask_cogtif_args = get_cogtif_options(
                acq.data(), blockxsize=tilexsize, blockysize=tileysize
            )

            # Set the predictor level
            fmask_cogtif_args["options"]["predictor"] = 2
            write_tif_from_file(
                antecedents["fmask"], fmask_cogtif_out, **fmask_cogtif_args
            )

            antecedent_metadata["fmask"] = get_fmask_metadata()

            with rasterio.open(fmask_cogtif_out) as ds:
                img_paths["fmask"] = get_img_dataset_info(ds, rel_path)

        create_quicklook(products, container, out_path)
        create_readme(out_path)

        # merge all the yaml documents
        if "gqa" in antecedents:
            with open(antecedents["gqa"]) as fl:
                antecedent_metadata["gqa"] = yaml.load(fl)
        else:
            antecedent_metadata["gqa"] = {
                "error_message": "GQA has not been configured for this product"
            }

        tags = merge_metadata(
            l1_tags, wagl_tags, granule, img_paths, platform, **antecedent_metadata
        )

        with open(pjoin(out_path, "ARD-METADATA.yaml"), "w") as src:
            yaml.dump(tags, src, default_flow_style=False, indent=4)

        # finally the checksum
        create_checksum(out_path)

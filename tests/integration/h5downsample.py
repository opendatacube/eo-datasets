"""
Make a HDF5 file much smaller by shrinking all embedded images.

This script:
1. overwrites the input data, and;
2. is intended for creating test datasets where the pixels don't
   matter much but the structure does. The downsampling is dirty.
"""
import re
import h5py
import click

from click import secho
from click import style
from pathlib import Path
from typing import List, Optional

from eodatasets3.ui import PathPath
from eodatasets3.wagl import find_a_granule_name


try:
    from sh import h5repack, gdal_translate
except ImportError:
    raise ImportError(
        "Unable to import h5repack/gdal_translate. "
        "Ensure Python's sh module is installed along "
        "with h5tools and gdal-bin"
    )


def find_h5_paths(h5_obj: h5py.Group, dataset_class: str = "") -> List[str]:
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


RES_GROUP_PATH = re.compile(r"(.*/RES-GROUP-\d+)/")


@click.command(help=__doc__)
@click.argument("input", type=PathPath(dir_okay=False, readable=True))
@click.option("--factor", type=int, default=100)
@click.option("--anti-alias/--no-anti-alias", is_flag=True, default=False)
def downsample(input: Path, factor: int, anti_alias: bool):

    granule_name = find_a_granule_name(input)
    fmask_image = input.with_name(f"{granule_name}.fmask.img")
    mndwi_image = input.with_name(f"{granule_name}.mndwi.h5")

    # list of dataset names excluded from the downsampling
    wagl_excl_dnames = ["NBAR", "NBART"]
    mndwi_excl_dnames = [
        "mndwi_image_LAMBERTIAN",
        "mndwi_image_LMBSKYG",
        "blue",
        "green",
        "red",
    ]

    # list required dataset names
    wagl_req_dnames = ["LMBSKYG"]
    mndwi_req_dnames = ["mndwi_image_LMBADJ"]

    # downsample mndwi.h5
    secho(f"Scaling mndwi {mndwi_image}")
    req_size = _downsample_h5_datasets(
        mndwi_image, factor, mndwi_excl_dnames, mndwi_req_dnames
    )

    # downsample wagl.h5
    secho(f"Scaling wagl {input}")
    req_size = _downsample_h5_datasets(input, factor, wagl_excl_dnames, wagl_req_dnames)

    if fmask_image.exists():
        secho(f"Scaling fmask {fmask_image}")
        tmp = fmask_image.with_suffix(f".tmp.{fmask_image.suffix}")
        gdal_translate("-outsize", req_size[1], req_size[0], fmask_image, tmp)
        tmp.rename(fmask_image)


def _downsample_h5_datasets(h5_fname, factor, excl_dnames=[], req_dnames=[]):

    with h5py.File(h5_fname, "r+") as fid:

        image_paths = find_h5_paths(fid, "IMAGE")
        npaths = len(image_paths)

        # ----------------------------------------- #
        #  assess if the required dataset(s) exist  #
        # ----------------------------------------- #
        if req_dnames:
            absent_ds = []
            for rq_dn in req_dnames:
                if not any(rq_dn in img_path for img_path in image_paths):
                    absent_ds.append(rq_dn)
            if absent_ds:
                raise ValueError("{0} image(s) not found".format(", ".join(absent_ds)))

        # --------------------------------------------------- #
        #  iterate and downscale, skipping excluded datasets  #
        # --------------------------------------------------- #
        req_size = None
        for i, img_path in enumerate(image_paths):

            if excl_dnames:
                # delete excluded dataset names if specified
                if any(ele in img_path for ele in excl_dnames):
                    del fid[str(img_path)]
                    continue

            old_image: Optional[h5py.Dataset] = fid[img_path]
            old_shape = old_image.shape

            info_str = f"{i: 4}/{npaths} {style(repr(img_path), fg='blue')}"

            if all(dim_size < factor for dim_size in old_shape):
                secho(f"{info_str}: Skipping")
                continue

            attrs = dict(old_image.attrs.items())
            old_geotransform = attrs["geotransform"]

            new_data = old_image[()][::factor, ::factor]
            new_shape = new_data.shape
            secho(f"{info_str}: New shape: {new_shape!r}")
            del old_image
            del fid[str(img_path)]

            folder, name = img_path.rsplit("/", 1)
            parent: h5py.Group = fid[str(folder)]

            image = parent.create_dataset(name, new_shape, data=new_data)
            new_geotransform = list(old_geotransform)
            new_geotransform[1] *= old_shape[1] / new_shape[1]
            new_geotransform[5] *= old_shape[0] / new_shape[0]
            attrs["geotransform"] = new_geotransform

            # update resolution attr if it exists
            res_key = [key for key in attrs.keys() if "resolution" in key]
            if res_key:
                attrs[res_key[0]] = abs(new_geotransform[5])

            # update dataset attrs
            image.attrs.update(attrs)

            # Update any res group with the new resolution.
            res_group_path = _get_res_group_path(img_path)
            if res_group_path:
                res_group = fid[res_group_path]
                res_group.attrs["resolution"] = [
                    abs(new_geotransform[5]),
                    abs(new_geotransform[1]),
                ]

            if req_dnames:
                if req_dnames[0] in img_path:
                    req_size = new_shape

    h5fn_repacked = h5_fname.with_suffix(".repacked.h5")
    h5repack("-f", "GZIP=5", h5_fname, h5fn_repacked)
    h5fn_repacked.rename(h5_fname)

    return req_size


def _get_res_group_path(image_path: str) -> Optional[str]:
    """
    >>> _get_res_group_path('LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBART/BAND-7')
    'LC80920842016180LGN01/RES-GROUP-1'
    >>> # Nothing if not in a res group.
    >>> _get_res_group_path('LC80920842016180LGN01/SATELLITE-SOLAR/SOLAR-ZENITH')
    """
    m = RES_GROUP_PATH.match(image_path)
    if m:
        return m.group(1)
    return None


if __name__ == "__main__":
    downsample()

"""
Make a HDF5 file much smaller by shrinking all embedded images.

(Intended for creating test datasets where the pixels don't
matter much but the structure does. The downsampling is dirty.)
"""
from collections import Counter
from pathlib import Path
from typing import List, Optional

import click
import h5py
from click import secho
from click import style
from skimage.transform import resize

from eodatasets2.ui import PathPath


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


@click.command(help=__doc__)
@click.argument("input", type=PathPath(dir_okay=False, readable=True))
@click.option("--factor", type=int, default=100)
def downsample(input: Path, factor: int):
    # Fail early if h5repack cli command is not available.
    from sh import h5repack

    # granule_name = _find_a_granule_name(input)
    # fmask_image = input.with_name(f"{granule_name}.fmask.img")

    sizes = Counter()
    with h5py.File(input) as f:
        image_paths = find_h5_paths(f, "IMAGE")

        for i, image_path in enumerate(image_paths):
            old_image: Optional[h5py.Dataset] = f[image_path]

            def info(msg: str):
                secho(
                    f"{i: 4}/{len(image_paths)} {style(repr(image_path), fg='blue')}: {msg}"
                )

            old_shape = old_image.shape
            if all(dim_size < factor for dim_size in old_shape):
                info(f"Skipping")
                continue

            new_shape = (old_shape[0] // factor, old_shape[1] // factor)
            info(f"New shape: {new_shape!r}")

            old_attrs = dict(old_image.attrs.items())
            new_data = resize(old_image[()], new_shape, anti_aliasing=False)
            old_image = None
            del f[str(image_path)]

            folder, name = image_path.rsplit("/", 1)
            parent: h5py.Group = f[str(folder)]

            image = parent.create_dataset(name, new_shape, data=new_data)
            old_attrs["geotransform"][1] *= old_shape[1] / new_shape[1]
            old_attrs["geotransform"][5] *= old_shape[0] / new_shape[0]
            image.attrs.update(old_attrs)

            sizes[new_shape] += 1

    # We need to repack the file to actually free up the space.
    repacked = input.with_suffix(".repacked.h5")
    h5repack("-f", "GZIP=5", input, repacked)
    repacked.rename(input)

    # The fmask is already small. Don't bother.
    # if fmask_image.exists():
    #     echo(f"Scaling fmask {fmask_image}")
    #     tmp = fmask_image.with_suffix(f'.tmp.{fmask_image.suffix}')
    #     gdal_translate('-outsize', 77, 77, fmask_image, tmp)
    #     tmp.rename(fmask_image)


if __name__ == "__main__":
    downsample()

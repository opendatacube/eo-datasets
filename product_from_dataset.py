import click
import fsspec
import yaml
import sys


@click.command()
@click.argument("input")
def convert(input):
    with fsspec.open(input) as fp:
        dataset = yaml.safe_load(fp)

    product = {
        "name": dataset["product"]["name"],
        "metadata_type": "eo3_landsat_ard",
        "metadata": {"product": dataset["product"]},
        "description": "Sample USGS Level 2 Collection 2 Landsat",
        "measurements": [
            {"name": k, "dtype": "int16", "nodata": 0, "units": "1", "aliases": []}
            for k in dataset["measurements"].keys()
        ],
    }

    yaml.safe_dump(product, sys.stdout)


if __name__ == "__main__":
    convert()

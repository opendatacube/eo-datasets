import datetime
import re
import uuid
from pathlib import Path
from typing import Iterable, Dict
from defusedxml import ElementTree

import click
import rasterio

from eodatasets3 import serialise
from eodatasets3.utils import read_paths_from_file, ItemProvider
from ..metadata.valid_region import valid_region

MCD43A1_NS = uuid.UUID(hex="80dc431b-fc6c-4e6f-bf08-585eba1d8dc9")


def parse_xml(filepath: Path):
    """
    Extracts metadata attributes from the xml document distributed
    alongside the MCD43A1 tiles.
    """
    root = ElementTree.parse(str(filepath), forbid_dtd=True).getroot()

    granule_id = root.find("*//ECSDataGranule/LocalGranuleID").text
    instrument = root.find("*//Platform/Instrument/InstrumentShortName").text
    platform = "+".join(
        sorted(
            (ele.text for ele in root.findall("*//Platform/PlatformShortName")),
            reverse=True,
        )
    )
    start_date = root.find("*//RangeDateTime/RangeBeginningDate").text
    start_time = root.find("*//RangeDateTime/RangeBeginningTime").text
    end_date = root.find("*//RangeDateTime/RangeEndingDate").text
    end_time = root.find("*//RangeDateTime/RangeEndingTime").text
    v_tile = (
        [
            ele
            for ele in root.findall("*//PSA")
            if ele.find("PSAName").text == "VERTICALTILENUMBER"
        ][0]
        .find("PSAValue")
        .text
    )
    h_tile = (
        [
            ele
            for ele in root.findall("*//PSA")
            if ele.find("PSAName").text == "HORIZONTALTILENUMBER"
        ][0]
        .find("PSAValue")
        .text
    )

    creation_dt = root.find("*//InsertTime").text

    return {
        "granule_id": granule_id,
        "instrument": instrument,
        "platform": platform,
        "vertical_tile": int(v_tile),
        "horizontal_tile": int(h_tile),
        "from_dt": (
            datetime.datetime.strptime(
                start_date + " " + start_time, "%Y-%m-%d %H:%M:%S.%f"
            ).replace(tzinfo=datetime.timezone.utc)
        ),
        "to_dt": (
            datetime.datetime.strptime(
                end_date + " " + end_time, "%Y-%m-%d %H:%M:%S.%f"
            ).replace(tzinfo=datetime.timezone.utc)
        ),
        "creation_dt": (
            datetime.datetime.strptime(creation_dt, "%Y-%m-%d %H:%M:%S.%f").replace(
                tzinfo=datetime.timezone.utc
            )
        ),
    }


def get_band_info(imagery_file: Path):
    """
    Summarises the available image bands for indexing into datacube
    Separate references are provided for each of the brdf parameter bands:
        volumetric (vol), isometric (iso) and geometric (geo)

    """
    band_info = {}
    with rasterio.open(imagery_file, "r") as collection:
        datasets = collection.subdatasets
        for ds in datasets:
            raster_params = re.match(
                "(?P<fmt>HDF4_EOS:EOS_GRID):(?P<path>[^:]+):(?P<layer>.*)$", ds
            )
            if "_Quality_" in raster_params["layer"]:
                name = raster_params["layer"].split(":")[-1]
                band_info[name] = {
                    "path": Path(raster_params["path"]).name,
                    "layer": raster_params["layer"],
                }
            else:
                name = raster_params["layer"].split(":")[-1]
                # BRDF parameter bands are isotropic, volumetric and geometric
                for idx, band_name in enumerate(["iso", "vol", "geo"], 1):
                    band_info[name + "_" + band_name] = {
                        "path": Path(raster_params["path"]).name,
                        "layer": raster_params["layer"],
                        "band": idx,
                    }
    return band_info, datasets


def _get_dataset_properties(rasterio_path: str):
    """
    returns dataset properties based on a sample dataset
    """
    props = {}
    with rasterio.open(rasterio_path, "r") as ds:
        props["eo:gsd"] = float(ds.tags()["CHARACTERISTICBINSIZE"])
        props["grids"] = {
            "default": {"shape": list(ds.shape), "transform": list(ds.transform)}
        }
        props["crs"] = ds.crs.wkt

    return props


def process_datasets(input_path: Path, xml_file: Path) -> Iterable[Dict]:
    """
    Generates a metadata document for each tile provided,
    requires a path to the input tile (hdf) and the
    corresponding xml document describing the dataset.
    """
    band_info, datasets = get_band_info(input_path)
    xml_md = parse_xml(xml_file)
    ds_props = _get_dataset_properties(datasets[0])

    md = {}
    md["id"] = str(uuid.uuid5(MCD43A1_NS, xml_md["granule_id"]))
    md["product"] = {"href": "https://collections.dea.ga.gov.au/nasa_c_m_mcd43a1_6"}
    md["crs"] = ds_props.pop("crs")
    md["geometry"] = valid_region(datasets)
    md["grids"] = ds_props.pop("grids")
    md["lineage"] = {}
    md["measurements"] = band_info
    md["properties"] = {
        "dtr:start_datetime": xml_md["from_dt"].isoformat(),
        "dtr:end_datetime": xml_md["to_dt"].isoformat(),
        "eo:instrument": xml_md["instrument"],
        "eo:platform": xml_md["platform"],
        "eo:gsd": ds_props.pop("eo:gsd"),
        "eo:epsg": None,
        "item:providers": [
            {
                "name": "National Aeronautics and Space Administration",
                "roles": [ItemProvider.PRODUCER.value, ItemProvider.PROCESSOR.value],
                "url": "https://modis.gsfc.nasa.gov/data/dataprod/mod43.php",
            },
            {
                "name": "United States Geological Society",
                "roles": [ItemProvider.PROCESSOR.value],
                "url": "https://lpdaac.usgs.gov/products/mcd43a1v006/",
            },
        ],
        "odc:creation_datetime": xml_md["creation_dt"].isoformat(),
        "odc:file_format": "HDF4_EOS:EOS_GRID",
        "odc:region_code": "h{}v{}".format(
            xml_md["horizontal_tile"], xml_md["vertical_tile"]
        ),
    }

    return [md]


def _process_datasets(output_dir, datasets, checksum):
    """
    Wrapper function for processing multiple datasets
    """
    for dataset in datasets:
        docs = process_datasets(dataset, Path(str(dataset) + ".xml"))
        outfile = output_dir / (dataset.stem + ".ga-md.yaml")
        serialise.dump_yaml(outfile, *docs)


@click.command(
    help="""\b
        Prepare MODIS MCD43A1 tiles for indexing into a Data Cube.
        This prepare script supports the HDF4_EOS:EOS_GRID datasets
            with associated xml documents

        Example usage: yourscript.py --output [directory] input_file1 input_file2"""
)
@click.option(
    "--output",
    "output_dir",
    help="Write datasets into this directory",
    type=click.Path(exists=False, writable=True, dir_okay=True),
)
@click.argument(
    "datasets", type=click.Path(exists=True, readable=True, writable=False), nargs=-1
)
@click.option(
    "--checksum/--no-checksum",
    help="Checksum the input dataset to confirm match",
    default=False,
)
@click.option(
    "-f",
    "dataset_listing_files",
    type=click.Path(exists=True, readable=True, writable=False),
    help="file containing a list of input paths (one per line)",
    multiple=True,
)
def main(output_dir, datasets, checksum, dataset_listing_files):
    datasets = [Path(p) for p in datasets]
    for listing_file in dataset_listing_files:
        datasets.extend(read_paths_from_file(Path(listing_file)))

    return _process_datasets(Path(output_dir), datasets, checksum)


if __name__ == "__main__":
    main()

"""
Prepare eo3 metadata for Sentinel-2 Level 1C data produced by Sinergise or esa.

Takes ESA zipped datasets or Sinergise dataset directories
"""
import dataclasses
import fnmatch
import json
import logging
import re
import sys
import uuid
import warnings
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

import click
from defusedxml import minidom

from eodatasets3 import DatasetPrepare
from eodatasets3.properties import Eo3Interface
from eodatasets3.ui import PathPath

# Static namespace to generate uuids for datacube indexing
S2_UUID_NAMESPACE = uuid.UUID("9df23adf-fc90-4ec7-9299-57bd536c7590")

_LOG = logging.getLogger("sentinel-l1")

SENTINEL_MSI_BAND_ALIASES = {
    "01": "coastal_aerosol",
    "02": "blue",
    "03": "green",
    "04": "red",
    "05": "red_edge_1",
    "06": "red_edge_2",
    "07": "red_edge_3",
    "08": "nir_1",
    "8A": "nir_2",
    "09": "water_vapour",
    "10": "swir_1_cirrus",
    "11": "swir_2",
    "12": "swir_3",
}


def process_sinergise_product_info(product_path: Path) -> Dict:
    with product_path.open() as fp:
        product = json.load(fp)

    if len(product["tiles"]) > 1:
        raise NotImplementedError("No support for multi-tiled products yet")
    tile = product["tiles"][0]

    utm_zone = tile["utmZone"]
    latitude_band = tile["latitudeBand"]
    grid_square = tile["gridSquare"]
    return {
        "sentinel:product_name": product["name"],
        "sinergise_product_id": product["id"],
        "odc:region_code": f"{utm_zone}{latitude_band}{grid_square}",
        "sentinel:utm_zone": utm_zone,
        "sentinel:latitude_band": latitude_band,
        "sentinel:grid_square": grid_square,
    }


def _node_name(el):
    """Get a human-readable path to the given xml node"""
    branch = [el]
    while branch[-1].parentNode is not None:
        branch.append(branch[-1].parentNode)

    branch.reverse()
    return "/".join(n.localName for n in branch[1:])


def _value(root, *tags: str, type_=None):
    """Get the text contents of a tag in an xml document.

    Takes the tag name(s) to search for.
    """
    el = root

    for tag in tags:
        found = el.getElementsByTagName(tag)
        if not found:
            raise ValueError(f"Element not found in document: {tags!r}")
        if len(found) > 1:
            vs = "\n\t".join([_node_name(e) for e in found])
            raise ValueError(f"Multiple matches found for tag: {tags!r}: \n\t{vs}")

        el = found[0]

    el.normalize()
    if not len(el.childNodes) == 1:
        raise NotImplementedError(
            f"Not yet supported: Multiple child tags found for {tags!r}"
        )
    value = el.firstChild.data.strip()

    if not value:
        raise ValueError(f"Empty value for document at {tags!r}")

    if type_ is not None:
        value = type_(value)
    return value


def process_tile_metadata(contents: str) -> Dict:
    """
    Tile xml metadata format, as described by
    xmlns https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-1C_Tile_Metadata.xsd
    """
    root = minidom.parseString(contents)

    resolution = min(
        int(i.attributes["resolution"].value) for i in root.getElementsByTagName("Size")
    )
    return {
        "datetime": _value(root, "SENSING_TIME"),
        "eo:cloud_cover": _value(root, "CLOUDY_PIXEL_PERCENTAGE", type_=float),
        "eo:gsd": resolution,
        "eo:sun_azimuth": _value(root, "Mean_Sun_Angle", "AZIMUTH_ANGLE", type_=float),
        "eo:sun_elevation": _value(root, "Mean_Sun_Angle", "ZENITH_ANGLE", type_=float),
        "odc:processing_datetime": _value(root, "ARCHIVING_TIME"),
        "sentinel:datastrip_id": _value(root, "DATASTRIP_ID"),
        "sentinel:sentinel_tile_id": _value(root, "TILE_ID"),
    }


def process_datastrip_metadata(contents: str) -> Dict:
    """
    Datastrip metadata format, as described by
    xmlns https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-1C_Datastrip_Metadata.xsd

    """
    root = minidom.parseString(contents)

    resolution = min(
        int(i.firstChild.data) for i in root.getElementsByTagName("RESOLUTION")
    )
    return {
        "sentinel:reception_station": _value(root, "RECEPTION_STATION"),
        "sentinel:processing_center": _value(root, "PROCESSING_CENTER"),
        "eo:gsd": resolution,
    }


def process_user_product_metadata(contents: str) -> Dict:
    root = minidom.parseString(contents)

    product_uri = _value(root, "PRODUCT_URI").split(".")[0]
    region_code = product_uri.split("_")[5][1:]
    return {
        "eo:platform": _value(root, "SPACECRAFT_NAME"),
        "sat:relative_orbit": _value(root, "SENSING_ORBIT_NUMBER", type_=int),
        "sat:orbit_state": _value(root, "SENSING_ORBIT_DIRECTION").lower(),
        "sentinel:datatake_type": _value(root, "DATATAKE_TYPE"),
        "sentinel:processing_baseline": _value(root, "PROCESSING_BASELINE"),
        "sentinel:product_name": product_uri,
        "eo:cloud_cover": _value(root, "Cloud_Coverage_Assessment"),
        "odc:region_code": region_code,
    }


def _get_stable_id(p: Eo3Interface) -> uuid.UUID:
    # These should have been extracted from our metadata files!
    producer = p.producer
    product_name = p.properties["sentinel:product_name"]
    tile_id = p.properties["sentinel:sentinel_tile_id"]
    return uuid.uuid5(
        S2_UUID_NAMESPACE,
        ":".join((producer, product_name, tile_id)),
    )


def prepare_and_write(
    dataset_location: Path,
    output_yaml: Path,
    producer: str,
    embed_location: bool = None,
) -> Tuple[uuid.UUID, Path]:

    if embed_location is None:
        # Default to embedding the location if they're not in the same folder.
        embed_location = dataset_location.parent != output_yaml.parent
        _LOG.debug(
            "Auto-embed location? %s: %s %s %s",
            "Yes" if embed_location else "No",
            dataset_location.parent,
            "!=" if embed_location else "==",
            output_yaml.parent,
        )

    with DatasetPrepare(
        metadata_path=output_yaml,
        dataset_location=dataset_location,
    ) as p:
        p.properties["odc:producer"] = producer

        if producer == "esa.int":
            jp2_offsets = _extract_esa_fields(dataset_location, p)
        elif producer == "sinergise.com":
            jp2_offsets = _extract_sinergise_fields(dataset_location.parent, p)
        else:
            raise NotImplementedError(
                f"Unknown s2 producer {producer}. Expected 'sinergise.com' or 'esa.int'"
            )

        p.dataset_id = _get_stable_id(p)

        p.platform = _get_platform_name(p.properties)
        p.instrument = "MSI"
        p.constellation = "sentinel-2"

        p.dataset_version = f"1.0.{p.processed:%Y%m%d}"

        p.properties["odc:file_format"] = "JPEG2000"
        p.product_family = "level1"

        for path in jp2_offsets:
            band_number = _extract_band_number(path.stem)
            if band_number.lower() in ("tci", "pvi", "preview"):
                continue
            if band_number not in SENTINEL_MSI_BAND_ALIASES:
                raise RuntimeError(
                    f"Unknown band number {band_number!r} in image {path}"
                )

            p.note_measurement(
                path=path,
                name=SENTINEL_MSI_BAND_ALIASES[band_number],
                relative_to_dataset_location=True,
            )

        return p.done(embed_location=embed_location)


def _get_platform_name(p: Mapping) -> str:
    # Grab it from the datastrip id,
    # Eg. S2B_OPER_MSI_L1C_DS_VGS4_20210426T010904_S20210425T235239_N03

    datastrip_id: str = p.get("sentinel:datastrip_id")
    if not datastrip_id:
        raise ValueError(
            "Could not find a sentinel datastrip ID after reading all metadata documents"
        )
    if not datastrip_id.lower().startswith("s2"):
        raise RuntimeError("Expected sentinel datastrip-id to start with 's2'!")
    platform_variant = datastrip_id[1:3].lower()
    platform_name = f"sentinel-{platform_variant}"
    return platform_name


def _extract_sinergise_fields(path: Path, p: DatasetPrepare) -> Iterable[Path]:
    """Extract Sinergise metadata and return list of image offsets"""
    product_info_path = path / "productInfo.json"
    metadata_xml_path = path / "metadata.xml"

    if not product_info_path.exists():
        raise ValueError(
            "No productInfo.json file found. "
            "Are you sure the input is a sinergise dataset folder?"
        )

    p.properties.update(process_sinergise_product_info(product_info_path))
    p.note_accessory_file("metadata:sinergise_product_info", product_info_path)

    p.properties.update(process_tile_metadata(metadata_xml_path.read_text()))
    p.note_accessory_file("metadata:s2_tile", metadata_xml_path)

    # TODO: sinergise folders could `process_datastrip_metadata()` in an outer directory?

    return list(path.glob("*.jp2"))


def _extract_esa_fields(dataset, p) -> Iterable[Path]:
    """Extract ESA metadata and return list of image offsets"""
    with zipfile.ZipFile(dataset, "r") as z:

        def one(suffix: str) -> str:
            """Find one path ending in the given name"""
            matches = [s for s in z.namelist() if s.endswith(suffix)]
            if len(matches) != 1:
                raise ValueError(
                    f"Expected exactly one file called {suffix} in {dataset}, found {len(matches)}"
                )
            return matches[0]

        datastrip_md = one("MTD_DS.xml")
        p.properties.update(
            process_datastrip_metadata(z.read(datastrip_md).decode("utf-8"))
        )
        p.note_accessory_file("metadata:s2_datastrip", datastrip_md)

        tile_md = one("MTD_TL.xml")
        p.properties.update(process_tile_metadata(z.read(tile_md).decode("utf-8")))
        p.note_accessory_file("metadata:s2_tile", tile_md)

        user_product_md = one("MTD_MSIL1C.xml")
        p.properties.update(
            process_user_product_metadata(z.read(user_product_md).decode("utf-8"))
        )
        p.note_accessory_file("metadata:s2_user_product", user_product_md)

        return [Path(p) for p in z.namelist() if "IMG_DATA" in p and p.endswith(".jp2")]


def _extract_band_number(name: str) -> str:
    """
    >>> _extract_band_number('B03')
    '03'
    >>> _extract_band_number('T55HFA_20201011T000249_B01')
    '01'
    """
    return name.split("_")[-1].replace("B", "")


def _rglob_with_self(path: Path, pattern: str) -> Iterable[Path]:
    if fnmatch.fnmatch(path.name, pattern):
        yield path
        return
    yield from path.rglob(pattern)


@dataclasses.dataclass
class FolderInfo:
    """
    Information extracted from a standard S2 folder layout.
    """

    year: int
    month: int
    region_code: str

    # Compiled regexp for extracting year, month and region
    # Standard layout is of the form: 'L1C/{yyyy}/{yyyy}-{mm}/{area}/S2*_{region}_{timestamp}(.zip)'
    STANDARD_SUBFOLDER_LAYOUT = re.compile(
        r"(\d{4})/(\d{4})-(\d{2})/[\dSE]+-[\dSE]+/S2[AB]_MSIL1C_[^/]+_T([A-Z\d]+)_[\dT]+(\.zip)?$"
    )

    @classmethod
    def for_path(cls, path: Path) -> Optional["FolderInfo"]:
        """
        Can we extract information from the given path?

        Returns None if we can't.
        """
        m = cls.STANDARD_SUBFOLDER_LAYOUT.search(path.as_posix())
        if not m:
            return None

        year, year2, month, region_code, extension = m.groups()
        if year != year2:
            raise ValueError(f"Year mismatch in {path}")

        return FolderInfo(int(year), int(month, 10), region_code)


class YearMonth(click.ParamType):
    """A YYYY-MM string converted to an integer tuple"""

    name = "year-month"

    def convert(self, value, param, ctx):
        if value is None:
            return None
        hp = value.split("-")
        if len(hp) != 2:
            self.fail("Expect value in YYYY-MM format")

        year, month = hp
        if not year.isdigit() or not month.isdigit():
            self.fail("Expect value in YYYY-MM format")

        return int(year), int(month)


@click.command(help=__doc__)
@click.option("-v", "--verbose", is_flag=True)
@click.argument(
    "datasets",
    type=PathPath(exists=True, readable=True, writable=False, resolve_path=True),
    nargs=-1,
)
@click.option(
    "--overwrite-existing/--skip-existing",
    is_flag=True,
    default=False,
    help="Overwrite if exists (otherwise skip)",
)
@click.option(
    "--embed-location/--no-embed-location",
    is_flag=True,
    default=None,
    help="Embed the location of the dataset in the metadata? "
    "(if you wish to store them separately. default: auto)",
)
@click.option(
    "--provider",
    default=None,
    type=click.Choice(
        [
            "sinergise.com",
            "esa.int",
        ]
    ),
    help="Restrict scanning to only packages of the given provider. "
    "(ESA assumes a zip file, sinergise a directory)",
)
@click.option(
    "--output-base",
    help="Write metadata files into a directory instead of alongside each dataset",
    required=False,
    type=PathPath(
        exists=True, writable=True, dir_okay=True, file_okay=False, resolve_path=True
    ),
)
@click.option(
    "--input-relative-to",
    help="Input root folder that should be used for the subfolder hierarchy in the output-base",
    required=False,
    type=PathPath(
        exists=True, writable=True, dir_okay=True, file_okay=False, resolve_path=True
    ),
)
@click.option(
    "--limit-regions-file",
    help="A file containing the list of region codes to limit the scan to",
    required=False,
    type=PathPath(
        exists=True, readable=True, dir_okay=False, file_okay=True, resolve_path=True
    ),
)
@click.option(
    "--after-month",
    help="Limit the scan to datasets newer than a given month "
    "(expressed as {year}-{month}, eg '2010-01')",
    required=False,
    type=YearMonth(),
)
@click.option(
    "--before-month",
    help="Limit the scan to datasets older than the given month "
    "(expressed as {year}-{month}, eg '2010-01')",
    required=False,
    type=YearMonth(),
)
def main(
    output_base: Optional[Path],
    input_relative_to: Optional[Path],
    datasets: List[Path],
    provider: Optional[str],
    overwrite_existing: bool,
    verbose: bool,
    embed_location: Optional[bool],
    limit_regions_file: Optional[Path],
    before_month: Optional[Tuple[int, int]],
    after_month: Optional[Tuple[int, int]],
):
    if sys.argv[1] == "sentinel-l1c":
        warnings.warn(
            "Command name 'sentinel-l1c-prepare' is deprecated: remove the 'c', and use `sentinel-l1-prepare`"
        )

    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
    _LOG.setLevel(logging.DEBUG if verbose else logging.INFO)

    limit_regions = None
    if limit_regions_file:
        limit_regions = set(limit_regions_file.read_text().splitlines())

    for i, input_path in enumerate(datasets):

        info = FolderInfo.for_path(input_path)

        # Skip regions that are not in the limit?
        if limit_regions or before_month or after_month:
            if info is None:
                raise ValueError(
                    f"Cannot filter from non-standard folder layout: {input_path}"
                )

            if limit_regions:
                if info.region_code in limit_regions:
                    _LOG.debug(
                        f"Skipping because region {info.region_code!r} is in region filter"
                    )
                    continue

            if after_month is not None:
                year, month = after_month

                if info.year < year or (info.year == year and info.month < month):
                    _LOG.debug(
                        f"Skipping because year {info.year}-{info.month} is older than {year}-{month}"
                    )
                    continue
            if before_month is not None:
                year, month = before_month

                if info.year > year or (info.year == year and info.month > month):
                    _LOG.debug(
                        f"Skipping because year {info.year}-{info.month} is newer than {year}-{month}"
                    )
                    continue

        # The default input_relative path is a parent folder named 'L1C'.
        if output_base and (i == 0 and input_relative_to is None):
            for parent in input_path.parents:
                if parent.name.lower() == "l1c":
                    input_relative_to = parent
                    break
            else:
                raise ValueError(
                    "Unknown root folder for path subfolders. "
                    "(Hint: specify --input-relative-to with a parent folder of the inputs. "
                    "Outputs will use the same subfolder structure.)"
                )

        in_out_paths = []

        if provider == "sinergise.com" or not provider:
            in_out_paths.extend(
                (
                    "sinergise.com",
                    # Dataset location is the metadata file itself.
                    p,
                    # Output is an inner metadata file, with the same name as the folder (usually S2A....).
                    (p.parent / f"{p.parent.stem}.odc-metadata.yaml"),
                )
                for p in _rglob_with_self(input_path, "tileInfo.json")
            )
        if provider == "esa.int" or not provider:
            in_out_paths.extend(
                (
                    "esa.int",
                    # Dataset location is the zip file
                    p,
                    # Metadata is a sibling file with a metadata suffix.
                    p.with_suffix(".odc-metadata.yaml"),
                )
                for p in _rglob_with_self(input_path, "*.zip")
            )

        if not in_out_paths:
            raise ValueError(
                f"No S2 datasets found in given path {input_path}. "
                f"Expected either Sinergise (productInfo.json) files or ESA zip files to be contained in it."
            )

        for producer, ds_path, output_yaml in in_out_paths:
            if output_base:
                output_folder = output_base.absolute()
                # If we want to copy the input folder hierarchy
                if input_relative_to:
                    output_folder = output_folder / input_path.parent.relative_to(
                        input_relative_to
                    )

                output_yaml = output_folder / output_yaml.name

            if output_yaml.exists():
                if not overwrite_existing:
                    _LOG.info("Output exists: skipping. %s", output_yaml)
                    continue

                _LOG.info("Output exists: overwriting %s", output_yaml)

            uuid_, path = prepare_and_write(
                ds_path, output_yaml, producer, embed_location=embed_location
            )
            _LOG.info("Wrote dataset %s to %s", uuid_, path)

    sys.exit(0)


if __name__ == "__main__":
    main()

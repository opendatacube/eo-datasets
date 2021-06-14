import re
from datetime import datetime
from pathlib import Path
from typing import (
    Optional,
    Sequence,
    Dict,
    Mapping,
    Set,
)

from eodatasets3 import utils
from eodatasets3.model import DEA_URI_PREFIX
from eodatasets3.properties import EoFields, Eo3Properties


def convention(props: Mapping, kind: str):
    """
    Get naming conventions for the given properties.
    """
    conventions = dict(
        default=NamingConventions,
        dea=DEANamingConventions,
        dea_s2=DEAS2NamingConventions,
        dea_s2_derivative=DEAS2DerivativesNamingConventions,
        dea_c3=DEADerivativesNamingConventions,
        deafrica=DEAfricaNamingConventions,
    )
    if kind not in conventions:
        available = ", ".join(conventions.keys())
        raise ValueError(
            f"Unknown naming conventions: {kind}. Possibilities: {available}"
        )

    return conventions[kind](props)


class LazyProductName:
    def __init__(self, include_instrument=True, include_collection=False) -> None:
        super().__init__()
        self.include_instrument = include_instrument
        self.include_collection = include_collection

    def __get__(self, c: "NamingConventions", owner) -> str:
        if c.dataset.product_name:
            return c.dataset.product_name

        instrument = c.instrument_abbreviated if self.include_instrument else ""
        return "_".join(
            p
            for p in (
                c.producer_abbreviated,
                f"{c.platform_abbreviated or ''}{instrument or ''}",
                c.dataset.product_family,
                (
                    f"{c.displayed_collection_number}"
                    if (self.include_collection and c.displayed_collection_number)
                    else None
                ),
            )
            if p
        )


class LazyFilePattern:
    def __get__(self, c: "NamingConventions", owner) -> str:
        return c.dataset_label + "{file_id}.{suffix}"


def _strip_major_version(version: str) -> str:
    """
    >>> _strip_major_version('1.2.3')
    '2.3'
    >>> _strip_major_version('01.02.03')
    '02.03'
    >>> _strip_major_version('30.40')
    '40'
    >>> _strip_major_version('40')
    ''
    """
    return ".".join(version.split(".")[1:])


class LazyLabel:
    def __init__(self, include_version=True, strip_major_version=False) -> None:
        super().__init__()
        self.strip_major_version = strip_major_version
        self.include_version = include_version

    def __get__(self, c: "NamingConventions", owner):
        d = c.dataset

        product_prefix = c.product_name

        version = d.dataset_version
        if version and self.include_version:
            if self.strip_major_version:
                version = _strip_major_version(version)
            version = version.replace(".", "-")
            product_prefix = f"{c.product_name}-{version}"

        maturity: str = d.properties.get("dea:dataset_maturity")
        return "_".join(
            [
                p
                for p in (
                    product_prefix,
                    d.region_code,
                    f"{d.datetime:%Y-%m-%d}",
                    maturity,
                )
                if p
            ]
        )


class LazyPlatformAbbreviation:
    # The abbreviations mentioned in DEA naming conventions doc.
    KNOWN_PLATFORM_ABBREVIATIONS = {
        "landsat-5": "ls5",
        "landsat-7": "ls7",
        "landsat-8": "ls8",
        "landsat-9": "ls9",
        "sentinel-1a": "s1a",
        "sentinel-1b": "s1b",
        "sentinel-2a": "s2a",
        "sentinel-2b": "s2b",
        "aqua": "aqu",
        "terra": "ter",
    }

    # If all platform (abbreviations) match a pattern, return this group name instead.
    KNOWN_PLATFORM_GROUPINGS = {
        "ls": re.compile(r"ls\d+"),
        "s1": re.compile(r"s1[a-z]+"),
        "s2": re.compile(r"s2[a-z]+"),
    }

    def __init__(
        self,
        *,
        known_abbreviations: Dict = None,
        grouped_abbreviations: Dict = None,
        show_specific_platform=True,
        allow_unknown_abbreviations=True,
    ) -> None:
        self.known_abbreviations = (
            known_abbreviations or self.KNOWN_PLATFORM_ABBREVIATIONS
        )
        self.grouped_abbreviations = (
            grouped_abbreviations or self.KNOWN_PLATFORM_GROUPINGS
        )
        self.show_specific_platform = show_specific_platform

        self.allow_unknown_abbreviations = allow_unknown_abbreviations

    def __get__(self, c: "NamingConventions", owner):
        """Abbreviated form of a satellite, as used in dea product names. eg. 'ls7'."""

        p = c.dataset.platforms
        if not p:
            return None

        if not self.allow_unknown_abbreviations:
            unknowns = p.difference(self.known_abbreviations)
            if unknowns:
                raise ValueError(
                    f"We don't know the DEA abbreviation for platforms {unknowns!r}. "
                    f"We'd love to add more! Raise an issue on Github: "
                    f"https://github.com/GeoscienceAustralia/eo-datasets/issues/new' "
                )

        abbreviations = sorted(
            self.known_abbreviations.get(s, s.replace("-", "")) for s in p
        )

        if self.show_specific_platform and len(abbreviations) == 1:
            return abbreviations[0]

        # If all abbreviations are in a group, name it using that group.
        # (eg. "ls" instead of "ls5-ls7-ls8")
        for group_name, pattern in self.grouped_abbreviations.items():
            if all(pattern.match(a) for a in abbreviations):
                return group_name

        # Otherwise, there's a mix of platforms.

        # Is there a common constellation?
        constellation = c.dataset.properties.get("constellation")
        if constellation:
            return constellation

        # Don't bother to include platform in name for un-groupable mixes of them.
        if not self.allow_unknown_abbreviations:
            raise NotImplementedError(
                f"Satellite constellation abbreviation is not known for platforms {p}. "
                f"(for DEA derivative naming conventions.)"
                f"    Is this a mistake? We'd love to add more! Raise an issue on Github: "
                f"https://github.com/GeoscienceAustralia/eo-datasets/issues/new' "
            )
        return None


class LazyInstrumentAbbreviation:
    def __get__(self, c: "NamingConventions", owner):
        """Abbreviated form of an instrument name, as used in dea product names. eg. 'c'."""
        platforms = c.dataset.platforms
        if not platforms or len(platforms) > 1:
            return None

        [p] = platforms

        if p.startswith("sentinel-1") or p.startswith("sentinel-2"):
            return c.dataset.instrument[0].lower()

        if p.startswith("landsat"):
            # Extract from usgs standard:
            # landsat:landsat_product_id: LC08_L1TP_091075_20161213_20170316_01_T2
            # landsat:landsat_scene_id: LC80910752016348LGN01
            landsat_id = c.dataset.properties.get("landsat:landsat_product_id")
            if landsat_id is None:
                landsat_id = c.dataset.properties.get("landsat:landsat_scene_id")

            # from USGS STAC, label is LC08_L2SP_178079_20210417_20210424_02_T1_SR and
            # landsat:scene_id: LC81780792021107LGN00
            if landsat_id is None:
                landsat_id = c.dataset.properties.get("landsat:scene_id")

            if not landsat_id:
                raise NotImplementedError(
                    "TODO: Can only currently abbreviate instruments from Landsat references."
                )

            return landsat_id[1].lower()

        # Otherwise, it's unknown.
        raise NotImplementedError(
            f"Instrument abbreviations aren't supported for platform {p!r}. "
            f"We'd love to add more support! Raise an issue on Github: "
            f"https://github.com/GeoscienceAustralia/eo-datasets/issues/new' "
        )


class LazyProducerAbbreviation:
    KNOWN_PRODUCER_ABBREVIATIONS = {
        "ga.gov.au": "ga",
        "usgs.gov": "usgs",
        "sinergise.com": "sinergise",
        "digitalearthafrica.org": "deafrica",
        "esa.int": "esa",
        # Is there another organisation you want to use? Pull requests very welcome!
    }

    def __init__(self, *, known_abbreviations: Dict = None) -> None:
        self.known_abbreviations = (
            known_abbreviations or self.KNOWN_PRODUCER_ABBREVIATIONS
        )

    def __get__(self, c: "NamingConventions", owner):
        """Abbreviated form of a producer, as used in dea product names. eg. 'ga', 'usgs'."""
        if not c.dataset.producer:
            return None

        try:
            return self.known_abbreviations[c.dataset.producer]
        except KeyError:
            raise NotImplementedError(
                f"We don't know how to abbreviate organisation domain name {c.dataset.producer!r}. "
                f"We'd love to add more orgs! Raise an issue on Github: "
                f"https://github.com/GeoscienceAustralia/eo-datasets/issues/new' "
            )


class LazyDestinationFolder:
    def __init__(
        self,
        date_folders_format="%Y/%m/%d",
        include_version=False,
        include_non_final_maturity=True,
    ) -> None:
        super().__init__()
        self.include_version = include_version
        self.include_non_final_maturity = include_non_final_maturity
        self.date_folders_format = date_folders_format

    def __get__(self, c: "NamingConventions", owner):
        """The folder hierarchy the datasets files go into.

        This is returned as a relative path.

        Example: Path("ga_ls8c_ard_3/092/084/2016/06/28")
        """
        d = c.dataset
        parts = [c.product_name]

        if self.include_version:
            parts.append(d.dataset_version.replace(".", "-"))

        # Cut the region code in subfolders
        region_code = d.region_code
        if region_code:
            parts.extend(utils.subfolderise(region_code))

        parts.extend(d.datetime.strftime(self.date_folders_format).split("/"))

        if self.include_non_final_maturity:
            # If it's not a final product, append the maturity to the folder.
            maturity: str = d.properties.get("dea:dataset_maturity")
            if maturity and maturity != "final":
                parts[-1] = f"{parts[-1]}_{maturity}"

        if c.dataset_separator_field is not None:
            val = d.properties[c.dataset_separator_field]
            # TODO: choosable formatter?
            if isinstance(val, datetime):
                val = f"{val:%Y%m%dT%H%M%S}"
            parts.append(val)
        return Path(*parts)


def _product_uri(product_name, base_uri):
    return f"{base_uri}/product/{product_name}"


class MissingRequiredFields(ValueError):
    ...


class EnforceRequiredProps(Eo3Properties):

    # Displayed to user for friendlier errors.
    _REQUIRED_PROPERTY_HINTS = {
        "odc:product_family": 'eg. "wofs" or "level1"',
        "odc:processing_datetime": "Time of processing, perhaps datetime.utcnow()?",
        "odc:producer": "Creator of data, eg 'usgs.gov' or 'ga.gov.au'",
        "odc:dataset_version": "eg. 1.0.0",
    }

    def __init__(
        self,
        required_fields: Set[str],
        properties=None,
    ) -> None:
        self.required_fields = required_fields
        super().__init__(properties)

    def __getitem__(self, item):
        try:
            val = super().__getitem__(item)
            if (not val) and (item in self.required_fields):
                self._raise_all_missing_requirements()
            return val
        except KeyError:
            if item in self.required_fields:
                self._raise_all_missing_requirements()
            raise

    def _raise_all_missing_requirements(self):
        """
        Do we have enough properties to generate file or product names?
        """
        missing_props = []
        for f in self.required_fields:
            if f not in self._props:
                missing_props.append(f)
        if missing_props:
            examples = []
            for p in sorted(missing_props):
                hint = self._REQUIRED_PROPERTY_HINTS.get(p, "")
                if hint:
                    hint = f" ({hint})"
                examples.append(f"\n- {p!r}{hint}")

            raise MissingRequiredFields(
                f"Need more properties to fulfill naming conventions."
                f"{''.join(examples)}"
            )


class EnforceRequirementFields(EoFields):
    @property
    def properties(self) -> Eo3Properties:
        return self._props

    def __init__(self, properties: Mapping, required_fields: Set[str]) -> None:
        self._props = EnforceRequiredProps(required_fields, properties)


class NamingConventions:
    """
    Naming conventions based on the DEA standard.

    Unlike the DEA standard, almost every field is optional by default.
    """

    _ABSOLUTE_MINIMAL_PROPERTIES = {
        "odc:product_family",
        # Required by Stac regardless.
        "datetime",
    }

    # Placed here for public usage, as people can extend the defaults.
    KNOWN_PRODUCER_ABBREVIATIONS = LazyProducerAbbreviation.KNOWN_PRODUCER_ABBREVIATIONS
    KNOWN_PLATFORM_ABBREVIATIONS = LazyPlatformAbbreviation.KNOWN_PLATFORM_ABBREVIATIONS
    KNOWN_PLATFORM_GROUPINGS = LazyPlatformAbbreviation.KNOWN_PLATFORM_GROUPINGS

    # These are lazily computed on read if not overridden by the user.
    # ie. User can set th names.product_name = 'blah'
    product_name: str = LazyProductName(include_collection=True)
    platform_abbreviated: str = LazyPlatformAbbreviation()
    instrument_abbreviated: str = LazyInstrumentAbbreviation()
    producer_abbreviated: str = LazyProducerAbbreviation()

    # No major version, as the product name contains it (the collection version).
    dataset_label: str = LazyLabel(strip_major_version=True)

    file_pattern: str = LazyFilePattern()

    destination_folder: Path = LazyDestinationFolder()

    def __init__(
        self,
        properties: Mapping,
        base_product_uri: str = None,
        required_fields: Sequence[str] = (),
        dataset_separator_field: Optional[str] = None,
        allow_unknown_abbreviations: bool = True,
    ) -> None:

        self.base_product_uri = base_product_uri
        self.required_fields = self._ABSOLUTE_MINIMAL_PROPERTIES.union(required_fields)

        # An extra folder to put each dataset inside, using the value of the given property name.
        self.dataset_separator_field = dataset_separator_field

        if self.dataset_separator_field is not None:
            self.required_fields.add(dataset_separator_field)

        self.allow_unknown_abbreviations = allow_unknown_abbreviations
        self.dataset = EnforceRequirementFields(properties, self.required_fields)

    @property
    def displayed_collection_number(self) -> Optional[int]:
        # An explicit collection number trumps all.
        if self.dataset.collection_number:
            return int(self.dataset.collection_number)

        # Otherwise it's the first digit of the dataset version.
        if not self.dataset.dataset_version:
            return None
        return int(self.dataset.dataset_version.split(".")[0])

    @property
    def product_uri(self) -> Optional[str]:
        if not self.base_product_uri:
            return None

        return _product_uri(self.product_name, base_uri=self.base_product_uri)

    def metadata_path(self, kind: str = "", suffix: str = "yaml") -> Path:
        return self.make_filename(kind, suffix)

    def checksum_path(self, suffix: str = "sha1") -> Path:
        return self.make_filename("", suffix)

    def measurement_file_path(
        self, measurement_name: str, suffix: str, file_id: str = None
    ) -> Path:
        name = measurement_name.replace(":", "_")

        return self.make_filename(
            # We use 'band01'/etc in the filename if provided, rather than 'red'
            file_id or name,
            suffix,
        )

    def make_filename(
        self, file_id: str, suffix: str, sub_package_name: str = None
    ) -> Path:
        file_id = "_" + file_id.replace("_", "-") if file_id else ""

        return Path(self.file_pattern.format(file_id=file_id, suffix=suffix))

    def thumbnail_name(self, kind: str = None, suffix: str = "jpg") -> Path:
        if kind:
            name = f"{kind}:thumbnail"
        else:
            name = "thumbnail"
        return self.measurement_file_path(name, suffix)


class DEANamingConventions(NamingConventions):
    """
    Example file structure (note version number in file):

            ga_ls8c_ones_3/090/084/2016/01/21/ga_ls8c_ones_3-0-0_090084_2016-01-21_final.odc-metadata.yaml
    """

    def __init__(
        self,
        properties: Mapping,
        required_fields=(
            "eo:platform",
            "eo:instrument",
            "odc:processing_datetime",
            "odc:producer",
            "odc:product_family",
            "odc:region_code",
            "odc:dataset_version",
        ),
        dataset_separator_field: Optional[str] = None,
    ) -> None:
        # DEA wants consistency via the naming-conventions doc.
        allow_unknown_abbreviations = False
        super().__init__(
            properties,
            DEA_URI_PREFIX,
            required_fields,
            dataset_separator_field,
            allow_unknown_abbreviations,
        )

    product_name: str = LazyProductName(include_collection=True)
    # Stricter: only allow pre-approved abbreviations.
    platform_abbreviated: str = LazyPlatformAbbreviation(
        allow_unknown_abbreviations=False
    )


class DEAS2NamingConventions(DEANamingConventions):
    """
    DEA naming conventions, but with an extra subfolder for each unique datatake.

    It will figure out the datatake if you set a sentinel_tile_id or datastrip_id.
    """

    def __init__(
        self,
        properties: Mapping,
        required_fields=(
            "eo:instrument",
            "eo:platform",
            "odc:dataset_version",
            "odc:processing_datetime",
            "odc:producer",
            "odc:product_family",
            "odc:region_code",
        ),
        dataset_separator_field="sentinel:datatake_start_datetime",
    ) -> None:
        super().__init__(
            properties, required_fields, dataset_separator_field=dataset_separator_field
        )


class DEADerivativesNamingConventions(DEANamingConventions):
    """
    Common derived products.

    Unlike plain 'DEA', they use an explicit collection number (odc:collection_number)
    in the product name which may differ from the software's dataset version
    (odc:dataset_version)

    Example file structure (note version number in folder):

        ga_ls_wo_3/1-6-0/090/081/1998/07/30/ga_ls_wo_3_090081_1998-07-30_interim.odc-metadata.yaml

    Derivatives have a slightly different folder structure.

    And they only show constellations (eg. "ls_" or "s2_") rather than the specific
    satellites in their names (eg. "ls8_").

    They have a version-number folder instead of putting it in each filename.

    And version numbers may not match the collection number (`odc:collection_number` is
    mandatory).
    """

    def __init__(
        self,
        properties: Mapping,
        required_fields: Sequence[str] = (
            "eo:platform",
            "odc:dataset_version",
            "odc:collection_number",
            "odc:processing_datetime",
            "odc:producer",
            "odc:product_family",
            "odc:region_code",
            "dea:dataset_maturity",
        ),
        dataset_separator_field: Optional[str] = None,
    ) -> None:
        super().__init__(
            properties,
            required_fields=required_fields,
            dataset_separator_field=dataset_separator_field,
        )

    # Derivates put the version in the folder instead.
    dataset_label = LazyLabel(include_version=False)
    product_name = LazyProductName(include_instrument=False, include_collection=True)

    destination_folder = LazyDestinationFolder(
        include_version=True,
        include_non_final_maturity=False,
    )

    platform_abbreviated = LazyPlatformAbbreviation(
        show_specific_platform=False,
        allow_unknown_abbreviations=False,
    )


class DEAS2DerivativesNamingConventions(DEADerivativesNamingConventions):
    """
    Sentinel-2-based DEA derivative naming conventions. Unlike regular
    derivatives, there's an extra subfolder for each unique datatake.

    It will figure out the datatake if you set a sentinel_tile_id
    or datastrip_id.
    """

    def __init__(self, properties: Mapping) -> None:
        super().__init__(
            properties,
            dataset_separator_field="sentinel:datatake_start_datetime",
        )


class AfricaProductName:
    def __get__(self, c: "NamingConventions", owner) -> str:
        if c.dataset.product_name:
            return c.dataset.product_name
        return f"{c.dataset.product_family}_{c.platform_abbreviated}"


class DEAfricaNamingConventions(NamingConventions):
    """
    DEAfrica avoids org names and uses simpler "{family}_{platform}" product names.

    Eg. "wo_ls" (water observations of landsat)

    """

    product_name = AfricaProductName()
    dataset_label = LazyLabel(include_version=False)
    destination_folder = LazyDestinationFolder(
        include_version=True,
        include_non_final_maturity=False,
    )
    platform_abbreviated = LazyPlatformAbbreviation(
        show_specific_platform=False,
        allow_unknown_abbreviations=False,
    )

    def __init__(
        self,
        properties: Mapping,
    ) -> None:
        super().__init__(
            properties,
            base_product_uri="https://digitalearthafrica.org",
            required_fields=(
                "eo:platform",
                "odc:producer",
                "odc:region_code",
                "odc:product_family",
                "odc:dataset_version",
            ),
        )

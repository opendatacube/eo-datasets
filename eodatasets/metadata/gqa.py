# -*- coding: utf-8 -*-
"""
Extract metadata from GQA 'results' csv files.
"""

import datetime
import logging

import yaml

_LOG = logging.getLogger(__name__)


def choose_and_populate_gqa(dataset, files):
    """
    Find any gqa results in the list of files and populate the dataset with them.

    :type dataset: eodatasets.type.DatasetMetadata
    :type files: tuple[pathlib.Path]
    :rtype dataset: eodatasets.type.DatasetMetadata
    """
    gqa_file = _choose_gqa(files)
    if gqa_file:
        dataset = populate_from_gqa(dataset, gqa_file)
    return dataset


def _choose_gqa(additional_files):
    """
    Choose the latest GQA in a list of files (or none if there aren't any).
    :type additional_files: tuple[pathlib.Path]
    :rtype: pathlib.Path or None

    >>> from pathlib import Path
    >>> files = (
    ...     Path('additional/20141201_19991029_B6_gqa_results.yaml'),
    ...     Path('additional/20141201_20000321_B6_gqa_results.yaml')
    ... )
    >>> str(_choose_gqa(files))
    'additional/20141201_20000321_B6_gqa_results.yaml'
    >>> str(_choose_gqa(files[:1]))
    'additional/20141201_19991029_B6_gqa_results.yaml'
    >>> _choose_gqa(())
    """
    gqa_files = [f for f in additional_files if f.name.endswith('gqa_results.yaml')]
    if not gqa_files:
        return None

    newest_first = list(sorted(gqa_files, reverse=True))
    return newest_first[0]


def populate_from_gqa(md, gqa_file):
    """
    :type md: eodatasets.type.DatasetMetadata
    :type gqa_file: pathlib.Path
    :rtype eodatasets.type.DatasetMetadata
    """
    with gqa_file.open('r') as f:
        gqa_values = yaml.safe_load(f)

    # "Scene id" is just the name of the parent folder. Often wrong and misleading.
    del gqa_values['scene_id']

    version = gqa_values.pop('software_version')
    repo = gqa_values.pop('software_repository')

    md.gqa = {}
    md.gqa.update(gqa_values)
    md.lineage.machine.note_software_version(
        'gqa',
        version=version,
        repo_url=repo,
        # We know this is more specific: so override existing versions.
        allow_override=True
    )
    return md


def _parse_day(f, fields):
    return datetime.datetime.strptime(fields[f], '%Y%m%d').date()

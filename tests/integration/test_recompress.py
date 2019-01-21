import tarfile
from functools import partial
from typing import List

import pytest
from click.testing import CliRunner, Result
from deepdiff import DeepDiff

from eodatasets.scripts import recompress
from pathlib import Path

this_folder = Path(__file__).parent
packaged_path = this_folder.joinpath(
    'recompress_packed/USGS/L1/Landsat/C1/092_091/LT50920911991126',
    'LT05_L1GS_092091_19910506_20170126_01_T2.tar.gz'
)
unpackaged_path = this_folder.joinpath(
    'recompress_unpackaged/USGS/L1/Landsat/C1/092_091/LT50920911991126'
)


@pytest.mark.parametrize(
    "input_path",
    [packaged_path, unpackaged_path],
    ids=('packaged', 'unpackaged')
)
def test_recompress_dataset(input_path: Path, tmp_path: Path):
    assert input_path.exists()

    output_base = tmp_path / 'out'

    res: Result = CliRunner().invoke(
        recompress.main,
        (
            '--output-base',
            str(output_base),
            # Out test data is smaller than the default block size.
            '--block-size', '32',
            str(input_path),
        ),
        catch_exceptions=False
    )
    assert res.exit_code == 0, res.output

    expected_output = (
            output_base /
            'L1/Landsat/C1/092_091/LT50920911991126' /
            'LT05_L1GS_092091_19910506_20170126_01_T2.tar'
    )

    # Pytest has better error messages for strings than Paths.
    all_output_files = [str(p) for p in output_base.rglob('*') if p.is_file()]

    print('\n\t'.join(all_output_files))

    assert len(all_output_files) == 1, f"Expected one output tar file. Got"
    assert all_output_files == [str(expected_output)]

    assert expected_output.exists(), f"No output produced in expected location {expected_output}."

    # It should contain all of our files
    with tarfile.open(expected_output, 'r') as tar:
        members: List[tarfile.TarInfo] = tar.getmembers()

        # Checksum is last (can be calculated while streaming)
        checksum_member = members[-1]
        assert checksum_member.name == 'package.sha1'
        checksum_lines = tar.extractfile(checksum_member).readlines()

    member_names = [m.name for m in members]

    # Note that MTL is first. We do this deliberately so it's quick to access.
    assert member_names == [
        'LT05_L1GS_092091_19910506_20170126_01_T2_MTL.txt',
        'LT05_L1GS_092091_19910506_20170126_01_T2_ANG.txt',
        'LT05_L1GS_092091_19910506_20170126_01_T2_B1.TIF',
        'LT05_L1GS_092091_19910506_20170126_01_T2_B2.TIF',
        'LT05_L1GS_092091_19910506_20170126_01_T2_B3.TIF',
        'LT05_L1GS_092091_19910506_20170126_01_T2_B4.TIF',
        'LT05_L1GS_092091_19910506_20170126_01_T2_B5.TIF',
        'LT05_L1GS_092091_19910506_20170126_01_T2_B6.TIF',
        'LT05_L1GS_092091_19910506_20170126_01_T2_B7.TIF',
        'LT05_L1GS_092091_19910506_20170126_01_T2_BQA.TIF',
        'README.GTF',
        'package.sha1',
    ]

    member_sizes = {m.name: m.size for m in members}

    # Text files should be unchanged.
    assert member_sizes['LT05_L1GS_092091_19910506_20170126_01_T2_MTL.txt'] == 6693
    mtl_lines = [l for l in checksum_lines if b'_MTL.txt' in l]
    assert len(mtl_lines) == 1
    assert b'beb4d546dc5e2850b2f33384bfbc6cf15b724197\tLT05_L1GS_092091_19910506_20170126_01_T2_MTL.txt\n' in mtl_lines

    assert member_sizes['package.sha1'] == 945
    assert member_sizes['README.GTF'] == 8686
    assert member_sizes['LT05_L1GS_092091_19910506_20170126_01_T2_ANG.txt'] == 34884


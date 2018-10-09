import click

from ..prepare.s2_prepare_cophub_zip import (
    from_list as s2_scihub_from_list,
    from_args as s2_scihub_from_args
)
from ..prepare.ls_usgs_l1_prepare import main as ls_usgs
from ..prepare.s2_l1c_aws_pds_prepare import main as s2_awspds


@click.group()
@click.version_option()
def run():
    pass


run.add_command(s2_scihub_from_args, name='s2-cophub')
run.add_command(s2_scihub_from_list, name='s2-cophub-list')
run.add_command(ls_usgs, name='ls-usgs')
run.add_command(s2_awspds, name='s2-awspds')

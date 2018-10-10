import click

from ..prepare.s2_prepare_cophub_zip import main as s2_scihub
from ..prepare.ls_usgs_l1_prepare import main as ls_usgs
from ..prepare.s2_l1c_aws_pds_prepare import main as s2_awspds


@click.group()
@click.version_option()
def run():
    pass


run.add_command(s2_scihub, name='s2-cophub')
run.add_command(ls_usgs, name='ls-usgs')
run.add_command(s2_awspds, name='s2-awspds')


if __name__ == '__main__':
    run()

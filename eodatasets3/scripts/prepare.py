import click

from ..prepare.landsat_l1_prepare import main as landsat_l1
from ..prepare.nasa_c_m_mcd43a1_6_prepare import main as mcd43a1
from ..prepare.noaa_c_c_prwtreatm_1_prepare import main as prwtr


@click.group()
@click.version_option()
def run():
    pass


run.add_command(landsat_l1, name="landsat-l1")
run.add_command(mcd43a1, name="modis-mcd43a1")
run.add_command(prwtr, name="noaa-prwtr")


if __name__ == "__main__":
    run()

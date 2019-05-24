#!/usr/env/bin python3
"""
Short script to extract a subset of data from a
    NCEP/NCAR Reanalysis 1 water vapour dataset stored in netCDF4 (classic)
"""

import click
from netCDF4 import Dataset


def _get_dimension_size(dim):
    """
    Returns the dimension size for netCDF dimension initialisation
    """
    if dim.isunlimited():
        return None
    return dim.size


def create_test_file(infile, outfile, time_subset=2, netcdf_fmt='NETCDF4_CLASSIC'):
    """
    Creates a subset of the water vapour dataset
    """
    with Dataset(infile, 'r', netcdf_fmt) as rootin, \
            Dataset(outfile, 'w', netcdf_fmt) as rootout:

        # Create dimensions
        for dim_name, dim in rootin.dimensions.items():
            rootout.createDimension(dim_name, _get_dimension_size(dim))

        # Propagate Attributes
        for attr_name in rootin.ncattrs():
            rootout.setncattr(attr_name, rootin.getncattr(attr_name))

        # Create variables
        for var_name in rootin.variables.keys():
            _var = rootin[var_name]
            var = rootout.createVariable(
                varname=var_name,
                datatype=_var.datatype,
                dimensions=_var.dimensions,
                endian=_var.endian()
            )

            for attr_name in _var.ncattrs():
                var.setncattr(attr_name, _var.getncattr(attr_name))

            if var_name in ['lat', 'lon']:
                var[:] = _var[:]
            else:
                var[:] = _var[:time_subset]


@click.command(
    help="""Creates a subset of a ncep/ncar reanalysis 1 water vapour file for testing"""
)
@click.option('--infile', type=click.Path(exists=True, dir_okay=False, readable=True),
              help='Path to source pr_wtr.eatm.{year}.nc source file')
@click.option('--outfile', type=click.Path(exists=False, dir_okay=False, writable=True),
              help='Destination to write the data subset to')
def cli(infile, outfile):
    """ CLI wrapper for subset creation """
    create_test_file(infile, outfile)


if __name__ == '__main__':
    cli()  # pylint: disable=E1120

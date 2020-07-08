#!/usr/bin/env python

from datetime import datetime
from posixpath import join as ppjoin, basename as pbasename, dirname
from pathlib import Path
import h5py
from rasterio.crs import CRS
from affine import Affine
from eodatasets3 import DatasetAssembler, images
from wagl.hdf5 import find


INDIR = Path("/g/data/up71/projects/index-testing-wagl/wagl/workdir/batchid-48b378b0f0/jobid-c59136/LC08_L1TP_099080_20160613_20180203_01_T1.tar.ARD/LC80990802016165LGN02")
WAGL_FNAME = Path("LC80990802016165LGN02.wagl.h5")
GROUP_PATH = "/LC80990802016165LGN02/RES-GROUP-1/STANDARDISED-PRODUCTS"
GDAL_H5_FMT = 'HDF5:"{filename}":/{dataset_pathname}'


def package_non_standard(outdir, granule):
    """
    yaml creator for the ard pipeline.
    Purely for demonstration purposes only.
    Can easily be expanded to include other datasets.

    A lot of the metadata such as date, can be extracted from the yaml
    doc contained withing the HDF5 file at the following path:

    [/<granule_id>/METADATA/CURRENT]

    
    """
    out_fname = INDIR.joinpath('LC80990802016165LGN02.yaml')
    with DatasetAssembler(metadata_path=out_fname, naming_conventions='dea') as da:
        level1 = granule.source_level1_metadata
        da.add_source_dataset(level1, auto_inherit_properties=True)
        
        #da.platform = 'landsat8'
        da.product_family = 'ard'
        da.maturity = 'final'

        #da.properties["landsat:landsat_scene_id"] = "LC80990802016165LGN02"
        #da.properties['eo:instrument'] = 'olitirs'
        da.properties['landsat:collection_number'] = '1'


        # not the real date of the dataset
        #da.datetime = datetime(2018, 6, 30, 19, 33, 4, 334934)

        #da.region_code = '096091'
        da.processed = '2019-07-12 07:40:50.137089Z'  # not the real processed date
        da.dataset_version = '1.0.0'
        da.producer = 'ga.gov.au'

        with h5py.File(str(INDIR.joinpath(WAGL_FNAME)), 'r') as fid:
            img_paths = [ppjoin(fid.name, pth) for pth in find(fid, 'IMAGE')]

            for pathname in img_paths:
                ds = fid[pathname]

                if ds.dtype.name == 'bool':
                   continue
                
                # eodatasets internally uses this grid spec to group
                # image dataset
                grid_spec = images.GridSpec(
                    shape=ds.shape,
                    transform=Affine.from_gdal(*ds.attrs['geotransform']),
                    crs=CRS.from_wkt(ds.attrs['crs_wkt'])
                )

                # note; pathname here is only a relative pathname
                pathname = GDAL_H5_FMT.format(
                    filename=str(outdir.joinpath(WAGL_FNAME)),
                    dataset_pathname=pathname
                )

                # just for this example, so we insert nbar_blue
                # otherwise we'll get duplicates if just using blue
                # something can be done for the other datasets
                parent = pbasename(ds.parent.name)
                
                # Get spatial resolution
                resolution = Path(ds.parent.name).parts[2]
                
                measurement_name = "_".join(
                    [
                        resolution,
                        parent,
                        ds.attrs.get('alias', pbasename(ds.name)),
                    ]
                ).replace('-', '_').lower()  # we don't wan't hyphens in odc land
                print(measurement_name)

                # include this band in defining the valid data bounds?
                include = True if 'nbart' in measurement_name else False

                # work around as note_measurement doesn't allow us to specify the gridspec
                da._measurements.record_image(
                    measurement_name,
                    grid_spec,
                    pathname,
                    ds[:],
                    nodata=-999,
                    expand_valid_data=include
                )

                # this method will not give as the transform and crs and eodatasets will complain later
                # TODO: raise an issue on github for eodatasets
                # da.note_measurement(
                #     measurement_name,
                #     pathname,
                #     expand_valid_data=False,
                # )
                
        # the longest part here is generating the valid data bounds vector
        # landsat 7 post SLC-OFF can take a really long time
        da.done()


def main():
    package_non_standard(INDIR, granule)


if __name__ == "__main__":
    main()

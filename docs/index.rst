.. eodatasets3 documentation master file, created by
   sphinx-quickstart on Mon Aug 26 14:33:29 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

EO Datasets 3
=============

EO Datasets aims to be the easiest way to write, validate and convert dataset imagery
and metadata for the `Open Data Cube`_

.. _Open Data Cube: https://github.com/opendatacube/datacube-core


Write a Dataset
================

Here's a simple example of creating a dataset with one measurement (called "blue") from an existing image::

   collection = Path('/some/output/collection/path')
   with DatasetAssembler(collection) as p:
      p.product_family = "blues"

      # Date of acquisition (UTC if no timezone).
      p.datetime = datetime(2019, 7, 4, 13, 7, 5)
      # When the data was processed/created.
      p.processed_now() # Right now!
      # (If not newly created, set the date on the field: `p.processed = ...`)

      # Write our measurement from the given path, calling it 'blue'.
      p.write_measurement("blue", blue_geotiff_path)

      # Add a jpg thumbnail using our only measurement for the r/g/b bands.
      p.write_thumbnail("blue", "blue", "blue")

      # Complete the dataset.
      p.done()

Note that until you call `done()`, nothing will exist in the dataset's final output location. It is stored in a hidden temporary
folder in the output, and renamed by `done()` if complete and valid.

Custom stac-like properties can also be set directly on ``.properties``::

      p['fmask:cloud_cover'] = 34.0

Any known properties are automatically normalised::

      p.platform = "LANDSAT_8"  # to: 'landsat-8'
      p.processed = "2016-03-04 14:23:30Z"  # into a date.
      p.maturity = "FINAL"  # lowercased
      p.properties["eo:off_nadir"] = "34"  # into a number


Including provenance
====================
Most of the time our datasets are processed from an existing (input) dataset and
have the same spatial information. We can add them as source datasets, to record the provenance, and the
assembler can optionally copy any common metadata automatically::

   collection = Path('/some/output/collection/path')
   with DatasetAssembler(collection) as p:
      # We add a source dataset, asking to inherit the common properties
      # (eg. platform, instrument, datetime)
      p.add_source_path(level1_ls8_dataset_path, auto_inherit_properties=True)

      # Set our product information.
      # It's a GA product of "numerus-unus" ("the number one").
      p.producer = "ga.gov.au"
      p.product_family = "blues"
      p.dataset_version = "3.0.0"

We can write our new pixels as a numpy array, inheriting the existing grid spatial information (*gridspec*)
from our input dataset::

      # Write a measurement from a numpy array, using the source dataset's grid spec.
      p.write_measurement_numpy(
         "ones",
         numpy.ones((60, 60), numpy.int16),
         GridSpec.from_dataset_doc(l1_ls8_dataset),
         nodata=-999,
      )

Writing only metadata
=====================

The above examples copy the imagery, converting them to valid COG imagery. But sometimes you
don't want to touch your imagery, you only want metadata. We can use :meth:`eodatasets3.DatasetAsssembler.note_measurement`
instead of :meth:`eodatasets3.DatasetAsssembler.write_measurement`
to refer to the image at it's current path::

    usgs_level1 = Path('datasets/LC08_L1TP_090084_20160121_20170405_01_T1')

    with DatasetAssembler(
      dataset_location=usgs_level1
    ) as p:
      p.product_family = "level1"
      p.datetime = datetime(2019, 7, 4, 13, 7, 5)

      # Note the measurement in the metadata. (instead of ``write``)
      p.note_measurement('red',
         usgs_level1 / 'LC08_L1TP_090084_20160121_20170405_01_T1_B3.TIF'
      )

      # Or relative to the dataset
      # (this will work unchanged on non-filesystem locations, such as ``s3://`` or tar files)
      p.note_measurement('blue',
         'LC08_L1TP_090084_20160121_20170405_01_T1_B3.TIF',
         relative_to_dataset_location=True
      )

Note that the assembler will throw an error if any measurements live outside
the dataset location, as they will have to be recorded as absolute rather
than relative paths. (Relative paths are considered best-practice for Open Data Cube.)

You can allow absolute paths with a field on :meth:`eodatasets3.DatasetAssembler.__init__`::

   with DatasetAssembler(
      dataset_location=usgs_level1,
      allow_absolute_paths=True,
    ):
        ...


API / Class
===========

.. autoclass:: eodatasets3.DatasetAssembler
   :members:
   :special-members: __init__


..
  We avoid using bigger heading types as readthedocs annoyingly collapses all table-of-contents
  otherwise, which is extremely annoying for small projects.
  (there's a mountain of empty vertical space on almost all projects! why collapse?)

EO Datasets 3
-------------


EO Datasets aims to be the easiest way to write, validate and convert dataset imagery
and metadata for the `Open Data Cube`_

.. _Open Data Cube: https://github.com/opendatacube/datacube-core

There are two major tools for creating datasets:

1. *DatasetPrepare*, for preparing a metadata document using existing imagery and files.
2. *DatasetAssembler*, for preparing a whole package folder: including metadata, writing imagery, thumbnails,
    checksums etc.

Their APIs are the same, except the latter adds functions named ``write_*`` in addition to the metadata
functions.

Assemble a Dataset Package
--------------------------

Here's a simple example of creating a dataset package with one measurement (called "blue") from an existing image::

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
folder in the output directory, and renamed by `done()` once complete and valid.

Custom stac-like properties can also be set directly on ``.properties``::

      p.properties['fmask:cloud_cover'] = 34.0

And known properties are automatically normalised::

      p.platform = "LANDSAT_8"  # to: 'landsat-8'
      p.processed = "2016-03-04 14:23:30Z"  # into a date.
      p.maturity = "FINAL"  # lowercased
      p.properties["eo:off_nadir"] = "34"  # into a number


Writing only a metadata doc
---------------------------

(ie. "I already have appropriate imagery!")

The above examples can be changed to use :class:`DatasetPrepare() <eodatasets3.DatasetPrepare>`
instead of a `DatasetAssembler`, which omits all file-writing logic..

And functions named ``write_`` (which write files) can be replaced by functions named ``note_*``
(which note information in the metadata).

Eg. :meth:`note_measurement() <eodatasets3.DatasetPrepare.note_measurement>` instead of
:meth:`write_measurement() <eodatasets3.DatasetAssembler.write_measurement>`::



    usgs_level1 = Path('datasets/LC08_L1TP_090084_20160121_20170405_01_T1')

    with DatasetPrepare(
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

      ...

      p.done()

By default, they will throw an error if a file lives outside the dataset (location),
as this will require absolute paths. Relative paths are considered best-practice for Open Data Cube
metadata.

You can allow absolute paths with a field on construction
(:meth:`DatasetPrepare() <eodatasets3.DatasetPrepare.__init__>`)::

   with DatasetPrepare(
      dataset_location=usgs_level1,
      allow_absolute_paths=True,
    ):
        ...

.. _COG: https://www.cogeo.org/


Including provenance
--------------------
Most datasets are processed from an existing (input) dataset and have the same spatial information as the input.
We can record them as source datasets, and the assembler can optionally copy any common metadata automatically::

   collection = Path('/some/output/collection/path')
   with DatasetAssembler(collection) as p:
      # We add a source dataset, asking to inherit the common properties
      # (eg. platform, instrument, datetime)
      p.add_source_path(level1_ls8_dataset_path, auto_inherit_properties=True)

      # Set our product information.
      # It's a GA product of "numerus-unus" ("the number one").
      p.producer = "ga.gov.au"
      p.product_family = "numerus-unus"
      p.dataset_version = "3.0.0"

      ...

In these situations, we often write our new pixels as a numpy array, inheriting the existing grid spatial information (*gridspec*)
from our input dataset::

      # Write a measurement from a numpy array, using the source dataset's grid spec.
      p.write_measurement_numpy(
         "water",
         my_computed_numpy_array,
         GridSpec.from_dataset_doc(source_dataset),
         nodata=-999,
      )


Generating names ahead of time
------------------------------

You can use the naming module alone to find file paths:

..
  Sorry future readers, but the error output of assertion failures is abysmal
  in these tests:

.. testcode::

   import eodatasets3
   from pathlib import Path
   from eodatasets3 import DatasetDoc

Create some properties.

.. testcode::

   d = DatasetDoc()
   d.platform = "sentinel-2a"
   d.product_family = "fires"
   d.datetime = "2018-05-04T12:23:32"
   d.processed_now()

   # Arbitrarily set any properties.
   d.properties["fmask:cloud_shadow"] = 42.0
   d.properties.update({"odc:file_format": "GeoTIFF"})

.. note::
   You can use a plain dict if you prefer. But we use an :class:`DatasetDoc() <eodatasets3.DatasetDoc>` here, which has
   convenience methods similar to :class:`DatasetAssembler <eodatasets3.DatasetAssembler>` for building properties.

Now create a `namer` instance with our properties:

.. testcode::

   names = eodatasets3.namer(conventions="default", properties=d)

And we can see some generated names:

.. testcode::

   print(names.product_name)
   print(names.dataset_folder)
   print(names.metadata_file)
   print(names.dataset_location)

Output:

.. testoutput::

   s2a_fires
   s2a_fires/2018/05/04
   s2a_fires_2018-05-04.odc-metadata.yaml
   s2a_fires/2018/05/04/s2a_fires_2018-05-04.odc-metadata.yaml


In reality, our paths go inside a folder (..or s3 bucket, etc) somewhere.

This folder is called the `collection_path` in :class:`DatasetAssembler <eodatasets3.DatasetAssembler>`'s
parameters, and we can join it ourselves to find our dataset the same way:

.. testcode::

   collection_path = Path('/datacube/collections')

..
   Let's override it quietly so we don't touch real paths on their system:

.. testcode::
   :hide:

   from eodatasets3 import DatasetAssembler

   import tempfile
   collection_path = Path(tempfile.mkdtemp())


.. testcode::

   absolute_metadata_path = collection_path / names.dataset_location

   if absolute_metadata_path.exists():
       print("Our dataset already exists!")

Now that we've created our own properties and names, we could also reuse them
if we later want to assemble a dataset:

.. testcode::

   with DatasetAssembler(collection_path, names=names) as p:

      # The properties are already set, thanks to our namer.

      ... # Write some measurements here, etc!

      p.done()

   # Now it actually exists!
   assert absolute_metadata_path.exists()


Naming things yourself
----------------------

You can set properties yourself on the namer to avoid automatic generation.
(or to avoid their finicky metadata requirements)

.. testsetup:: nametest

   from eodatasets3 import DatasetAssembler
   from pathlib import Path
   import tempfile

   collection_path = Path(tempfile.mkdtemp())

   p = DatasetAssembler(collection_path)
   p.platform = 'sentinel-2a'
   p.product_family = 'ard'

.. doctest:: nametest

   >>> p.names.product_name
   's2a_ard'
   >>> p.names.platform_abbreviated = "s2"
   >>> p.names.product_name
   's2_ard'

See more examples in the assembler :attr:`.names <eodatasets3.DatasetPrepare.names>` property.


Dataset Prepare API
-------------------

.. autoclass:: eodatasets3.DatasetPrepare
   :members:
   :special-members: __init__

Dataset Assembler API
---------------------

This contains all methods in :class:`eodatasets3.DatasetPrepare`, with additional
functions for writing out files.

.. autoclass:: eodatasets3.DatasetAssembler
   :members:
   :special-members: __init__

Reading/Writing YAMLs
---------------------

Methods for parsing and outputting EO3 docs as a :class:`eodatasets3.DatasetDoc`

Parsing
^^^^^^^

.. autofunction:: eodatasets3.serialise.from_path
.. autofunction:: eodatasets3.serialise.from_doc


Writing
^^^^^^^

.. autofunction:: eodatasets3.serialise.to_path
.. autofunction:: eodatasets3.serialise.to_stream
.. autofunction:: eodatasets3.serialise.to_doc

Name Generation API
-------------------

You may want to use the name generation alone, for instance
to tell if a dataset has already been written before you assemble it.

.. autofunction:: eodatasets3.namer

.. autoclass:: eodatasets3.NameGenerator
   :members:
   :inherited-members:

EO Metadata API
---------------

These are convenience properties for common metadata fields. They are available
on DatasetAssemblers and within other naming APIs.

(This is abstract. If you want one of these of your own, you probably want to create
an :class:`eodatasets3.DatasetDoc`)

.. autoclass:: eodatasets3.properties.Eo3Interface
   :members:


Misc Types
----------

..
  Catch any types we didn't add explicitly above

.. automodule:: eodatasets3
   :members:
   :exclude-members: DatasetAssembler, DatasetPrepare, NameGenerator, namer


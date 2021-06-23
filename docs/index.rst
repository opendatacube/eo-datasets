
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

#. :ref:`DatasetPrepare<preparing_metadata>`, for preparing a metadata document using existing imagery and files.
#. :ref:`DatasetAssembler<assembling_metadata>`, for creating a package folder: including metadata, writing imagery, thumbnails, checksums etc.

Their APIs are the same, except the latter adds functions named ``write_*`` in addition to the metadata
functions.

.. _assembling_metadata:

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


.. _preparing_metadata:

Writing only a metadata doc
---------------------------

(ie. "I already have appropriate imagery!")

The above examples can be changed to use :class:`DatasetPrepare() <eodatasets3.DatasetPrepare>`
(which prepares metadata) instead of a :class:`DatasetAssembler() <eodatasets3.DatasetAssembler>`
(which writes packages)

Functions named ``write_`` on assembler (which write files) have equivalent functions named ``note_*``
(which note information into the metadata) available in both classes.

Eg. :meth:`note_measurement() <eodatasets3.DatasetPrepare.note_measurement>` instead of
:meth:`write_measurement() <eodatasets3.DatasetAssembler.write_measurement>`

Example of generating metadata::


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

      # Add links to other files included in the package ("accessories"), such as
      # alternative metadata files.
      p.note_accessory_file('metadata:mtl', mtl_path)

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
Most datasets are processed from an existing input dataset and have the same spatial information as the input.
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

In these situations, we often write our new pixels as a numpy array, inheriting the existing
:class:`grid spatial information <eodatasets3.GridSpec>` of our input dataset::

      # Write a measurement from a numpy array, using the source dataset's grid spec.
      p.write_measurement_numpy(
         "water",
         my_computed_numpy_array,
         GridSpec.from_dataset_doc(source_dataset),
         nodata=-999,
      )

Creating documents in-memory
----------------------------

You may want to assemble metadata entirely in memory without touching the filesystem.

To do this, prepare a dataset as normal. You still need a dataset location, as referenced paths
will be relative to this location:

.. testsetup:: inmem

   from eodatasets3 import GridSpec
   from affine import Affine
   from rasterio.crs import CRS
   from pathlib import Path
   import numpy
   from datetime import datetime

   grid_spec = GridSpec(shape=(7721, 7621),
      transform=Affine(30.0, 0.0, 241485.0, 0.0, -30.0, -2281485.0),
      crs=CRS.from_epsg(32656)
   )

   import tempfile
   dataset_location = Path(tempfile.mkdtemp())
   measurement_path = dataset_location / "our_image_dont_read_it.tif"

.. doctest:: inmem

   >>> from eodatasets3 import DatasetPrepare
   >>> p = DatasetPrepare(dataset_location=dataset_location)
   >>> p.datetime = datetime(2019, 7, 4, 13, 7, 5)
   >>> p.product_name = "loch_ness_sightings"
   >>> p.processed = datetime(2019, 7, 4, 13, 8, 7)


We can give it a :class:`GridSpec <eodatasets3.GridSpec>` when adding measurements,
so it will not access the measurements to read grid information itself:

.. doctest:: inmem

   >>> p.note_measurement(
   ...     "blue",
   ...     measurement_path,
   ...     # We give it grid information, so it doesn't have to read it itself.
   ...     grid=grid_spec,
   ...     # And the image pixels, since we are letting it calculate our geometry.
   ...     pixels=numpy.ones((60, 60), numpy.int16),
   ...     nodata=-1,
   ... )

.. note::

   If you're writing your own image files manually, you may still want to use eodataset's
   name generation. You can ask for suitable paths from
   :attr:`p.names <eodatasets3.DatasetPrepare.names>`:

   .. doctest:: inmem

      >>> # The offset within our collection
      >>> p.names.dataset_folder
      PosixPath('loch_ness_sightings/2019/07/04')
      >>> # How should I name a 'red' measurement file?
      >>> p.names.measurement_filename('red')
      PosixPath('loch_ness_sightings_2019-07-04_red.tif')

   All generated filenames are relative to the dataset folder (but can also be absolute!),
   so we calculate the full offset by combining them:

   .. doctest:: inmem

      >>> full_measurement_path = p.names.dataset_folder / p.names.measurement_filename('red')

   (this will still be identical to the original filename if it's absolute, as desired.)

Now finish it as a :class:`DatasetDoc <eodatasets3.DatasetDoc>`:

.. doctest:: inmem

   >>> dataset = p.to_dataset_doc()

You can now use :ref:`serialise functions<serialise_explanation>` on the result yourself,
such as conversion to a dictionary:

.. doctest:: inmem

   >>> from eodatasets3 import serialise
   >>> doc: dict = serialise.to_doc(dataset)
   >>> doc['label']
   'loch_ness_sightings_2019-07-04'

Or convert it to a formatted yaml: :meth:`serialise.to_path(path, dataset) <eodatasets3.serialise.to_path>` or
:meth:`serialise.to_stream(stream, dataset) <eodatasets3.serialise.to_stream>`.


Avoiding geometry calculation
-----------------------------

Datasets include a geometry field, which shows the coverage of valid data pixels of
all measurements.

By default, the assembler will create this geometry by reading the pixels from your
measurements, and calculate a geometry vector on completion.

If you want to avoid these reads and calculations, you can set the geometry manually::

    p.geometry = my_shapely_polygon

Or copy it from one of your source datasets when you add your provenance (if it has
the same coverage)::

    p.add_source_path(source_path, inherit_geometry=True)

If you do this before you `note` measurements, it will not need to read any pixels
from them.

Generating names & paths alone
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

Now create a `namer` instance with our properties (and chosen naming conventions):

.. testcode::

   names = eodatasets3.namer(conventions="default", properties=d)

We can see some generated names:

.. testcode::

   print(names.metadata_file)
   print(names.measurement_filename('water'))
   print()
   print(names.product_name)
   print(names.dataset_folder)

Output:

.. testoutput::

    s2a_fires_2018-05-04.odc-metadata.yaml
    s2a_fires_2018-05-04_water.tif

    s2a_fires
    s2a_fires/2018/05/04

In reality, these paths go within a location (folder, s3 bucket, etc) somewhere.

This is called the `collection_path` in :class:`DatasetAssembler <eodatasets3.DatasetAssembler>`'s
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

We could now start assembling some metadata if our dataset doesn't exist,
passing it our existing fields:

.. testcode::

   with DatasetAssembler(collection_path, names=names) as p:

      # The properties are already set, thanks to our namer.

      ... # Write some measurements here, etc!

      p.done()

   # Now it actually exists!
   assert absolute_metadata_path.exists()


Naming things yourself
----------------------

Names and paths are only auto-generated if they have not been set manually
by the user.

You can set properties yourself on the :class:`NameGenerator <eodatasets3.NameGenerator>`
to avoid automatic generation (or to avoid their finicky metadata requirements).

.. testsetup:: nametest

   from eodatasets3 import DatasetPrepare
   from pathlib import Path
   import tempfile

   collection_path = Path(tempfile.mkdtemp())

.. doctest:: nametest

   >>> p = DatasetPrepare(collection_path)
   >>> p.platform = 'sentinel-2a'
   >>> p.product_family = 'ard'
   >>> # The namer will generate a product name:
   >>> p.names.product_name
   's2a_ard'
   >>> # Let's customise the generated abbreviation:
   >>> p.names.platform_abbreviated = "s2"
   >>> p.names.product_name
   's2_ard'

See more examples in the assembler :attr:`.names <eodatasets3.DatasetPrepare.names>` property.


Dataset Prepare class reference
-------------------------------

.. autoclass:: eodatasets3.DatasetPrepare
   :members:
   :special-members: __init__

Dataset Assembler class reference
---------------------------------

.. autoclass:: eodatasets3.DatasetAssembler
   :members:
   :special-members: __init__

.. _serialise_explanation:

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

.. autoclass:: eodatasets3.properties.Eo3Interface
   :members:


Misc Types
----------

..
  Catch any types we didn't add explicitly above

.. automodule:: eodatasets3
   :members:
   :exclude-members: DatasetAssembler, DatasetPrepare, NameGenerator, namer


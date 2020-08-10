# Configuration file for the Sphinx documentation builder.
#
# Full list of options: https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

sys.path.insert(0, os.path.abspath(".."))
sys.path.insert(0, os.path.abspath("."))
is_on_readthedocs = os.environ.get("READTHEDOCS", None) == "True"

project = "eodatasets3"
copyright = "2019, Geoscience Australia"
author = "Geoscience Australia"

# Show __init__ docstring as the class docstring.
autosummary_generate = True

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx_autodoc_typehints",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
    "sphinx.ext.viewcode",
]
intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "odc": ("https://datacube-core.readthedocs.io/en/stable/", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable/", None),
    "numpy": ("https://docs.scipy.org/doc/numpy/", None),
    "xarray": ("https://xarray.pydata.org/en/stable/", None),
}
templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# Custom static files (such as style sheets)
# They are copied after the builtin static files, so a file named "default.css" will
# overwrite the builtin "default.css".
html_static_path = ["_static"]


if is_on_readthedocs:
    html_theme = "default"
else:
    import sphinx_rtd_theme

    html_theme = "sphinx_rtd_theme"
    html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

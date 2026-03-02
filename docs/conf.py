# Sphinx documentation configuration

import importlib.metadata

project = "jupyterhub-monitoring"
author = "Brian Aydemir"
release = importlib.metadata.version("jupyterhub-monitoring")

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
]

napoleon_google_docstring = True
napoleon_numpy_docstring = False

html_theme = "sphinx_rtd_theme"

# Suppress "document isn't included in any toctree" warnings for api/ stubs.
suppress_warnings = ["toc.not_readable"]

# Sphinx documentation configuration

import importlib.metadata

project = "jupyterhub-monitoring"
author = "Brian Aydemir"
release = importlib.metadata.version("jupyterhub-monitoring")

html_theme = "shibuya"
html_theme_options = {"accent_color": "cyan"}

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
]

napoleon_google_docstring = True
napoleon_numpy_docstring = True

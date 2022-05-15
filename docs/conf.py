# coding: utf-8


import sys
import os


thisdir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(thisdir, "_extensions"))
sys.path.insert(0, os.path.dirname(thisdir))

import ap

project = ap.__name__
author = ap.__author__
copyright = ap.__copyright__
copyright = copyright[10:] if copyright.startswith("Copyright ") else copyright
version = ap.__version__[:ap.__version__.index(".", 2)]
release = ap.__version__
language = "en"

templates_path = ["_templates"]
html_static_path = ["_static"]
master_doc = "index"
source_suffix = ".rst"
exclude_patterns = []
pygments_style = "sphinx"
add_module_names = False

html_title = project + " Documentation"
# html_logo = "../logo.png"
html_sidebars = {"**": [
    "about.html",
    "localtoc.html",
    "searchbox.html",
]}
html_theme = "sphinx_rtd_theme"
html_theme_options = {}
if html_theme == "sphinx_rtd_theme":
    html_theme_options.update({
        "logo_only": True,
        "prev_next_buttons_location": None,
        "collapse_navigation": False,
    })
elif html_theme == "alabaster":
    html_theme_options.update({
        "github_user": "uhh-cms",
        "github_repo": "analysis_playground",
        "travis_button": True,
    })

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosectionlabel",
    "autodocsumm",
    "pydomain_patch",
]

autodoc_default_options = {
    "member-order": "bysource",
    "show-inheritance": True,
}

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}


# setup the app
def setup(app):
    # set style sheets
    app.add_css_file("styles_common.css")
    if html_theme in ("sphinx_rtd_theme", "alabaster"):
        app.add_css_file("styles_{}.css".format(html_theme))

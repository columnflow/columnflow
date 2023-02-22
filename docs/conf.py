# coding: utf-8


import sys
import os


# adjust the environment in a minimal way just so that the docs build
projdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(projdir, "docs", "_extensions"))
sys.path.insert(0, os.path.join(projdir, "modules", "law"))
sys.path.insert(0, os.path.join(projdir, "modules", "order"))
sys.path.insert(0, projdir)
os.environ["LAW_CONFIG_FILE"] = os.path.join(projdir, "docs", "law.cfg")

import columnflow as cf

project = "columnflow"
author = cf.__author__
copyright = cf.__copyright__
copyright = copyright[10:] if copyright.startswith("Copyright ") else copyright
version = cf.__version__[:cf.__version__.index(".", 2)]
release = cf.__version__
language = "en"

templates_path = ["_templates"]
html_static_path = ["_static"]
master_doc = "index"
source_suffix = ".rst"
exclude_patterns = []
pygments_style = "sphinx"
add_module_names = False

html_title = project + " Documentation"
html_logo = "../assets/logo_dark.png"
html_favicon = "../assets/fav_bright.ico"
html_theme = "sphinx_book_theme"
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
    })
elif html_theme == "sphinx_book_theme":
    copyright = copyright.split(",", 1)[0]
    html_theme_options.update({
        "logo_only": True,
        "home_page_in_toc": True,
        "show_navbar_depth": 2,
        "show_toc_level": 2,
        "repository_url": "https://github.com/uhh-cms/columnflow",
        "use_repository_button": True,
        "use_issues_button": True,
        "use_edit_page_button": True,
    })

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosectionlabel",
    "autodocsumm",
    "myst_parser",
    "sphinx_lfs_content",
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
    if html_theme in ("sphinx_rtd_theme", "alabaster", "sphinx_book_theme"):
        app.add_css_file("styles_{}.css".format(html_theme))

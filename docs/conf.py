# coding: utf-8


import sys
import os


# adjust the environment in a minimal way just so that the docs build
projdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(projdir, "docs", "_extensions"))
sys.path.insert(0, os.path.join(projdir, "modules", "law"))
sys.path.insert(0, os.path.join(projdir, "modules", "order"))
sys.path.insert(0, projdir)
# os.environ["LAW_CONFIG_FILE"] = os.path.join(projdir, "docs", "law.cfg")
os.environ["LAW_CONFIG_FILE"] = os.path.join(projdir, "law.cfg")

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
html_favicon = "../assets/fav_bright.ico"
html_theme = "sphinx_book_theme"
html_theme_options = {}
if html_theme == "sphinx_rtd_theme":
    html_logo = "../assets/logo_dark.png"
    html_theme_options.update({
        "logo_only": True,
        "prev_next_buttons_location": None,
        "collapse_navigation": False,
    })
elif html_theme == "alabaster":
    html_logo = "../assets/logo_dark.png"
    html_theme_options.update({
        "github_user": "columnflow",
        "github_repo": "columnflow",
    })
elif html_theme == "sphinx_book_theme":
    copyright = copyright.split(",", 1)[0]
    html_theme_options.update({
        "home_page_in_toc": True,
        "show_navbar_depth": 2,
        "show_toc_level": 2,
        "repository_url": "https://github.com/columnflow/columnflow",
        "use_repository_button": True,
        "use_issues_button": True,
        "use_edit_page_button": True,
        "logo": {
            "image_light": "../assets/logo_dark.png",
            "image_dark": "../assets/logo_bright.png",
        },
    })

extensions = [
    "sphinx_design",
    "sphinx_copybutton",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autodoc",
    "sphinx_autodoc_typehints",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosectionlabel",
    "sphinxcontrib.mermaid",
    "sphinx_lfs_content",
    "autodocsumm",
    "myst_parser",
    "pydomain_patch",
]

myst_enable_extensions = ["colon_fence"]

typehints_defaults = "comma"

autodoc_default_options = {
    "member-order": "bysource",
    "show-inheritance": True,
}

autosectionlabel_prefix_document = True

intersphinx_aliases = {
    # alias for a class that was imported at its package level
    ("py:class", "awkward.highlevel.Array"):
        ("py:class", "ak.Array"),
}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "coffea": ("https://coffeateam.github.io/coffea", None),
    "law": ("https://law.readthedocs.io/en/latest/", None),
    "order": ("https://python-order.readthedocs.io/en/latest/", None),
    "ak": ("https://awkward-array.org/doc/main", None),
    "awkward": ("https://awkward-array.org/doc/main", None),
    "correctionlib": ("https://cms-nanoaod.github.io/correctionlib", None),
    "scipy": ("https://docs.scipy.org/doc/scipy/", None),
    "uproot": ("https://uproot.readthedocs.io/en/latest/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
}


def add_intersphinx_aliases_to_inv(app):
    from sphinx.ext.intersphinx import InventoryAdapter

    inventories = InventoryAdapter(app.builder.env)

    for alias, target in app.config.intersphinx_aliases.items():
        alias_domain, alias_name = alias
        target_domain, target_name = target
        try:
            found = inventories.main_inventory[target_domain][target_name]
            inventories.main_inventory[alias_domain][alias_name] = found
        except KeyError:
            continue


# setup the app
def setup(app):
    # implement intersphinx aliases with workaround
    app.add_config_value("intersphinx_aliases", {}, "env")
    app.connect("builder-inited", add_intersphinx_aliases_to_inv)

    # set style sheets
    app.add_css_file("styles_common.css")
    if html_theme in ("sphinx_rtd_theme", "alabaster", "sphinx_book_theme"):
        app.add_css_file("styles_{}.css".format(html_theme))

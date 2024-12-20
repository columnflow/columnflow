# coding: utf-8


import os
from setuptools import setup, find_packages  # type: ignore
from glob import glob


this_dir = os.path.dirname(os.path.abspath(__file__))

# read the readme file
with open(os.path.join(this_dir, "README.md"), "r") as f:
    long_description = f.read()

# load package infos
pkg = {}  # type: ignore
with open(os.path.join(this_dir, "columnflow", "__version__.py"), "r") as f:
    exec(f.read(), pkg)

scripts: list[str] = list()
for f in list(glob(os.path.join("bin", "*"))):
    if os.path.isfile(f):
        scripts.append(f)

setup(
    name="columnflow",
    version=pkg["__version__"],
    author=pkg["__author__"],
    author_email=pkg["__email__"],
    description=pkg["__doc__"].strip().split("\n")[0].strip(),
    license=pkg["__license__"],
    url=pkg["__contact__"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    # install shell commands
    scripts=scripts,
    zip_safe=False,
    packages=find_packages(exclude=["tests"]),
)

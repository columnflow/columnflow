# coding: utf-8


import os
from setuptools import setup, find_packages


this_dir = os.path.dirname(os.path.abspath(__file__))


keywords = [
    "physics", "analysis", "experiment", "columnar", "vectoized",
    "law", "order", "luigi", "lhc", "cms",
]


classifiers = [
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Development Status :: 4 - Beta",
    "Operating System :: OS Independent",
    "License :: OSI Approved :: BSD License",
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "Intended Audience :: Information Technology",
]


# read the readme file
with open(os.path.join(this_dir, "README.rst"), "r") as f:
    long_description = f.read()


# load installation requirements
with open(os.path.join(this_dir, "requirements_prod.txt"), "r") as f:
    install_requires = [line.strip() for line in f.readlines() if line.strip()]


# load package infos
pkg = {}
with open(os.path.join(this_dir, "columnflow", "__version__.py"), "r") as f:
    exec(f.read(), pkg)


setup(
    name="columnflow",
    version=pkg["__version__"],
    author=pkg["__author__"],
    author_email=pkg["__email__"],
    description=pkg["__doc__"].strip().split("\n")[0].strip(),
    license=pkg["__license__"],
    url=pkg["__contact__"],
    keywords=keywords,
    classifiers=classifiers,
    long_description=long_description,
    install_requires=install_requires,
    python_requires=">=3.6, <4",
    zip_safe=False,
    packages=find_packages(exclude=["tests"]),
)

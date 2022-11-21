# coding: utf-8
# flake8: noqa

"""
Entry point for all tests.
"""

__all__ = []


# adjust the path to import the package
import os
import sys
base = os.path.normpath(os.path.join(os.path.abspath(__file__), "../.."))
sys.path.append(base)
import plhld  # noqa

# import all tests
# ...

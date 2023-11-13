# coding: utf-8


__all__ = ["UtilTest"]

import unittest

from columnflow.util import create_random_name


class UtilTest(unittest.TestCase):

    def test_create_random_name(self):
        self.assertIsInstance(create_random_name(), str)

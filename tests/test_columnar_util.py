# coding: utf-8


__all__ = ["ColumnarUtilTest"]


import unittest

from ap.columnar_util import add_ak_alias


class ColumnarUtilTest(unittest.TestCase):

    def test_add_ak_alias(self):
        print(add_ak_alias)
        self.assertEqual(1, 1)

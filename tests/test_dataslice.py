# The MIT License (MIT)
# Copyright (c) 2021 by Brockmann Consult GmbH and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
import unittest

import pytest

from nc2zarr.dataslice import append_slice
from nc2zarr.dataslice import find_slice
from nc2zarr.dataslice import update_slice
from tests.helpers import IOCollector
from tests.helpers import new_append_test_datasets

import numpy as np
import xarray as xr

class DatasliceTest(unittest.TestCase, IOCollector):
    def setUp(self):
        self.reset_paths()

    def tearDown(self):
        self.delete_paths()

    def test_find_slice_no_store(self):
        self.assertEqual(
            (-1, "create"),
            find_slice("/this/path/does/not/exist", 42, "arbitrary_string")
        )

    def test_update_slice_illegal_mode(self):
        with pytest.raises(ValueError, match="illegal mode value"):
            # noinspection PyTypeChecker
            update_slice("/does/not/exist", 42, None, "not_a_valid_mode")

    def test_append_slice_with_coordinates_attribute(self):
        dst_path = "my.zarr"
        self.add_path(dst_path)
        ds1, ds2 = new_append_test_datasets(
            ["2001-01-01"],
            ["2001-01-02", "2001-01-03"]
        )
        ds1.attrs["coordinates"] = "test value 1"
        ds2.attrs["coordinates"] = "test value 2"
        ds1.to_zarr(dst_path)
        append_slice(dst_path, ds2, dimension="t")
        ds3 = xr.open_zarr(dst_path, decode_coords=False)
        self.assertNotIn("coordinates", ds3.attrs)

    def test_update_slice_with_append_dimension_not_first(self):
        dst_path = "my.zarr"
        self.add_path(dst_path)
        ds = xr.Dataset(
            {"v": (["x", "y", "t"], np.zeros((2, 2, 1)))},
            coords={"t": np.array(["2001-01-01"], dtype="datetime64"),
                    "x": np.array([0, 1]), "y": np.array([0, 1])
                    })
        ds.to_zarr(dst_path)
        with pytest.raises(ValueError, match="must be first dimension"):
            update_slice(dst_path, 0, ds, "replace", dimension="t")
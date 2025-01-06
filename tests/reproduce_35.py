# The MIT License (MIT)
# Copyright (c) 2021–2025 by Brockmann Consult GmbH and contributors
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

import shutil
import unittest

import numpy as np
import xarray as xr
import zarr

SRC_DS_1_PATH = 'src_ds_1.zarr'
SRC_DS_2_PATH = 'src_ds_2.zarr'
DST_DS_PATH = 'dst_ds.zarr'


class XarrayToZarrAppendInconsistencyTest(unittest.TestCase):
    @classmethod
    def del_paths(cls):
        for path in (SRC_DS_1_PATH, SRC_DS_2_PATH, DST_DS_PATH):
            shutil.rmtree(path, ignore_errors=True)

    def setUp(self):
        self.del_paths()

        scale_factor = 0.0001
        self.v_values_encoded = np.array([[0, 10000, 15000, 20000]], dtype=np.uint16)
        self.v_values_decoded = np.array([[np.nan, 1., 1.5, 2.]], dtype=np.float32)

        # The variable for the two source datasets
        v = xr.DataArray(self.v_values_encoded,
                         dims=('t', 'x'),
                         attrs=dict(scale_factor=scale_factor, _FillValue=0))

        # Create two source datasets
        src_ds = xr.Dataset(data_vars=dict(v=v))
        src_ds.to_zarr(SRC_DS_1_PATH)
        src_ds.to_zarr(SRC_DS_2_PATH)

        # Assert we have written encoded data
        a1 = zarr.convenience.open_array(SRC_DS_1_PATH + '/v')
        a2 = zarr.convenience.open_array(SRC_DS_2_PATH + '/v')
        np.testing.assert_equal(a1, self.v_values_encoded)  # succeeds
        np.testing.assert_equal(a2, self.v_values_encoded)  # succeeds

        # Assert we correctly decode data
        src_ds_1 = xr.open_zarr(SRC_DS_1_PATH, decode_cf=True)
        src_ds_2 = xr.open_zarr(SRC_DS_2_PATH, decode_cf=True)
        np.testing.assert_equal(src_ds_1.v.data, self.v_values_decoded)  # succeeds
        np.testing.assert_equal(src_ds_2.v.data, self.v_values_decoded)  # succeeds

    def tearDown(self):
        self.del_paths()

    def test_decode_cf_true(self):
        """
        This test succeeds.
        """
        # Open the two source datasets
        src_ds_1 = xr.open_zarr(SRC_DS_1_PATH, decode_cf=True)
        src_ds_2 = xr.open_zarr(SRC_DS_2_PATH, decode_cf=True)
        # Expect data is decoded
        np.testing.assert_equal(src_ds_1.v.data, self.v_values_decoded)  # succeeds
        np.testing.assert_equal(src_ds_2.v.data, self.v_values_decoded)  # succeeds

        # Write 1st source datasets to new dataset, append the 2nd source
        src_ds_1.to_zarr(DST_DS_PATH, mode='w-')
        src_ds_2.to_zarr(DST_DS_PATH, append_dim='t')

        # Open the new dataset
        dst_ds = xr.open_zarr(DST_DS_PATH, decode_cf=True)
        dst_ds_1 = dst_ds.isel(t=slice(0, 1))
        dst_ds_2 = dst_ds.isel(t=slice(1, 2))
        # Expect data is decoded
        np.testing.assert_equal(dst_ds_1.v.data, self.v_values_decoded)  # succeeds
        np.testing.assert_equal(dst_ds_2.v.data, self.v_values_decoded)  # succeeds

    def test_decode_cf_false(self):
        """
        This test fails by the last assertion with

        AssertionError:
        Arrays are not equal

        Mismatched elements: 3 / 4 (75%)
        Max absolute difference: 47600
        Max relative difference: 4.76
         x: array([[    0, 57600, 53632, 49664]], dtype=uint16)
         y: array([[    0, 10000, 15000, 20000]], dtype=uint16)
        """
        # Open the two source datasets
        src_ds_1 = xr.open_zarr(SRC_DS_1_PATH, decode_cf=False)
        src_ds_2 = xr.open_zarr(SRC_DS_2_PATH, decode_cf=False)
        # Expect data is NOT decoded (still encoded)
        np.testing.assert_equal(src_ds_1.v.data, self.v_values_encoded)  # succeeds
        np.testing.assert_equal(src_ds_2.v.data, self.v_values_encoded)  # succeeds

        # Write 1st source datasets to new dataset, append the 2nd source
        src_ds_1.to_zarr(DST_DS_PATH, mode='w-')
        # Avoid ValueError: failed to prevent overwriting existing key scale_factor in attrs. ...
        del src_ds_2.v.attrs['scale_factor']
        del src_ds_2.v.attrs['_FillValue']
        src_ds_2.to_zarr(DST_DS_PATH, append_dim='t')

        # Open the new dataset
        dst_ds = xr.open_zarr(DST_DS_PATH, decode_cf=False)
        dst_ds_1 = dst_ds.isel(t=slice(0, 1))
        dst_ds_2 = dst_ds.isel(t=slice(1, 2))
        # Expect data is NOT decoded (still encoded)
        np.testing.assert_equal(dst_ds_1.v.data, self.v_values_encoded)  # succeeds
        np.testing.assert_equal(dst_ds_2.v.data, self.v_values_encoded)  # fails


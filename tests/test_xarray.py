# The MIT License (MIT)
# Copyright (c) 2022 by Brockmann Consult GmbH and contributors
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


class XarrayTest(unittest.TestCase):
    """Make sure some xarray methods work as expected."""

    @classmethod
    def new_input_dataset(cls, offset, size) -> xr.Dataset:
        return xr.Dataset(dict(time=xr.DataArray(np.arange(offset, offset + size * 100, 100),
                                                 dims='sounding_dim'),
                               pressure=xr.DataArray(np.random.random(size * 20).reshape((size, 20)),
                                                     dims=['sounding_dim', 'levels_dim'])))

    def test_swap_dims(self):
        ds1 = self.new_input_dataset(offset=1000, size=6)
        ds2 = ds1.swap_dims({'sounding_dim': 'time'})
        np.testing.assert_equal(ds1.time.values, ds2.time.values)
        np.testing.assert_equal(ds1.pressure.values, ds2.pressure.values)
        ds3 = ds2.rename_dims({'time': 'sounding_dim'})
        time = ds3.coords['time']
        del ds3.coords['time']
        ds3 = ds3.assign(time=time)
        xr.testing.assert_equal(ds3, ds1)

    def test_rename_dims(self):
        ds1 = self.new_input_dataset(offset=1000, size=6)
        ds2 = ds1.rename_dims({'sounding_dim': 'time2'})
        np.testing.assert_equal(ds1.time.values, ds2.time.values)
        np.testing.assert_equal(ds1.pressure.values, ds2.pressure.values)
        ds3 = ds2.rename_dims({'time2': 'sounding_dim'})
        xr.testing.assert_equal(ds3, ds1)

    def test_append_dim(self):
        ds1 = xr.Dataset(
            dict(
                pressure=xr.DataArray(np.linspace(0, 1, 20).reshape((5, 4)),
                                      dims=('sounding_dim', 'level_dim')),
                time=xr.DataArray([0, 1, 2, 3, 4],
                                  dims='sounding_dim')
            )
        )
        ds2 = xr.Dataset(
            dict(
                pressure=xr.DataArray(np.linspace(1, 2, 20).reshape((5, 4)),
                                      dims=('sounding_dim', 'level_dim')),
                time=xr.DataArray([5, 6, 8, 7, 9],  # Order!
                                  dims='sounding_dim')
            )
        )

        ds_expected = xr.Dataset(
            dict(
                pressure=xr.DataArray(np.concatenate([np.linspace(0, 1, 20),
                                                      np.linspace(1, 2, 20)]).reshape((10, 4)),
                                      dims=('sounding_dim', 'level_dim')),
                time=xr.DataArray([0, 1, 2, 3, 4,
                                   5, 6, 8, 7, 9],
                                  dims='sounding_dim')
            )
        )

        try:
            ds1.to_zarr('append_test.zarr', mode='w')
            ds2.to_zarr('append_test.zarr', append_dim='sounding_dim')
            ds_actual = xr.open_zarr('append_test.zarr', decode_cf=False).compute()
            xr.testing.assert_equal(ds_expected, ds_actual)
        finally:
            shutil.rmtree('append_test.zarr', ignore_errors=True)

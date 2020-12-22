# The MIT License (MIT)
# Copyright (c) 2020 by Brockmann Consult GmbH and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
import os.path
import shutil
from typing import List

import numpy as np
import xarray as xr


class PathCollector:

    def __init__(self):
        self._outputs: List[str] = []

    def reset_paths(self):
        self._outputs = []

    def add_path(self, path, ensure_deleted=True):
        self._outputs.append(path)
        if ensure_deleted:
            delete_path(path)

    def delete_paths(self, ignore_errors=False):
        for path in self._outputs:
            delete_path(path, ignore_errors=ignore_errors)


class IOCollector(PathCollector):

    def add_inputs(self, input_dir_path, day_offset=1, num_days=5, prefix='input'):
        self.add_path(input_dir_path)
        for day in range(day_offset, day_offset + num_days):
            self.add_input(input_dir_path, day, prefix=prefix, add=False)

    def add_input(self, input_dir_path, day, prefix='input', add=True):
        input_path = os.path.join(input_dir_path, '{}-{:02d}.nc'.format(prefix, day))
        if add:
            self.add_path(input_path)
        if not os.path.exists(input_dir_path):
            os.makedirs(input_dir_path)
        ds = new_test_dataset(w=36, h=18, day=day)
        ds.to_netcdf(input_path)


def delete_path(path, ignore_errors=False):
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=ignore_errors)
    elif os.path.exists(path):
        os.remove(path)


def new_test_dataset(w: int = 36,
                     h: int = 18,
                     day: int = None):
    res = 180 / h
    lon = xr.DataArray(np.linspace(-180 + res, 180 - res, num=w), dims=('lon',))
    lat = xr.DataArray(np.linspace(-90 + res, 90 - res, num=h), dims=('lat',))

    if day is None:
        var_dims = ('lat', 'lon')
        var_shape = (h, w)
        coords = dict(lon=lon, lat=lat)
    else:
        var_dims = ('time', 'lat', 'lon')
        var_shape = (1, h, w)
        time = xr.DataArray(np.array(['2020-12-{:02d}T10:00:00'.format(day)],
                                     dtype='datetime64[s]'),
                            dims=('time',))
        time.encoding.update(
            calendar="proleptic_gregorian",
            units="seconds since 1970-01-01 00:00:00"
        )
        coords = dict(lon=lon, lat=lat, time=time)

    r_ui16 = xr.DataArray(
        np.random.randint(0, 1000, size=var_shape).astype(dtype=np.uint16),
        dims=var_dims,
        attrs=dict(_FillValue=9999,
                   scale_factor=1 / 1000,
                   add_offset=0.0)
    )
    r_i32 = xr.DataArray(
        np.random.randint(0, 1000, size=var_shape).astype(dtype=np.int32),
        dims=var_dims,
        attrs=dict(_FillValue=-1,
                   scale_factor=1 / 1000,
                   add_offset=0.0)
    )
    r_f32 = xr.DataArray(
        np.random.random(size=var_shape).astype(dtype=np.float32),
        dims=var_dims,
        attrs=dict(_FillValue=float('nan'))
    )

    data_vars = dict(r_ui16=r_ui16, r_i32=r_i32, r_f32=r_f32)

    return xr.Dataset(data_vars=data_vars, coords=coords)

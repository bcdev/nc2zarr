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

    def add_path(self, path, keep=False, delete=True):
        if not keep:
            self._outputs.append(path)
        if delete:
            delete_path(path)

    def delete_paths(self, ignore_errors=False):
        for path in self._outputs:
            delete_path(path, ignore_errors=ignore_errors)


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
        np.random.random_integers(0, 1000, size=var_shape).astype(dtype=np.uint16),
        dims=var_dims,
        attrs=dict(_FillValue=9999,
                   scale_factor=1 / 1000,
                   add_offset=0.0)
    )
    r_i32 = xr.DataArray(
        np.random.random_integers(0, 1000, size=var_shape).astype(dtype=np.int32),
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

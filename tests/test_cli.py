import os
import os.path
import shutil
import unittest
from typing import List

import click
import click.testing
import numpy as np
import xarray as xr

from nc2zarr.cli import nc2zarr


class MainTest(unittest.TestCase):

    def _invoke_cli(self, args: List[str]):
        self.runner = click.testing.CliRunner()
        return self.runner.invoke(nc2zarr, args, catch_exceptions=False)


class NoOpMainTest(MainTest):
    def test_noargs(self):
        self.assertEqual(1, self._invoke_cli([]).exit_code)

    def test_version(self):
        self.assertEqual(0, self._invoke_cli(['--version']).exit_code)

    def test_help(self):
        self.assertEqual(0, self._invoke_cli(['--help']).exit_code)


class OpMainTest(MainTest):
    input_dir = os.path.join(os.path.dirname(__file__), 'inputs')

    def test_slices(self):
        self.output('out.zarr')
        result = self._invoke_cli([os.path.join(self.input_dir, '*.nc')])
        self.assertEqual(0, result.exit_code)
        self.assertTrue(os.path.isdir('out.zarr'))
        ds = xr.open_zarr('out.zarr')
        self.assertEqual({'lon', 'lat', 'time', 'chl_1', 'chl_2', 'chl_3'},
                         set(ds.variables))
        self.assertEqual(5, len(ds.time))
        np.testing.assert_equal(ds.time.values,
                                np.array(['2020-12-01T10:00:00',
                                          '2020-12-02T10:00:00',
                                          '2020-12-03T10:00:00',
                                          '2020-12-04T10:00:00',
                                          '2020-12-05T10:00:00'], dtype='datetime64'))

    def test_multi_file(self):
        self.output('out.zarr')
        result = self._invoke_cli(['--multi-file', os.path.join(self.input_dir, '*.nc')])
        self.assertEqual(0, result.exit_code)
        self.assertTrue(os.path.isdir('out.zarr'))
        ds = xr.open_zarr('out.zarr')
        self.assertEqual({'lon', 'lat', 'time', 'chl_1', 'chl_2', 'chl_3'},
                         set(ds.variables))
        self.assertEqual(5, len(ds.time))
        np.testing.assert_equal(ds.time.values,
                                np.array(['2020-12-01T10:00:00',
                                          '2020-12-02T10:00:00',
                                          '2020-12-03T10:00:00',
                                          '2020-12-04T10:00:00',
                                          '2020-12-05T10:00:00'], dtype='datetime64'))

    def output(self, path, keep=False):
        if not keep:
            self._outputs.append(path)
        self._delete(path)

    def setUp(self):
        self._outputs = []

    def tearDown(self):
        for path in self._outputs:
            self._delete(path)

    def _delete(self, path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)

    @classmethod
    def setUpClass(cls):
        if not os.path.exists(cls.input_dir):
            os.mkdir(cls.input_dir)
        else:
            cls._remove_inputs()
        num_days = 5
        for day in range(1, num_days + 1):
            ds = cls._new_test_dataset(w=36, h=18, day=day)
            ds.to_netcdf(os.path.join(cls.input_dir, 'CHL-{:02d}.nc'.format(day)))

    @classmethod
    def tearDownClass(cls):
        cls._remove_inputs()

    @classmethod
    def _remove_inputs(cls):
        shutil.rmtree(cls.input_dir, ignore_errors=True)

    @classmethod
    def _new_test_dataset(cls, w: int, h: int, day: int):
        res = 180 / h
        ds = xr.Dataset(
            data_vars=dict(
                chl_1=xr.DataArray(
                    np.random.random(size=(1, h, w)),
                    dims=('time', 'lat', 'lon')
                ),
                chl_2=xr.DataArray(
                    np.random.random(size=(1, h, w)),
                    dims=('time', 'lat', 'lon')
                ),
                chl_3=xr.DataArray(
                    np.random.random(size=(1, h, w)),
                    dims=('time', 'lat', 'lon')
                ),
            ),
            coords=dict(
                lon=xr.DataArray(
                    np.linspace(-180 + res, 180 - res, num=w),
                    dims=('lon',)
                ),
                lat=xr.DataArray(
                    np.linspace(-90 + res, 90 - res, num=h),
                    dims=('lat',)
                ),
                time=xr.DataArray(
                    np.array(['2020-12-{:02d}T10:00:00'.format(day)], dtype='datetime64[s]'),
                    dims=('time',),
                ),
            ))
        ds.time.encoding.update(
            calendar="proleptic_gregorian",
            units="seconds since 1970-01-01 00:00:00"
        )
        return ds

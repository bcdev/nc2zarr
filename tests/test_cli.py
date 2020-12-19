import os
import os.path
import unittest
from typing import List

import click
import click.testing
import numpy as np
import xarray as xr

from nc2zarr.cli import nc2zarr
from tests.helpers import PathCollector
from tests.helpers import delete_path
from tests.helpers import new_test_dataset


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


class OpMainTest(MainTest, PathCollector):
    input_dir = os.path.join(os.path.dirname(__file__), 'inputs')

    def test_slices_with_defaults(self):
        self.add_path('out.zarr')
        result = self._invoke_cli([os.path.join(self.input_dir, '*.nc')])
        self.assertCliResultOk(result, 'out.zarr')

    def test_slices_with_overwrite(self):
        self.add_path('out.zarr')
        result = self._invoke_cli(['--overwrite', os.path.join(self.input_dir, '*.nc')])
        self.assertCliResultOk(result, 'out.zarr')

    def test_multi_file_with_defaults(self):
        self.add_path('out.zarr')
        result = self._invoke_cli(['--multi-file', os.path.join(self.input_dir, '*.nc')])
        self.assertCliResultOk(result, 'out.zarr')

    def assertCliResultOk(self, result, output_path: str):
        self.assertEqual(0, result.exit_code)
        self.assertTrue(os.path.isdir('out.zarr'))
        ds = xr.open_zarr('out.zarr')
        self.assertEqual({'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                         set(ds.variables))
        self.assertEqual(5, len(ds.time))
        np.testing.assert_equal(ds.time.values,
                                np.array(['2020-12-01T10:00:00',
                                          '2020-12-02T10:00:00',
                                          '2020-12-03T10:00:00',
                                          '2020-12-04T10:00:00',
                                          '2020-12-05T10:00:00'], dtype='datetime64'))

    def setUp(self):
        self.reset_paths()

    def tearDown(self):
        self.delete_paths()

    @classmethod
    def setUpClass(cls):
        if not os.path.exists(cls.input_dir):
            os.mkdir(cls.input_dir)
        else:
            delete_path(cls.input_dir, ignore_errors=True)
        num_days = 5
        for day in range(1, num_days + 1):
            ds = new_test_dataset(w=36, h=18, day=day)
            ds.to_netcdf(os.path.join(cls.input_dir, 'CHL-{:02d}.nc'.format(day)))

    @classmethod
    def tearDownClass(cls):
        delete_path(cls.input_dir, ignore_errors=True)

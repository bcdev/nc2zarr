import os
import os.path
import unittest
from typing import List

import click
import click.testing
import numpy as np
import xarray as xr

from nc2zarr.cli import nc2zarr
from tests.helpers import IOCollector


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


class OpMainTest(MainTest, IOCollector):
    def setUp(self):
        self.reset_paths()

    def tearDown(self):
        self.delete_paths()

    def test_slices_with_defaults(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_path('out.zarr')
        result = self._invoke_cli(['inputs/*.nc'])
        self.assertCliResultOk(result,
                               expected_output_path='out.zarr',
                               expected_times=['2020-12-01T10:00:00',
                                               '2020-12-02T10:00:00',
                                               '2020-12-03T10:00:00'])

    def test_slices_with_overwrite(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_path('out.zarr')
        result = self._invoke_cli(['--overwrite', 'inputs/*.nc'])
        self.assertCliResultOk(result,
                               expected_output_path='out.zarr',
                               expected_times=['2020-12-01T10:00:00',
                                               '2020-12-02T10:00:00',
                                               '2020-12-03T10:00:00'])

    def test_multi_file_with_defaults(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_path('out.zarr')
        result = self._invoke_cli(['--multi-file', 'inputs/*.nc'])
        self.assertCliResultOk(result,
                               expected_output_path='out.zarr',
                               expected_times=['2020-12-01T10:00:00',
                                               '2020-12-02T10:00:00',
                                               '2020-12-03T10:00:00'])

    def test_output(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_path('my.zarr')
        result = self._invoke_cli(['--output', 'my.zarr', 'inputs/*.nc'])
        self.assertCliResultOk(result,
                               expected_output_path='my.zarr',
                               expected_times=['2020-12-01T10:00:00',
                                               '2020-12-02T10:00:00',
                                               '2020-12-03T10:00:00'])

    def test_append_one_to_many(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_path('out.zarr')
        result = self._invoke_cli(['inputs/*.nc'])
        self.assertCliResultOk(result, 'out.zarr',
                               expected_times=['2020-12-01T10:00:00',
                                               '2020-12-02T10:00:00',
                                               '2020-12-03T10:00:00'])
        self.add_input('inputs', day=4)
        result = self._invoke_cli(['--append', 'inputs/input-04.nc'])
        self.assertCliResultOk(result,
                               expected_output_path='out.zarr',
                               expected_times=['2020-12-01T10:00:00',
                                               '2020-12-02T10:00:00',
                                               '2020-12-03T10:00:00',
                                               '2020-12-04T10:00:00'])

    def test_append_one_to_one(self):
        self.add_input('inputs', day=1)
        self.add_path('out.zarr')
        result = self._invoke_cli(['--append', 'inputs/input-01.nc'])
        self.assertCliResultOk(result, 'out.zarr',
                               expected_times=['2020-12-01T10:00:00'])
        self.add_input('inputs', day=2)
        result = self._invoke_cli(['--append', 'inputs/input-02.nc'])
        self.assertCliResultOk(result,
                               expected_output_path='out.zarr',
                               expected_times=['2020-12-01T10:00:00',
                                               '2020-12-02T10:00:00'])

    def test_append_zarr_to_zarr(self):
        self.add_inputs('inputs', day_offset=1, num_days=6)
        self.add_path('out-1.zarr')
        self.add_path('out-2.zarr')
        self.add_path('out.zarr')

        result = self._invoke_cli(['-o', 'out-1.zarr',
                                   'inputs/input-01.nc', 'inputs/input-02.nc', 'inputs/input-03.nc'])
        self.assertCliResultOk(result, 'out-1.zarr',
                               expected_times=['2020-12-01T10:00:00',
                                               '2020-12-02T10:00:00',
                                               '2020-12-03T10:00:00'])

        result = self._invoke_cli(['-o', 'out-2.zarr',
                                   'inputs/input-04.nc', 'inputs/input-05.nc', 'inputs/input-06.nc'])
        self.assertCliResultOk(result, 'out-2.zarr',
                               expected_times=['2020-12-04T10:00:00',
                                               '2020-12-05T10:00:00',
                                               '2020-12-06T10:00:00'])

        result = self._invoke_cli(['out-1.zarr', 'out-2.zarr'])
        self.assertCliResultOk(result,
                               expected_output_path='out.zarr',
                               expected_times=['2020-12-01T10:00:00',
                                               '2020-12-02T10:00:00',
                                               '2020-12-03T10:00:00',
                                               '2020-12-04T10:00:00',
                                               '2020-12-05T10:00:00',
                                               '2020-12-06T10:00:00'])

    def assertCliResultOk(self,
                          result,
                          expected_output_path=None,
                          expected_vars=None,
                          expected_times=None):
        expected_output_path = expected_output_path or 'out.zarr'
        expected_vars = expected_vars or {'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'}
        expected_times = expected_times or ['2020-12-01T10:00:00',
                                            '2020-12-02T10:00:00',
                                            '2020-12-03T10:00:00',
                                            '2020-12-04T10:00:00',
                                            '2020-12-05T10:00:00']
        if result.exit_code != 0:
            if result.stderr_bytes:
                print(f'stderr: {result.stderr_bytes.decode("utf-8")}')
            if result.stdout_bytes:
                print(f'stdout: {result.stdout_bytes.decode("utf-8")}')
        self.assertEqual(0, result.exit_code, msg='Unexpected exit code')
        self.assertTrue(os.path.isdir(expected_output_path))
        ds = xr.open_zarr(expected_output_path)
        self.assertEqual(expected_vars, set(ds.variables))
        self.assertEqual(len(expected_times), len(ds.time))
        np.testing.assert_equal(ds.time.values,
                                np.array(expected_times, dtype='datetime64'))



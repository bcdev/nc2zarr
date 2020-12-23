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

import subprocess
import sys
import unittest
from typing import List, Collection

import click
import click.testing

from nc2zarr.cli import nc2zarr
from tests.helpers import IOCollector
from tests.helpers import ZarrOutputTestMixin


class MainTest(unittest.TestCase):

    def _invoke_cli(self, args: List[str]):
        self.runner = click.testing.CliRunner()
        return self.runner.invoke(nc2zarr, args, catch_exceptions=False)


class NoOpCliTest(MainTest):
    def test_noargs(self):
        self.assertEqual(1, self._invoke_cli([]).exit_code)

    def test_version(self):
        self.assertEqual(0, self._invoke_cli(['--version']).exit_code)

    def test_help(self):
        self.assertEqual(0, self._invoke_cli(['--help']).exit_code)

    def test_help_main(self):
        subprocess.call([sys.executable, '-m', 'nc2zarr.cli', '--help'])


class CliTest(MainTest, ZarrOutputTestMixin, IOCollector):
    def setUp(self):
        self.reset_paths()

    def tearDown(self):
        self.delete_paths()

    def test_0_inputs(self):
        self.add_output('out.zarr')
        result = self._invoke_cli([])
        self.assertCliResultError(result, 1,
                                  expected_stdout='Error: At least one input must be given.')

    def test_3_netcdf_inputs(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_output('out.zarr')
        result = self._invoke_cli(['inputs/*.nc'])
        self.assertCliResultOk(result,
                               'out.zarr',
                               expected_vars={'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                               expected_times=['2020-12-01T10:00:00',
                                               '2020-12-02T10:00:00',
                                               '2020-12-03T10:00:00'])

    def assertCliResultOk(self,
                          result,
                          expected_output_path: str,
                          expected_vars: Collection[str],
                          expected_times: Collection[str]):
        if result.exit_code != 0:
            self._dump_cli_output(result)
        self.assertEqual(0, result.exit_code)
        self.assertZarrOutputOk(expected_output_path,
                                expected_vars=expected_vars,
                                expected_times=expected_times)

    def assertCliResultError(self,
                             result,
                             expected_error_code: int = None,
                             expected_stdout: str = None,
                             expected_stderr: str = None):
        if result.exit_code != 0:
            self._dump_cli_output(result)

        if expected_error_code is not None:
            self.assertEqual(expected_error_code, result.exit_code)
        else:
            self.assertTrue(result.exit_code != 0)

        if expected_stdout is not None:
            self.assertIn(expected_stdout,
                          result.stdout_bytes.decode("utf-8") if result.stdout_bytes else '')
        if expected_stderr is not None:
            self.assertIn(expected_stderr,
                          result.stderr_bytes.decode("utf-8") if result.stderr_bytes else '')

    @staticmethod
    def _dump_cli_output(result):
        if result.stderr_bytes:
            print(f'stderr: {result.stderr_bytes.decode("utf-8")}')
        if result.stdout_bytes:
            print(f'stdout: {result.stdout_bytes.decode("utf-8")}')

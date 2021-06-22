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
import subprocess
import sys
import unittest
from typing import List, Collection, Callable, Union, Optional

import click
import click.testing

from nc2zarr.cli import expand_config_template_variables
from nc2zarr.cli import nc2zarr
from nc2zarr.cli import nc2zarr_batch
from tests.helpers import IOCollector
from tests.helpers import ZarrOutputTestMixin


class CliTest(unittest.TestCase):

    def invoke_cli(self, args: List[str], main: Callable = nc2zarr):
        self.runner = click.testing.CliRunner()
        return self.runner.invoke(main, args, catch_exceptions=False)

    @staticmethod
    def dump_cli_output(result):
        if result.stderr_bytes:
            print(f'stderr: {result.stderr_bytes.decode("utf-8")}')
        if result.stdout_bytes:
            print(f'stdout: {result.stdout_bytes.decode("utf-8")}')

    def assertCliResult(self,
                        result,
                        expected_exit_code: int = None,
                        expected_stdout: Union[str, List[str]] = None,
                        expected_stderr: Union[str, List[str]] = None):
        if expected_exit_code is not None:
            if result.exit_code != expected_exit_code:
                self.dump_cli_output(result)
            self.assertEqual(expected_exit_code,
                             result.exit_code)
        self._assertOutput(result.stdout_bytes, expected_stdout, 'stdout')
        self._assertOutput(result.stderr_bytes, expected_stderr, 'stderr')

    def _assertOutput(self,
                      out_bytes: Optional[bytes],
                      expected_parts: Union[str, List[str]],
                      msg: str):
        if expected_parts is not None:
            actual = out_bytes.decode("utf-8") if out_bytes else ''
            if isinstance(expected_parts, str):
                expected_parts = [expected_parts]
            for expected_part in expected_parts:
                self.assertIn(expected_part, actual, msg=msg)


class NoOpNc2zarrCliTest(CliTest):
    def test_noargs(self):
        self.assertCliResult(self.invoke_cli([]), expected_exit_code=1)

    def test_version(self):
        self.assertCliResult(self.invoke_cli(['--version']), expected_exit_code=0)

    def test_help(self):
        self.assertCliResult(self.invoke_cli(['--help']), expected_exit_code=0)

    def test_help_main(self):
        subprocess.call([sys.executable, '-m', 'nc2zarr.cli', '--help'])


class Nc2zarrCliTest(CliTest, ZarrOutputTestMixin, IOCollector):
    def setUp(self):
        self.reset_paths()

    def tearDown(self):
        self.delete_paths()

    def test_0_inputs(self):
        self.add_output('out.zarr')
        result = self.invoke_cli([])
        self.assertCliResult(result,
                             expected_exit_code=1,
                             expected_stdout='Error: At least one input must be given.')

    def test_3_netcdf_inputs(self):
        self.add_inputs('inputs', day_offset=1, num_days=3, add_time_bnds=True)
        self.add_output('out.zarr')
        result = self.invoke_cli(['--sort-by', 'path', 'inputs/*.nc'])
        ds = self.assertCliResultOk(result,
                                    'out.zarr',
                                    expected_vars={'lon', 'lat', 'time', 'time_bnds',
                                                   'r_ui16', 'r_i32', 'r_f32'},
                                    expected_times=['2020-12-01T10:00:00',
                                                    '2020-12-02T10:00:00',
                                                    '2020-12-03T10:00:00'])
        self.assertEqual({}, ds.attrs)

    def test_3_netcdf_inputs_finalize_only(self):
        self.add_inputs('inputs', day_offset=1, num_days=3, add_time_bnds=True)
        self.add_output('out.zarr')
        result = self.invoke_cli(['--sort-by', 'path', 'inputs/*.nc'])
        self.assertEqual(0, result.exit_code)
        result = self.invoke_cli(['--adjust-metadata', '--finalize-only',
                                  'inputs/*.nc',
                                  '--sort-by', 'path'])
        ds = self.assertCliResultOk(result,
                                    'out.zarr',
                                    expected_vars={'lon', 'lat', 'time', 'time_bnds',
                                                   'r_ui16', 'r_i32', 'r_f32'},
                                    expected_times=['2020-12-01T10:00:00',
                                                    '2020-12-02T10:00:00',
                                                    '2020-12-03T10:00:00'])
        self.assertIn('history', ds.attrs)
        self.assertEqual('inputs/input-01.nc, '
                         'inputs/input-02.nc, '
                         'inputs/input-03.nc',
                         ds.attrs.get('source', '').replace('\\', '/'))
        self.assertEqual('2020-12-01 09:30:00', ds.attrs.get('time_coverage_start'))
        self.assertEqual('2020-12-03 10:30:00', ds.attrs.get('time_coverage_end'))

    def assertCliResultOk(self,
                          result,
                          expected_output_path: str,
                          expected_vars: Collection[str],
                          expected_times: Collection[str]):
        if result.exit_code != 0:
            self.dump_cli_output(result)
        self.assertEqual(0, result.exit_code)
        return self.assertZarrOutputOk(expected_output_path,
                                       expected_vars=expected_vars,
                                       expected_times=expected_times)


class NoOpNc2zarrBatchCliTest(CliTest):
    def test_noargs(self):
        self.assertCliResult(self.invoke_cli([], main=nc2zarr_batch),
                             expected_exit_code=2)

    def test_help(self):
        self.assertCliResult(self.invoke_cli(['--help'], main=nc2zarr_batch),
                             expected_exit_code=0)


class Nc2zarrBatchCliTest(CliTest, ZarrOutputTestMixin, IOCollector):
    def setUp(self):
        self.reset_paths()

    def tearDown(self):
        self.delete_paths()

    def test_config_template_path_not_found(self):
        result = self.invoke_cli(['--range', 'year', '2010', '2013',
                                  f'config-template.yml',
                                  'batch/${year}.yml'],
                                 main=nc2zarr_batch)
        self.assertCliResult(result,
                             expected_exit_code=1,
                             expected_stdout=['Error: Could not open file',
                                              'config-template.yml',
                                              ': not found'])

    def test_config_path_template_invalid(self):
        # create empty file so it exists
        self.add_path('config-template.yml')
        with open('config-template.yml', 'w') as fp:
            fp.write('')

        result = self.invoke_cli(['--range', 'year', '2010', '2013',
                                  f'config-template.yml',
                                  'batch/${YEAR}.yml'],
                                 main=nc2zarr_batch)
        self.assertCliResult(result,
                             expected_exit_code=2,
                             expected_stdout='Error: reference "${year}" '
                                             'missing in CONFIG_PATH_TEMPLATE')

    def test_scheduler_config_not_found(self):
        # create empty file so it exists
        self.add_path('config-template.yml')
        with open('config-template.yml', 'w') as fp:
            fp.write('')

        result = self.invoke_cli(['--range', 'year', '2010', '2013',
                                  '--scheduler', 'local.yml',
                                  f'config-template.yml',
                                  'batch/${year}.yml'],
                                 main=nc2zarr_batch)
        self.assertCliResult(result,
                             expected_exit_code=1,
                             expected_stdout=['Error: Could not open file ',
                                              'local.yml',
                                              ': not found'])

    def test_fully_configured_run(self):
        base_dir = os.path.dirname(__file__)

        self.add_path(f'{base_dir}/inputs')
        for year in range(2010, 2014):
            self.add_inputs(f'{base_dir}/inputs/{year}', day_offset=1, num_days=3, prefix=f'input-{year}')

        self.add_path(f'{base_dir}/config-template.yml')
        with open(f'{base_dir}/config-template.yml', 'w') as fp:
            fp.write('input:\n'
                     '  paths: ${base_dir}/inputs/${year}/input-*.nc\n'
                     'output:\n'
                     '  path: ${base_dir}/outputs/${year}.zarr\n')

        self.add_path(f'{base_dir}/local-config.yml')
        with open(f'{base_dir}/local-config.yml', 'w') as fp:
            fp.write('type: local\n'
                     'env_vars:\n'
                     '  TEST1: "123"\n'
                     '  TEST2: "ABC"\n')

        self.add_path(f'{base_dir}/batch')
        self.add_path(f'{base_dir}/outputs')
        result = self.invoke_cli(['--range', 'year', '2010', '2013',
                                  '--value', 'base_dir', base_dir,
                                  '--scheduler', f'{base_dir}/local-config.yml',
                                  f'{base_dir}/config-template.yml',
                                  '${base_dir}/batch/${year}.yml'],
                                 main=nc2zarr_batch)

        self.assertCliResult(result,
                             expected_exit_code=0)

        self.assertEqual({
            '2010.err',
            '2010.out',
            '2010.yml',
            '2011.err',
            '2011.out',
            '2011.yml',
            '2012.err',
            '2012.out',
            '2012.yml',
            '2013.err',
            '2013.out',
            '2013.yml',
        }, set(os.listdir(f'{base_dir}/batch')))

        self.assertEqual({
            '2010.zarr',
            '2011.zarr',
            '2012.zarr',
            '2013.zarr',
        }, set(os.listdir(f'{base_dir}/outputs')))


class ConfigTemplateVariablesTest(unittest.TestCase):

    def test_expand_config_template_variables(self):
        self.assertEqual([],
                         expand_config_template_variables((), ()))
        self.assertEqual([{'base_dir': '.', 'year': 2010},
                          {'base_dir': '.', 'year': 2011},
                          {'base_dir': '.', 'year': 2012},
                          {'base_dir': '.', 'year': 2013}],
                         expand_config_template_variables((('year', '2010', '2013'),),
                                                          (('base_dir', '.'),)))

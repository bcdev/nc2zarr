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

import os.path
import unittest

from nc2zarr.error import ConverterError
from nc2zarr.main import nc2zarr
from tests.helpers import IOCollector
from tests.helpers import ZarrOutputTestMixin


class MainTest(unittest.TestCase, IOCollector, ZarrOutputTestMixin):
    def setUp(self):
        self.reset_paths()

    def tearDown(self):
        self.delete_paths()

    def test_defaults(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_output('out.zarr')
        nc2zarr(input_paths='inputs/*.nc')
        self.assertZarrOutputOk('out.zarr',
                                expected_vars={'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                                expected_times=['2020-12-01T10:00:00',
                                                '2020-12-02T10:00:00',
                                                '2020-12-03T10:00:00'])

    def test_explicit_append_dim(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_output('out.zarr')
        nc2zarr(input_paths='inputs/*.nc', output_append_dim='time')
        self.assertZarrOutputOk('out.zarr',
                                expected_vars={'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                                expected_times=['2020-12-01T10:00:00',
                                                '2020-12-02T10:00:00',
                                                '2020-12-03T10:00:00'])

    def test_dry_run_with_higher_verbosity(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_output('out.zarr')
        nc2zarr(input_paths='inputs/*.nc', dry_run=True, verbosity=0)
        self.assertFalse(os.path.exists('out.zarr'))

    def test_slices_with_overwrite(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_output('out.zarr')
        nc2zarr(input_paths='inputs/*.nc', output_overwrite=True)
        self.assertZarrOutputOk('out.zarr',
                                expected_vars={'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                                expected_times=['2020-12-01T10:00:00',
                                                '2020-12-02T10:00:00',
                                                '2020-12-03T10:00:00'])

    def test_multi_file_with_defaults(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_output('out.zarr')
        nc2zarr(input_paths=['inputs/*.nc'], input_multi_file=True)
        self.assertZarrOutputOk('out.zarr',
                                expected_vars={'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                                expected_times=['2020-12-01T10:00:00',
                                                '2020-12-02T10:00:00',
                                                '2020-12-03T10:00:00'])

    def test_output(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_output('my.zarr')
        nc2zarr(input_paths=['inputs/*.nc'], output_path='my.zarr')
        self.assertZarrOutputOk('my.zarr',
                                expected_vars={'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                                expected_times=['2020-12-01T10:00:00',
                                                '2020-12-02T10:00:00',
                                                '2020-12-03T10:00:00'])

    def test_append_one_to_many(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_output('out.zarr')
        nc2zarr(input_paths=['inputs/*.nc'])
        self.assertZarrOutputOk('out.zarr',
                                expected_vars={'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                                expected_times=['2020-12-01T10:00:00',
                                                '2020-12-02T10:00:00',
                                                '2020-12-03T10:00:00'])
        self.add_input('inputs', day=4)
        nc2zarr(input_paths=['inputs/input-04.nc'], output_append=True)
        self.assertZarrOutputOk('out.zarr',
                                expected_vars={'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                                expected_times=['2020-12-01T10:00:00',
                                                '2020-12-02T10:00:00',
                                                '2020-12-03T10:00:00',
                                                '2020-12-04T10:00:00'])

    def test_append_one_to_one(self):
        self.add_input('inputs', day=1)
        self.add_output('out.zarr')
        nc2zarr(input_paths=['inputs/input-01.nc'], output_append=True)
        self.assertZarrOutputOk('out.zarr',
                                expected_vars={'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                                expected_times=['2020-12-01T10:00:00'])
        self.add_input('inputs', day=2)
        nc2zarr(input_paths=['inputs/input-02.nc'], output_append=True)
        self.assertZarrOutputOk('out.zarr',
                                expected_vars={'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                                expected_times=['2020-12-01T10:00:00',
                                                '2020-12-02T10:00:00'])

    def test_append_zarr_to_zarr(self):
        self.add_inputs('inputs', day_offset=1, num_days=6)
        self.add_output('out-1.zarr')
        self.add_output('out-2.zarr')
        self.add_output('out.zarr')

        nc2zarr(input_paths=['inputs/input-01.nc', 'inputs/input-02.nc', 'inputs/input-03.nc'],
                output_path='out-1.zarr')
        self.assertZarrOutputOk('out-1.zarr',
                                expected_vars={'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                                expected_times=['2020-12-01T10:00:00',
                                                '2020-12-02T10:00:00',
                                                '2020-12-03T10:00:00'])

        nc2zarr(input_paths=['inputs/input-04.nc', 'inputs/input-05.nc', 'inputs/input-06.nc'],
                output_path='out-2.zarr')
        self.assertZarrOutputOk('out-2.zarr',
                                expected_vars={'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                                expected_times=['2020-12-04T10:00:00',
                                                '2020-12-05T10:00:00',
                                                '2020-12-06T10:00:00'])

        nc2zarr(input_paths=['out-1.zarr', 'out-2.zarr'])
        self.assertZarrOutputOk('out.zarr',
                                expected_vars={'lon', 'lat', 'time', 'r_ui16', 'r_i32', 'r_f32'},
                                expected_times=['2020-12-01T10:00:00',
                                                '2020-12-02T10:00:00',
                                                '2020-12-03T10:00:00',
                                                '2020-12-04T10:00:00',
                                                '2020-12-05T10:00:00',
                                                '2020-12-06T10:00:00'])

    def test_both_append_dim_and_concat_dim(self):
        self.add_inputs('inputs', day_offset=1, num_days=3)
        self.add_output('out.zarr')
        with self.assertRaises(ConverterError) as cm:
            nc2zarr(input_paths='inputs/*.nc', output_overwrite=True, output_append=True)
        self.assertEqual('Output overwrite and append flags cannot be given both.', f'{cm.exception}')

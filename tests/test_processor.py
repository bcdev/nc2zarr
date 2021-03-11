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

import unittest

import xarray as xr

from nc2zarr.processor import DatasetProcessor
from tests.helpers import new_test_dataset
from tests.test_preprocessor import CustomPreprocessorTest


class DatasetProcessorTest(unittest.TestCase):

    def test_rename(self):
        ds = new_test_dataset(day=1)
        self.assertIn('r_f32', ds)
        processor = DatasetProcessor(process_rename={'r_f32': 'bibo'})
        new_ds, new_encoding = processor.process_dataset(ds)
        self.assertIsInstance(new_ds, xr.Dataset)
        self.assertIn('bibo', new_ds)
        self.assertNotIn('r_f32', new_ds)
        self.assertEqual({}, new_encoding)

    def test_rechunk_default(self):
        ds = new_test_dataset(day=1)
        processor = DatasetProcessor(process_rechunk={'*': dict(lon=8, lat=4, time=1)})
        new_ds, new_encoding = processor.process_dataset(ds)
        self.assertIsNot(ds, new_ds)
        self.assertEqual({
            'r_f32': {'chunks': (1, 4, 8)},
            'r_i32': {'chunks': (1, 4, 8)},
            'r_ui16': {'chunks': (1, 4, 8)},
            'lon': {'chunks': (8,)},
            'lat': {'chunks': (4,)},
            'time': {'chunks': (1,)},
        }, new_encoding)

    def test_rechunk_with_lon_lat_time_unchunked(self):
        ds = new_test_dataset(day=1)
        processor = DatasetProcessor(process_rechunk={'*': dict(lon=8, lat=4, time=1),
                                                      'lon': None,
                                                      'lat': None,
                                                      'time': 100})
        new_ds, new_encoding = processor.process_dataset(ds)
        self.assertIsNot(ds, new_ds)
        self.assertEqual({
            'r_f32': {'chunks': (1, 4, 8)},
            'r_i32': {'chunks': (1, 4, 8)},
            'r_ui16': {'chunks': (1, 4, 8)},
            'lon': {'chunks': (36,)},
            'lat': {'chunks': (18,)},
            'time': {'chunks': (100,)},
        }, new_encoding)

    def test_rechunk_all_unchunked_except_time(self):
        ds = new_test_dataset(day=1)
        processor = DatasetProcessor(process_rechunk={
            '*':
                {
                    'lon': None,
                    'lat': None,
                    'time': 1
                },
            'lon': None,
            'lat': None,
            'time': 128
        })
        new_ds, new_encoding = processor.process_dataset(ds)
        self.assertIsNot(ds, new_ds)
        self.assertEqual({'r_f32': {'chunks': (1, 18, 36)},
                          'r_i32': {'chunks': (1, 18, 36)},
                          'r_ui16': {'chunks': (1, 18, 36)},
                          'lon': {'chunks': (36,)},
                          'lat': {'chunks': (18,)},
                          'time': {'chunks': (128,)}},
                         new_encoding)

    def test_rechunk_with_input(self):
        ds = new_test_dataset(day=1, chunked=True)
        processor = DatasetProcessor(process_rechunk={
            '*':
                {
                    'lon': 'input',
                    'lat': 'input',
                    'time': 1
                },
            'lon': None,
            'lat': None,
            'time': 128
        })
        new_ds, new_encoding = processor.process_dataset(ds)
        self.assertIsNot(ds, new_ds)
        self.assertEqual({'r_f32': {'chunks': (1, 9, 18)},
                          'r_i32': {'chunks': (1, 9, 18)},
                          'r_ui16': {'chunks': (1, 9, 18)},
                          'lon': {'chunks': (36,)},
                          'lat': {'chunks': (18,)},
                          'time': {'chunks': (128,)}},
                         new_encoding)

    def test_rechunk_with_invalid_size(self):
        ds = new_test_dataset()
        processor = DatasetProcessor(process_rechunk={
            '*':
                {
                    'lon': [1, 2, 3],
                    'lat': 'input',
                },
        })
        with self.assertRaises(ValueError) as cm:
            processor.process_dataset(ds)
        self.assertEqual('invalid chunk size: [1, 2, 3]', f'{cm.exception}')

    def test_rechunk_and_encodings_merged(self):
        ds = new_test_dataset(day=1)
        processor = DatasetProcessor(process_rechunk={'r_i32': dict(lon=8, lat=8),
                                                      'lon': None,
                                                      'lat': None},
                                     output_encoding={'r_i32': dict(compressor=None, fill_value=None)})
        new_ds, new_encoding = processor.process_dataset(ds)
        self.assertIsNot(ds, new_ds)
        self.assertEqual({
            'r_f32': {'chunks': (1, 18, 36)},
            'r_i32': {'chunks': (1, 8, 8), 'compressor': None, 'fill_value': None},
            'r_ui16': {'chunks': (1, 18, 36)},
            'lon': {'chunks': (36,)},
            'lat': {'chunks': (18,)},
            'time': {'chunks': (1,)},
        }, new_encoding)

    def test_no_op(self):
        ds = new_test_dataset(day=1)
        processor = DatasetProcessor(process_rechunk={},
                                     output_encoding={})
        new_ds, new_encoding = processor.process_dataset(ds)
        self.assertIs(ds, new_ds)
        self.assertEqual({}, new_encoding)


class CustomProcessorTest(unittest.TestCase):
    @classmethod
    def my_processor(cls, ds: xr.Dataset) -> xr.Dataset:
        return ds

    def test_swap_dims(self):
        processor = DatasetProcessor(process_rename={"pressure": "surface_pressure"},
                                     process_custom_processor="tests.test_processor:"
                                                              "CustomProcessorTest.my_processor",
                                     process_rechunk={"surface_pressure": {
                                         "time": 20,
                                         "levels_dim": 10,
                                     }})
        ds = CustomPreprocessorTest.new_input_dataset(0, size=100)
        ds = CustomPreprocessorTest.my_preprocessor(ds)
        self.assertEqual({'time': 100, 'levels_dim': 20}, ds.dims)
        self.assertEqual(('time',), ds.time.dims)
        self.assertEqual(('time', 'levels_dim'), ds.pressure.dims)

        ds, encoding = processor.process_dataset(ds)
        self.assertEqual({'time': 100, 'levels_dim': 20}, ds.dims)
        self.assertEqual(('time',), ds.time.dims)
        self.assertEqual(None, ds.time.chunks)
        self.assertEqual(('time', 'levels_dim'), ds.surface_pressure.dims)
        self.assertEqual(((20, 20, 20, 20, 20), (10, 10)), ds.surface_pressure.chunks)

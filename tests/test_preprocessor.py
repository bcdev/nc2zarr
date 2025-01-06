# The MIT License (MIT)
# Copyright (c) 2021–2025 by Brockmann Consult GmbH and contributors
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

import unittest
from typing import List

import numpy as np
import xarray as xr

from nc2zarr.error import ConverterError
from nc2zarr.preprocessor import DatasetPreProcessor
from tests.helpers import new_test_dataset


class DatasetPreProcessorTest(unittest.TestCase):

    def test_time_dim_is_added_and_time_bounds_variable_is_converted_to_var(self):
        ds = new_test_dataset(day=5,
                              add_time_bnds=True,
                              add_time_as_dim_to_vars=False,
                              time_bounds_name='date_bnds',
                              add_time_bounds_as_var=True)
        pre_processor = DatasetPreProcessor(input_concat_dim='time')
        ds = pre_processor.preprocess_dataset(ds)
        self.assertIn('time', ds)
        self.assertEqual(np.array(['2020-12-05T10:00:00.000000000'], dtype='datetime64[ns]'),
                         ds.time.data)
        self.assertIn('bounds', ds.time.attrs)
        self.assertEqual('date_bnds', ds.time.bounds)
        self.assertIn('date_bnds', ds)
        self.assertIn('date_bnds', ds.coords)
        self.assertEqual(('time', 'bnds'), ds.date_bnds.dims)
        self.assertIn('r_ui16', ds)
        self.assertEqual(('time', 'lat', 'lon'), ds.r_ui16.dims)

    def test_select_variables(self):
        ds = new_test_dataset(day=1)
        self.assertIn('time', ds)
        pre_processor = DatasetPreProcessor(input_variables=['r_i32', 'lon', 'lat', 'time'], input_concat_dim='time')
        new_ds = pre_processor.preprocess_dataset(ds)
        self.assertIsInstance(new_ds, xr.Dataset)
        self.assertAllInDataset(['r_i32', 'lon', 'lat', 'time'], new_ds)
        self.assertNoneInDataset(['r_ui16', 'r_f32'], new_ds)

    def test_leaves_time_coord_untouched(self):
        ds = new_test_dataset(day=1)
        self.assertIn('time', ds)
        pre_processor = DatasetPreProcessor(input_variables=None, input_concat_dim='time')
        new_ds = pre_processor.preprocess_dataset(ds)
        self.assertIsInstance(new_ds, xr.Dataset)
        self.assertAllInDataset(['r_ui16', 'r_ui16', 'r_i32', 'lon', 'lat', 'time'], new_ds)
        self.assertIn('time', new_ds)
        self.assertEqual(ds.time, new_ds.time)

    def test_time_var_is_scalar(self):
        ds = new_test_dataset(day=2)
        time = ds.time
        ds = ds.rename_vars({'time': 't'})
        ds = ds.swap_dims({'time': 't'})
        ds = ds.assign_coords(
            {'time_tmp': (xr.DataArray(time[0].values.item(), dims=()))}
        )
        ds = ds.rename_vars({'time_tmp': 'time'})
        ds.time.attrs.update(time.attrs)
        ds.time.encoding.update(time.encoding)
        ds = ds.drop_vars('t')
        pre_processor = DatasetPreProcessor(input_variables=None, input_concat_dim='time')
        new_ds = pre_processor.preprocess_dataset(ds)
        self.assertIsInstance(new_ds, xr.Dataset)
        self.assertAllInDataset(['r_ui16', 'r_ui16', 'r_i32', 'lon', 'lat', 'time'],
                                new_ds)
        self.assertIn('time', new_ds)
        self.assertEqual(('time',), new_ds.time.dims)
        self.assertEqual((1,), new_ds.time.shape)
        self.assertEqual(ds.time, new_ds.time)
        self.assertEqual({"long_name": "time"}, new_ds.time.attrs)
        self.assertEqual({'calendar': 'proleptic_gregorian',
                          'units': 'seconds since 1970-01-01 00:00:00'},
                         new_ds.time.encoding)

    def test_adds_time_dim_from_iso_format_attrs(self):
        ds = new_test_dataset(day=None)
        ds.attrs.update(time_coverage_start='2020-09-08 10:30:00',
                        time_coverage_end='2020-09-08 12:30:00')
        self._test_adds_time_dim(ds)

    def test_adds_time_dim_from_iso_format_attrs_2(self):
        ds = new_test_dataset(day=None)
        ds.attrs.update(time_coverage_start='2020-09-08T10:30:00Z',
                        time_coverage_end='2020-09-08T12:30:00Z')
        self._test_adds_time_dim(ds)

    def test_adds_time_dim_from_non_iso_format_attrs(self):
        ds = new_test_dataset(day=None)
        ds.attrs.update(time_coverage_start='20200908103000',
                        time_coverage_end='20200908123000')
        self._test_adds_time_dim(ds)

    def test_illegal_time_coverage(self):
        ds = new_test_dataset(day=None)
        ds.attrs.update(time_coverage_start='yesterday',
                        time_coverage_end='20200908123000')
        self._test_raises(ds, 'Cannot parse timestamp from "yesterday".')

    def test_missing_time_coverage(self):
        ds = new_test_dataset(day=None)
        self._test_raises(ds, 'Missing time_coverage_start and/or time_coverage_end in dataset attributes.')

        ds = new_test_dataset(day=None)
        ds.attrs.update(start_time='2020-09-08T10:30:00Z',
                        end_time='2020-09-08T12:30:00Z')
        self._test_raises(ds, 'Missing time_coverage_start and/or time_coverage_end in dataset attributes.')

    def test_illegal_concat_dim(self):
        ds = new_test_dataset(day=None)
        self._test_raises(ds, 'Missing (coordinate) variable "t" for dimension "t".',
                          input_concat_dim='t')

    def _test_adds_time_dim(self, ds: xr.Dataset):
        self.assertNotIn('time', ds)
        pre_processor = DatasetPreProcessor(input_variables=None, input_concat_dim='time')
        new_ds = pre_processor.preprocess_dataset(ds)
        self.assertIsInstance(new_ds, xr.Dataset)
        self.assertAllInDataset(['r_ui16', 'r_ui16', 'r_i32', 'lon', 'lat', 'time', 'time_bnds'], new_ds)
        self.assertEqual(1, len(new_ds.time))
        self.assertEqual(np.array(['2020-09-08T11:30:00'], dtype='datetime64[ns]'),
                         np.array(new_ds.time[0], dtype='datetime64[ns]'))
        self.assertEqual({'bounds': 'time_bnds'}, new_ds.time.attrs)
        self.assertEqual(1, len(new_ds.time_bnds))
        self.assertEqual(np.array(['2020-09-08T10:30:00'], dtype='datetime64[ns]'),
                         np.array(new_ds.time_bnds[0][0], dtype='datetime64[ns]'))
        self.assertEqual(np.array(['2020-09-08T12:30:00'], dtype='datetime64[ns]'),
                         np.array(new_ds.time_bnds[0][1], dtype='datetime64[ns]'))

    def _test_raises(self, ds, expected_message: str, input_concat_dim='time'):
        pre_processor = DatasetPreProcessor(input_variables=None, input_concat_dim=input_concat_dim)
        with self.assertRaises(ConverterError) as cm:
            pre_processor.preprocess_dataset(ds)
        self.assertEqual(expected_message, f'{cm.exception}')

    def assertAllInDataset(self, var_names: List[str], ds: xr.Dataset):
        for var_name in var_names:
            self.assertIn(var_name, ds)

    def assertNoneInDataset(self, var_names: List[str], ds: xr.Dataset):
        for var_name in var_names:
            self.assertNotIn(var_name, ds)


class CustomPreprocessorTest(unittest.TestCase):
    @classmethod
    def my_preprocessor(cls, ds: xr.Dataset) -> xr.Dataset:
        return ds.swap_dims({"sounding_dim": "time"})

    @classmethod
    def new_input_dataset(cls, /, offset, size):
        return xr.Dataset(dict(time=xr.DataArray(np.arange(offset, offset + size),
                                                 dims='sounding_dim'),
                               pressure=xr.DataArray(np.random.random(size * 20).reshape((size, 20)),
                                                     dims=['sounding_dim', 'levels_dim'])))

    def test_swap_dims(self):
        pre_processor = DatasetPreProcessor(input_variables=None,
                                            input_custom_preprocessor="tests.test_preprocessor:"
                                                                      "CustomPreprocessorTest.my_preprocessor",
                                            input_concat_dim='time')
        ds = self.new_input_dataset(0, size=100)
        self.assertEqual({'sounding_dim': 100, 'levels_dim': 20}, ds.dims)
        self.assertEqual(('sounding_dim',), ds.time.dims)
        self.assertEqual(('sounding_dim', 'levels_dim'), ds.pressure.dims)

        ds = pre_processor.preprocess_dataset(ds)
        self.assertEqual({'time': 100, 'levels_dim': 20}, ds.dims)
        self.assertEqual(('time',), ds.time.dims)
        self.assertEqual(('time', 'levels_dim'), ds.pressure.dims)

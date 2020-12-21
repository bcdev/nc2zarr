import unittest
from typing import List

import numpy as np
import xarray as xr

from nc2zarr.preprocessor import DatasetPreProcessor
from tests.helpers import new_test_dataset


class DatasetPreProcessorTest(unittest.TestCase):

    def test_filters_vars(self):
        ds = new_test_dataset(day=1)
        self.assertIn('time', ds)
        pre_processor = DatasetPreProcessor(input_variables=['r_i32', 'lon', 'lat', 'time'],
                                            input_concat_dim='time',
                                            verbosity=2)
        new_ds = pre_processor.preprocess_dataset(ds)
        self.assertIsInstance(new_ds, xr.Dataset)
        self.assertAllInDataset(['r_i32', 'lon', 'lat', 'time'], new_ds)
        self.assertNoneInDataset(['r_ui16', 'r_f32'], new_ds)

    def test_leaves_time_coord_untouched(self):
        ds = new_test_dataset(day=1)
        self.assertIn('time', ds)
        pre_processor = DatasetPreProcessor(input_variables=None,
                                            input_concat_dim='time')
        new_ds = pre_processor.preprocess_dataset(ds)
        self.assertIsInstance(new_ds, xr.Dataset)
        self.assertAllInDataset(['r_ui16', 'r_ui16', 'r_i32', 'lon', 'lat', 'time'], new_ds)
        self.assertIn('time', new_ds)
        self.assertEqual(ds.time, new_ds.time)

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

    def assertAllInDataset(self, var_names: List[str], ds: xr.Dataset):
        for var_name in var_names:
            self.assertIn(var_name, ds)

    def assertNoneInDataset(self, var_names: List[str], ds: xr.Dataset):
        for var_name in var_names:
            self.assertNotIn(var_name, ds)

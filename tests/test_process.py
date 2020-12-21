import unittest

import xarray as xr

from nc2zarr.processor import DatasetProcessor
from tests.helpers import new_test_dataset


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
        self.assertIn('r_f32', ds)
        processor = DatasetProcessor(process_rechunk={'*': dict(lon=8, lat=4, time=1)})
        new_ds, new_encoding = processor.process_dataset(ds)
        self.assertIs(ds, new_ds)
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
        self.assertIn('r_f32', ds)
        processor = DatasetProcessor(process_rechunk={'*': dict(lon=8, lat=4, time=1),
                                                      'lon': None,
                                                      'lat': None,
                                                      'time': 100})
        new_ds, new_encoding = processor.process_dataset(ds)
        self.assertIs(ds, new_ds)
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
        self.assertIn('r_f32', ds)
        processor = DatasetProcessor(process_rechunk=
        {
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
        self.assertIs(ds, new_ds)
        self.assertEqual({'r_f32': {'chunks': (1, 18, 36)},
                          'r_i32': {'chunks': (1, 18, 36)},
                          'r_ui16': {'chunks': (1, 18, 36)},
                          'lon': {'chunks': (36,)},
                          'lat': {'chunks': (18,)},
                          'time': {'chunks': (128,)}},
                         new_encoding)

    def test_rechunk_and_encodings_merged(self):
        ds = new_test_dataset(day=1)
        self.assertIn('r_f32', ds)
        processor = DatasetProcessor(process_rechunk={'r_i32': dict(lon=8, lat=8),
                                                      'lon': None,
                                                      'lat': None},
                                     output_encoding={'r_i32': dict(compressor=None, fill_value=None)})
        new_ds, new_encoding = processor.process_dataset(ds)
        self.assertIs(ds, new_ds)
        self.assertEqual({
            'r_f32': {'chunks': (1, 18, 36)},
            'r_i32': {'chunks': (1, 8, 8), 'compressor': None, 'fill_value': None},
            'r_ui16': {'chunks': (1, 18, 36)},
            'lon': {'chunks': (36,)},
            'lat': {'chunks': (18,)},
            'time': {'chunks': (1,)},
        }, new_encoding)

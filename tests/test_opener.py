import os.path
import unittest
from typing import Sequence, Union, Type

import xarray as xr

from nc2zarr.error import ConverterError
from nc2zarr.opener import DatasetOpener
from tests.helpers import IOCollector


class DatasetOpenerTest(unittest.TestCase):
    io_collector = IOCollector()

    @classmethod
    def setUpClass(cls):
        cls.io_collector.reset_paths()
        cls.io_collector.add_inputs('inputs', day_offset=1, num_days=3)

    @classmethod
    def tearDownClass(cls):
        cls.io_collector.delete_paths()

    @classmethod
    def _preprocess_dataset(cls, ds: xr.Dataset) -> xr.Dataset:
        return ds.assign_attrs(marker=True)

    def test_open_datasets_no_inputs(self):
        opener = DatasetOpener(input_paths='imports/*.nc')
        with self.assertRaises(ConverterError) as cm:
            list(opener.open_datasets())
        self.assertEqual('At least one input file must be given.', f'{cm.exception}')

    def test_open_datasets(self):
        opener = DatasetOpener(input_paths='inputs/*.nc', verbosity=1)

        result = list(opener.open_datasets())
        self.assertEqual(3, len(result))
        for i in range(3):
            self.assertIsInstance(result[i], xr.Dataset)
            self.assertIn('time', result[i])
            self.assertEqual(1, len(result[i].time))
            self.assertNotIn('marker', result[i].attrs)

        result = list(opener.open_datasets(preprocess=self._preprocess_dataset))
        self.assertEqual(3, len(result))
        for i in range(3):
            self.assertIsInstance(result[i], xr.Dataset)
            self.assertIn('time', result[i])
            self.assertEqual(1, len(result[i].time))
            self.assertIn('marker', result[i].attrs)

    def test_open_datasets_mf(self):
        opener = DatasetOpener(input_paths='inputs/*.nc', input_multi_file=True)

        result = list(opener.open_datasets())
        self.assertEqual(1, len(result))
        self.assertIsInstance(result[0], xr.Dataset)
        self.assertIn('time', result[0])
        self.assertEqual(3, len(result[0].time))
        self.assertNotIn('marker', result[0].attrs)

        result = list(opener.open_datasets(preprocess=self._preprocess_dataset))
        self.assertEqual(1, len(result))
        self.assertIsInstance(result[0], xr.Dataset)
        self.assertIn('time', result[0])
        self.assertEqual(3, len(result[0].time))
        self.assertIn('marker', result[0].attrs)


class ResolveInputPathsTest(unittest.TestCase):
    io_collector = IOCollector()

    @classmethod
    def setUpClass(cls):
        cls.io_collector.reset_paths()
        cls.io_collector.add_inputs('inputs/set1', day_offset=1, num_days=3)
        cls.io_collector.add_inputs('inputs/set2', day_offset=1, num_days=3)
        cls.io_collector.add_inputs('inputs/set3', day_offset=1, num_days=3)

    @classmethod
    def tearDownClass(cls):
        cls.io_collector.delete_paths()

    def test_illegal_sort_by(self):
        with self.assertRaises(ConverterError) as cm:
            DatasetOpener.resolve_input_paths('inputs/**/*.nc', sort_by='date')
        self.assertEqual('Can sort by "path" or "name" only, got "date".', f'{cm.exception}')

    def test_nothing_found(self):
        resolved_paths = DatasetOpener.resolve_input_paths('outputs/**/*.nc')
        self.assertEqual([], resolved_paths)

    def test_no_inputs(self):
        resolved_paths = DatasetOpener.resolve_input_paths([])
        self.assertEqual([], resolved_paths)

    def test_unsorted(self):
        resolved_paths = DatasetOpener.resolve_input_paths('inputs/**/*.nc')
        self._assert_unsorted(resolved_paths)

        resolved_paths = DatasetOpener.resolve_input_paths(
            [
                'inputs/set2/*.nc',
                'inputs/set3/*.nc',
                'inputs/set1/*.nc',
                'inputs/set3/*.nc',  # Doubled
            ],
            sort_by='name'
        )
        self._assert_unsorted(resolved_paths)

    def _assert_unsorted(self, actual_paths):
        self.assertEqual(
            norm_paths(['inputs/set1/input-01.nc',
                        'inputs/set1/input-02.nc',
                        'inputs/set1/input-03.nc',
                        'inputs/set2/input-01.nc',
                        'inputs/set2/input-02.nc',
                        'inputs/set2/input-03.nc',
                        'inputs/set3/input-01.nc',
                        'inputs/set3/input-02.nc',
                        'inputs/set3/input-03.nc'], c=set),
            norm_paths(actual_paths, c=set))

    def test_sort_by_name(self):
        resolved_paths = DatasetOpener.resolve_input_paths('inputs/**/*.nc', sort_by='name')
        self._assert_sort_by_name(resolved_paths)

        resolved_paths = DatasetOpener.resolve_input_paths(
            [
                'inputs/set2/*.nc',
                'inputs/set3/*.nc',
                'inputs/set1/*.nc',
                'inputs/set3/*.nc',  # Doubled
            ],
            sort_by='name'
        )
        self._assert_sort_by_name(resolved_paths)

    def _assert_sort_by_name(self, actual_paths):
        self.assertEqual(9, len(actual_paths))
        self.assertEqual(
            norm_paths(['inputs/set1/input-01.nc', 'inputs/set2/input-01.nc', 'inputs/set3/input-01.nc'], c=set),
            norm_paths(actual_paths[0:3], c=set))
        self.assertEqual(
            norm_paths(['inputs/set1/input-02.nc', 'inputs/set2/input-02.nc', 'inputs/set3/input-02.nc'], c=set),
            norm_paths(actual_paths[3:6], c=set))
        self.assertEqual(
            norm_paths(['inputs/set1/input-03.nc', 'inputs/set2/input-03.nc', 'inputs/set3/input-03.nc'], c=set),
            norm_paths(actual_paths[6:9], c=set))

    def test_sort_by_path(self):
        resolved_paths = DatasetOpener.resolve_input_paths('inputs/**/*.nc', sort_by='path')
        self._assert_sort_by_path(resolved_paths)

        resolved_paths = DatasetOpener.resolve_input_paths(
            [
                'inputs/set2/*.nc',
                'inputs/set3/*.nc',
                'inputs/set1/*.nc',
                'inputs/set3/*.nc',  # Doubled
            ],
            sort_by='path'
        )
        self._assert_sort_by_path(resolved_paths)

    def _assert_sort_by_path(self, actual_paths):
        self.assertEqual(
            norm_paths([
                'inputs/set1/input-01.nc',
                'inputs/set1/input-02.nc',
                'inputs/set1/input-03.nc',
                'inputs/set2/input-01.nc',
                'inputs/set2/input-02.nc',
                'inputs/set2/input-03.nc',
                'inputs/set3/input-01.nc',
                'inputs/set3/input-02.nc',
                'inputs/set3/input-03.nc'
            ]),
            norm_paths(actual_paths))


def norm_paths(paths: Sequence[str], c: Union[Type[set], Type[list]] = list) -> Union[set, list]:
    return c(map(lambda p: p.replace('/', os.path.sep), paths))

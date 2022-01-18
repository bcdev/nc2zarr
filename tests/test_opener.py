# The MIT License (MIT)
# Copyright (c) 2022 by Brockmann Consult GmbH and contributors
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

    def test_open_datasets_wildcard_does_not_resolve(self):
        opener = DatasetOpener(input_paths='imports/*.nc')
        with self.assertRaises(ConverterError) as cm:
            list(opener.open_datasets())
        self.assertEqual('No inputs found for wildcard: "imports/*.nc"',
                         f'{cm.exception}')

    def test_open_datasets_not_given(self):
        opener = DatasetOpener(input_paths=[])
        with self.assertRaises(ConverterError) as cm:
            list(opener.open_datasets())
        self.assertEqual('No inputs given.',
                         f'{cm.exception}')

    def test_open_datasets_not_found(self):
        opener = DatasetOpener(input_paths='imports/pippo.nc')
        with self.assertRaises(ConverterError) as cm:
            list(opener.open_datasets())
        self.assertEqual('Input not found: "imports/pippo.nc"',
                         f'{cm.exception}')

    def test_open_datasets(self):
        opener = DatasetOpener(input_paths='inputs/*.nc')

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

    def test_open_datasets_prefetch_chunks(self):

        opener = DatasetOpener(input_paths='inputs/*.nc', input_prefetch_chunks=True)
        result = list(opener.open_datasets(preprocess=self._preprocess_dataset))
        self.assertEqual(3, len(result))
        for i in range(3):
            self.assertIsInstance(result[i], xr.Dataset)
            self.assertIn('r_f32', result[i])
            var = result[i]['r_f32']
            self.assertEqual(((1,), (9, 9), (9, 9, 9, 9)), var.chunks)

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

    def test_open_datasets_mf_prefetch_chunks(self):
        opener = DatasetOpener(input_paths='inputs/*.nc',
                               input_multi_file=True,
                               input_prefetch_chunks=True)

        result = list(opener.open_datasets())
        self.assertEqual(1, len(result))
        self.assertIsInstance(result[0], xr.Dataset)

        self.assertIn('r_f32', result[0])
        var = result[0]['r_f32']
        self.assertEqual(((1, 1, 1), (9, 9), (9, 9, 9, 9)), var.chunks)


class ResolveInputPathsTest(unittest.TestCase):
    io_collector = IOCollector()

    @classmethod
    def setUpClass(cls):
        cls.io_collector.reset_paths()

        cls.io_collector.add_inputs('inputs/set1', day_offset=1, num_days=3, format='nc')
        cls.io_collector.add_inputs('inputs/set2', day_offset=1, num_days=3, format='nc')
        cls.io_collector.add_inputs('inputs/set3', day_offset=1, num_days=3, format='nc')

        cls.io_collector.add_inputs('inputs/set1', day_offset=1, num_days=3, format='zarr')
        cls.io_collector.add_inputs('inputs/set2', day_offset=1, num_days=3, format='zarr')
        cls.io_collector.add_inputs('inputs/set3', day_offset=1, num_days=3, format='zarr')

    @classmethod
    def tearDownClass(cls):
        cls.io_collector.delete_paths()

    def test_illegal_sort_by(self):
        with self.assertRaises(ConverterError) as cm:
            DatasetOpener.resolve_input_paths('inputs/**/*.nc', sort_by='date')
        self.assertEqual('Can sort by "path" or "name" only, got "date".',
                         f'{cm.exception}')

    def test_nothing_found(self):
        with self.assertRaises(ConverterError) as cm:
            DatasetOpener.resolve_input_paths('outputs/**/*.nc')
        self.assertEqual('No inputs found for wildcard: "outputs/**/*.nc"',
                         f'{cm.exception}')

    def test_nothing_given(self):
        resolved_paths = DatasetOpener.resolve_input_paths([])
        self.assertEqual([], resolved_paths)

    def test_expands_user(self):
        resolved_paths = DatasetOpener.resolve_input_paths(['~'])
        self.assertEqual([os.path.expanduser('~')], resolved_paths)

    def test_unsorted_nc(self):
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

    def test_unsorted_zarr(self):
        resolved_paths = DatasetOpener.resolve_input_paths('inputs/**/*.zarr')
        self._assert_unsorted(resolved_paths, ext='zarr')

        resolved_paths = DatasetOpener.resolve_input_paths(
            [
                'inputs/set2/*.zarr',
                'inputs/set3/*.zarr',
                'inputs/set1/*.zarr',
                'inputs/set3/*.zarr',  # Doubled
            ],
            sort_by='name'
        )
        self._assert_unsorted(resolved_paths, ext='zarr')

    def _assert_unsorted(self, actual_paths, ext='nc'):
        self.assertEqual(
            norm_paths([f'inputs/set1/input-01.{ext}',
                        f'inputs/set1/input-02.{ext}',
                        f'inputs/set1/input-03.{ext}',
                        f'inputs/set2/input-01.{ext}',
                        f'inputs/set2/input-02.{ext}',
                        f'inputs/set2/input-03.{ext}',
                        f'inputs/set3/input-01.{ext}',
                        f'inputs/set3/input-02.{ext}',
                        f'inputs/set3/input-03.{ext}'], c=set),
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

    def test_sort_by_name_which_is_dir(self):
        resolved_paths = DatasetOpener.resolve_input_paths('inputs/**/*.zarr/', sort_by='name')
        self._assert_sort_by_name(resolved_paths, ext='zarr/')

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

    def _assert_sort_by_name(self, actual_paths, ext='nc'):
        self.assertEqual(9, len(actual_paths))
        self.assertEqual(
            norm_paths([f'inputs/set1/input-01.{ext}',
                        f'inputs/set2/input-01.{ext}',
                        f'inputs/set3/input-01.{ext}'], c=set),
            norm_paths(actual_paths[0:3], c=set))
        self.assertEqual(
            norm_paths([f'inputs/set1/input-02.{ext}',
                        f'inputs/set2/input-02.{ext}',
                        f'inputs/set3/input-02.{ext}'], c=set),
            norm_paths(actual_paths[3:6], c=set))
        self.assertEqual(
            norm_paths([f'inputs/set1/input-03.{ext}',
                        f'inputs/set2/input-03.{ext}',
                        f'inputs/set3/input-03.{ext}'], c=set),
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

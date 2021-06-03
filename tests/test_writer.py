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
import uuid

import numpy as np
import xarray as xr
import zarr.errors

from nc2zarr.writer import DatasetWriter
from tests.helpers import IOCollector
from tests.helpers import new_test_dataset


def my_postprocessor(ds: xr.Dataset) -> xr.Dataset:
    return ds.assign(crs=xr.DataArray(42))


class DatasetWriterTest(unittest.TestCase, IOCollector):
    def setUp(self):
        self.reset_paths()

    def tearDown(self):
        self.delete_paths()

    def test_no_output_path(self):
        with self.assertRaises(ValueError) as cm:
            DatasetWriter('')
        self.assertEqual('output_path must be given',
                         f'{cm.exception}')

    def test_append_and_postprocessor(self):
        with self.assertRaises(ValueError) as cm:
            DatasetWriter('my.zarr',
                          output_append=True,
                          output_custom_postprocessor='tests.test_writer:my_postprocessor')
        self.assertEqual('output_append and output_custom_postprocessor cannot be given both',
                         f'{cm.exception}')

    def test_local_dry_run(self):
        self.add_path('my.zarr')
        writer = DatasetWriter('my.zarr', dry_run=True)
        ds = new_test_dataset(day=1)
        writer.write_dataset(ds)
        self.assertFalse(os.path.isdir('out.zarr'))

    def test_local_dry_run_for_existing(self):
        self.add_path('my.zarr')
        ds = new_test_dataset(day=1)
        writer = DatasetWriter('my.zarr', output_overwrite=True)
        writer.write_dataset(ds)
        self.assertTrue(os.path.isdir('my.zarr'))
        writer = DatasetWriter('my.zarr', output_overwrite=True, dry_run=True)
        writer.write_dataset(ds)
        self.assertTrue(os.path.isdir('my.zarr'))

    def test_local(self):
        self.add_path('my.zarr')
        writer = DatasetWriter('my.zarr', output_overwrite=False)
        ds = new_test_dataset(day=1)
        writer.write_dataset(ds)
        self.assertTrue(os.path.isdir('my.zarr'))

        ds = new_test_dataset(day=2)
        with self.assertRaises(zarr.errors.ContainsGroupError):
            writer.write_dataset(ds)

    def test_expands_user(self):
        writer = DatasetWriter('~/my.zarr')
        self.assertEqual(os.path.expanduser('~/my.zarr'), writer._output_path)

    def test_local_overwrite(self):
        self.add_path('my.zarr')
        writer = DatasetWriter('my.zarr', output_overwrite=False)
        ds = new_test_dataset(day=1)
        writer.write_dataset(ds)
        self.assertTrue(os.path.isdir('my.zarr'))

        writer = DatasetWriter('my.zarr', output_overwrite=True)
        ds = new_test_dataset(day=2)
        writer.write_dataset(ds)
        self.assertTrue(os.path.isdir('my.zarr'))

    def test_local_postprocessor(self):
        self.add_path('my.zarr')
        writer = DatasetWriter('my.zarr',
                               output_overwrite=False,
                               output_custom_postprocessor='tests.test_writer:my_postprocessor')
        ds = new_test_dataset(day=1)
        self.assertNotIn('crs', ds)

        writer.write_dataset(ds)
        self.assertTrue(os.path.isdir('my.zarr'))
        with xr.open_zarr('my.zarr') as ds:
            self.assertIn('crs', ds)

    # noinspection PyMethodMayBeStatic
    def test_object_storage_params(self):
        # Not a real test but test coverage will increase a bit.
        DatasetWriter('mybucket/my.zarr',
                      output_s3_kwargs=dict(
                          key='mykey',
                          secret='mysecret',
                          client_kwargs=dict(
                              endpoint_url='http://bibo.s3.com'
                          )))

    def test_aws_s3_with_unknown_bucket(self):
        ds = new_test_dataset(day=1)
        writer = DatasetWriter(f's3://my{uuid.uuid4()}/my.zarr')
        with self.assertRaises(Exception):
            # We know this will raise, but our test coverage increases a little bit.
            writer.write_dataset(ds)

    def test_append_with_input_decode_cf(self):

        src_path_pat = 'src_{}.zarr'
        dst_path = 'my.zarr'
        self.add_path(dst_path)

        writer = DatasetWriter(dst_path, output_overwrite=False, input_decode_cf=False)

        n = 3
        for i in range(0, n):
            src_dataset = new_test_dataset(day=i + 1)
            src_path = src_path_pat.format(i)
            self.add_path(src_path)
            src_dataset.to_zarr(src_path)
            with xr.open_zarr(src_path, decode_cf=False) as src_dataset:
                writer.write_dataset(src_dataset, append=i > 0)

        self._assert_time_slices_ok(dst_path, src_path_pat, n)

    # see also https://github.com/pydata/xarray/issues/4412
    def test_append_with_input_decode_cf_xarray(self):

        src_path_pat = 'src_{}.zarr'
        dst_path = 'my.zarr'
        self.add_path(dst_path)

        n = 3
        for i in range(0, n):
            src_dataset = new_test_dataset(day=i + 1)
            src_path = src_path_pat.format(i)
            self.add_path(src_path)
            src_dataset.to_zarr(src_path)
            with xr.open_zarr(src_path, decode_cf=False) as src_dataset:
                if i == 0:
                    src_dataset.to_zarr(dst_path, mode='w-')
                else:
                    # Hack:
                    src_dataset = xr.decode_cf(src_dataset)
                    for var_name in src_dataset.variables:
                        src_dataset[var_name].encoding = {}
                        src_dataset[var_name].attrs = {}
                    src_dataset.to_zarr(dst_path, append_dim='time')

        self._assert_time_slices_ok(dst_path, src_path_pat, n)

    def test_appending_vars_that_lack_append_dim(self):

        src_path_pat = 'src_{}.zarr'
        dst_path = 'my.zarr'
        self.add_path(dst_path)

        writer = DatasetWriter(dst_path, output_overwrite=False, input_decode_cf=False)

        n = 3
        for i in range(0, n):
            field_names_values = np.full((3, 50), 0, dtype='S')
            field_names_values[0, 0] = np.array('A')
            field_names_values[1, 0] = np.array('B')
            field_names_values[2, 0] = np.array('C')

            src_dataset = new_test_dataset(day=i + 1)
            src_dataset = src_dataset.assign(
                field_names=xr.DataArray(field_names_values,
                                         dims=("fields", "field_name_length"))
            )
            src_path = src_path_pat.format(i)
            self.add_path(src_path)
            src_dataset.to_zarr(src_path)
            with xr.open_zarr(src_path, decode_cf=False) as src_dataset:
                writer.write_dataset(src_dataset, append=i > 0)

        self._assert_time_slices_ok(dst_path, src_path_pat, n)

    @classmethod
    def _assert_time_slices_ok(cls, dst_path, src_path_pat, n):
        with xr.open_zarr(dst_path, decode_cf=False) as ds:
            for i in range(0, n):
                src_path = src_path_pat.format(i)
                with xr.open_zarr(src_path, decode_cf=False) as src_dataset:
                    dst_dataset_slice = ds.isel(time=slice(i, i + 1)).compute()
                    src_dataset.load()
                    print(f'comparing time step {i} or {dst_path} with {src_path}')
                    xr.testing.assert_allclose(dst_dataset_slice, src_dataset)

    # TODO: add real s3 tests using moto for boto mocking

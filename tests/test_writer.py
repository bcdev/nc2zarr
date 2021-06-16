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

import json
import os.path
import unittest
import uuid

import numpy as np
import pytest
import xarray as xr
import zarr.errors

from nc2zarr.writer import AppendMode
from nc2zarr.writer import DatasetWriter
from tests.helpers import IOCollector
from tests.helpers import new_append_test_datasets
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
                          output_custom_postprocessor=
                          'tests.test_writer:my_postprocessor')
        self.assertEqual('output_append and output_custom_postprocessor '
                         'cannot both be given',
                         f'{cm.exception}')

    def test_local_dry_run(self):
        self.add_path('my.zarr')
        writer = DatasetWriter('my.zarr', dry_run=True)
        ds = new_test_dataset(day=1)
        writer.write_dataset(ds)
        self.assertFalse(os.path.isdir('out.zarr'))

    def test_finalize_adjusts_metadata(self):
        self.add_path('my.zarr')
        writer = DatasetWriter('my.zarr',
                               output_append=True,
                               output_adjust_metadata=True,
                               input_paths=['a.nc', 'z.zarr', 'b.nc'])
        for i in range(3):
            ds = new_test_dataset(day=i + 1)
            writer.write_dataset(ds)
        with xr.open_zarr('my.zarr') as ds:
            self.assertNotIn('history', ds.attrs)
            self.assertNotIn('source', ds.attrs)
            self.assertNotIn('time_coverage_start', ds.attrs)
            self.assertNotIn('time_coverage_end', ds.attrs)
        writer.finalize_dataset()
        with xr.open_zarr('my.zarr') as ds:
            self.assertIn('history', ds.attrs)
            self.assertIn('source', ds.attrs)
            self.assertEqual('a.nc, b.nc', ds.attrs['source'])
            self.assertIn('time_coverage_start', ds.attrs)
            self.assertEqual('2020-12-01 10:00:00', ds.attrs['time_coverage_start'])
            self.assertIn('time_coverage_end', ds.attrs)
            self.assertEqual('2020-12-03 10:00:00', ds.attrs['time_coverage_end'])

    def test_finalize_adjusts_metadata_with_time_bnds(self):
        self.add_path('my.zarr')
        writer = DatasetWriter('my.zarr', output_append=True, output_adjust_metadata=True)
        for i in range(3):
            ds = new_test_dataset(day=i + 1, add_time_bnds=True)
            writer.write_dataset(ds)
        writer.finalize_dataset()
        with xr.open_zarr('my.zarr') as ds:
            self.assertIn('time_coverage_start', ds.attrs)
            self.assertEqual('2020-12-01 09:30:00', ds.attrs['time_coverage_start'])
            self.assertIn('time_coverage_end', ds.attrs)
            self.assertEqual('2020-12-03 10:30:00', ds.attrs['time_coverage_end'])

    def test_finalize_updates_metadata(self):
        self.add_path('my.zarr')
        writer = DatasetWriter('my.zarr',
                               output_append=True,
                               output_metadata=dict(comment='This dataset is a test.'))
        for i in range(3):
            ds = new_test_dataset(day=i + 1)
            writer.write_dataset(ds)
        with xr.open_zarr('my.zarr') as ds:
            self.assertNotIn('comment', ds.attrs)
        writer.finalize_dataset()
        with xr.open_zarr('my.zarr') as ds:
            self.assertIn('comment', ds.attrs)
            self.assertEqual('This dataset is a test.', ds.attrs['comment'])

    def test_finalize_only_and_append(self):
        self.add_path('my.zarr')
        writer = DatasetWriter('my.zarr',
                               finalize_only=True,
                               output_append=True)

        ds = new_test_dataset(day=1)
        with self.assertRaises(RuntimeError) as e:
            writer.write_dataset(ds)
        self.assertEqual(('internal error: cannot write/append'
                          ' datasets when in finalize-only mode',),
                         e.exception.args)

    def test_finalize_only_and_no_output(self):
        self.add_path('my.zarr')
        writer = DatasetWriter('my.zarr',
                               finalize_only=True,
                               output_append=True,
                               output_metadata=dict(comment='This dataset is a test.'))

        with self.assertRaises(FileNotFoundError) as e:
            writer.finalize_dataset()
        self.assertEqual(('output path not found: my.zarr',),
                         e.exception.args)

    def test_finalize_only_and_consolidate_if_specified(self):
        self.add_path('my.zarr')
        ds = new_test_dataset(day=1)
        writer = DatasetWriter('my.zarr',
                               output_overwrite=True)
        writer.write_dataset(ds)
        writer.finalize_dataset()
        self.assertTrue(os.path.isdir('my.zarr'))
        self.assertFalse(os.path.isfile('my.zarr/.zmetadata'))
        writer = DatasetWriter('my.zarr',
                               output_consolidated=True,
                               finalize_only=True)
        writer.finalize_dataset()
        self.assertTrue(os.path.isdir('my.zarr'))
        self.assertTrue(os.path.isfile('my.zarr/.zmetadata'))
        with open('my.zarr/.zmetadata') as fp:
            metadata = json.load(fp)
        self.assertIn('metadata', metadata)
        self.assertEqual({},
                         metadata['metadata'].get('.zattrs'))

    def test_finalize_only_and_consolidate_if_not_specified(self):
        self.add_path('my.zarr')
        ds = new_test_dataset(day=1)
        writer = DatasetWriter('my.zarr',
                               output_consolidated=True,
                               output_overwrite=True)
        writer.write_dataset(ds)
        writer.finalize_dataset()
        self.assertTrue(os.path.isdir('my.zarr'))
        self.assertTrue(os.path.isfile('my.zarr/.zmetadata'))
        writer = DatasetWriter('my.zarr',
                               output_consolidated=False,
                               output_metadata=dict(comment='This dataset is a test.'),
                               finalize_only=True)
        writer.finalize_dataset()
        self.assertTrue(os.path.isdir('my.zarr'))
        self.assertTrue(os.path.isfile('my.zarr/.zmetadata'))
        with open('my.zarr/.zmetadata') as fp:
            metadata = json.load(fp)
        self.assertIn('metadata', metadata)
        self.assertEqual({'comment': 'This dataset is a test.'},
                         metadata['metadata'].get('.zattrs'))

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

        writer = DatasetWriter(dst_path, output_overwrite=False,
                               input_decode_cf=False)

        n = 3
        for i in range(0, n):
            src_dataset = new_test_dataset(day=i + 1)
            src_path = src_path_pat.format(i)
            self.add_path(src_path)
            src_dataset.to_zarr(src_path)
            with xr.open_zarr(src_path, decode_cf=False) as src_dataset:
                writer.write_dataset(src_dataset, append=i > 0)

        self.assertTimeSlicesOk(dst_path, src_path_pat, n)

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

        self.assertTimeSlicesOk(dst_path, src_path_pat, n)

    def test_appending_vars_that_lack_append_dim(self):

        src_path_pat = 'src_{}.zarr'
        dst_path = 'my.zarr'
        self.add_path(dst_path)

        writer = DatasetWriter(dst_path, output_overwrite=False,
                               input_decode_cf=False)

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

        self.assertTimeSlicesOk(dst_path, src_path_pat, n)

    def test_append_to_non_increasing_append_mode_all(self):
        dst_path = "my.zarr"
        self.add_path(dst_path)
        ds1, ds2 = new_append_test_datasets(
            ["2001-01-01", "2001-01-03", "2001-01-02"],
            ["2001-01-04", "2001-01-05", "2001-01-06"]
        )
        ds1.to_zarr(dst_path)
        w = DatasetWriter(dst_path, output_append=True, output_append_dim="t",
                          output_append_mode=AppendMode.all)
        w.write_dataset(ds2)

    def test_append_to_non_increasing_forbid_overlap(self):
        dst_path = "my.zarr"
        self.add_path(dst_path)
        ds1, ds2 = new_append_test_datasets(
            ["2001-01-01", "2001-01-03", "2001-01-02"],
            ["2001-01-04", "2001-01-05", "2001-01-06"]
        )
        ds1.to_zarr(dst_path)
        with pytest.raises(ValueError,
                           match="must be increasing"):
            w = DatasetWriter(dst_path, output_append=True,
                              output_append_dim="t",
                              output_append_mode=AppendMode.no_overlap)
            w.write_dataset(ds2)

    def test_append_overlapping_forbid_overlap(self):
        dst_path = "my.zarr"
        self.add_path(dst_path)
        ds1, ds2 = new_append_test_datasets(
            ["2001-01-01", "2001-01-02", "2001-01-03"],
            ["2001-01-02", "2001-01-03", "2001-01-04"]
        )
        ds1.to_zarr(dst_path)
        with pytest.raises(ValueError,
                           match="may not overlap"):
            w = DatasetWriter(dst_path, output_append=True,
                              output_append_dim="t",
                              output_append_mode=AppendMode.no_overlap)
            w.write_dataset(ds2)

    def test_append_overlapping_append_newer(self):
        dst_path = "my.zarr"
        self.add_path(dst_path)
        ds1, ds2 = new_append_test_datasets(
            ["2001-01-01", "2001-01-02", "2001-01-03"],
            ["2001-01-02", "2001-01-03", "2001-01-04", "2001-02-05"]
        )
        ds1.to_zarr(dst_path)
        w = DatasetWriter(dst_path, output_append=True,
                          output_append_dim="t",
                          output_append_mode=AppendMode.newer)
        w.write_dataset(ds2)
        ds3 = xr.open_zarr(dst_path)
        expected = np.array(["2001-01-01", "2001-01-02", "2001-01-03",
                             "2001-01-04", "2001-02-05"],
                            dtype="datetime64[ns]")
        np.testing.assert_equal(expected, ds3.t.data)

    def test_append_non_increasing_append_newer(self):
        dst_path = "my.zarr"
        self.add_path(dst_path)
        ds1, ds2 = new_append_test_datasets(
            ["2001-01-01", "2001-01-02", "2001-01-03"],
            ["2001-01-05", "2001-01-04", "2001-01-03", "2001-02-02"]
        )
        ds1.to_zarr(dst_path)
        w = DatasetWriter(dst_path, output_append=True,
                          output_append_dim="t",
                          output_append_mode=AppendMode.newer)
        with pytest.raises(ValueError,
                           match="must be increasing"):
            w.write_dataset(ds2)

    def test_append_overlapping_replace(self):
        dst_path = "my.zarr"
        self.add_path(dst_path)
        ds1, ds2 = new_append_test_datasets(
            ["2001-01-01", "2001-01-02", "2001-01-03", "2001-01-05"],
            ["2001-01-02", "2001-01-03", "2001-01-04", "2001-01-06"]
        )
        ds1.to_zarr(dst_path)
        w = DatasetWriter(dst_path, output_append=True,
                          output_append_dim="t",
                          output_append_mode=AppendMode.replace)
        w.write_dataset(ds2)
        ds3 = xr.open_zarr(dst_path)
        np.testing.assert_equal(
            np.array(["2001-01-01", "2001-01-02", "2001-01-03",
                      "2001-01-04", "2001-01-05", "2001-01-06"],
                     dtype="datetime64[ns]"), ds3.t.data)
        np.testing.assert_equal(
            np.array([0, 1, 1, 1, 0, 1]),
            ds3.v.isel(x=0, y=0)
        )

    def test_append_overlapping_retain(self):
        dst_path = "my.zarr"
        self.add_path(dst_path)
        ds1, ds2 = new_append_test_datasets(
            ["2001-01-01", "2001-01-02", "2001-01-03", "2001-01-05"],
            ["2001-01-03", "2001-01-04", "2001-01-05", "2001-01-06"]
        )
        ds1.to_zarr(dst_path)
        w = DatasetWriter(dst_path, output_append=True,
                          output_append_dim="t",
                          output_append_mode=AppendMode.retain)
        w.write_dataset(ds2)
        ds3 = xr.open_zarr(dst_path)
        np.testing.assert_equal(
            np.array(["2001-01-01", "2001-01-02", "2001-01-03",
                      "2001-01-04", "2001-01-05", "2001-01-06"],
                     dtype="datetime64[ns]"), ds3.t.data)
        np.testing.assert_equal(
            np.array([0, 0, 0, 1, 0, 1]),
            ds3.v.isel(x=0, y=0)
        )

    @classmethod
    def assertTimeSlicesOk(cls, dst_path, src_path_pat, n):
        with xr.open_zarr(dst_path, decode_cf=False) as ds:
            for i in range(0, n):
                src_path = src_path_pat.format(i)
                with xr.open_zarr(src_path, decode_cf=False) as src_dataset:
                    dst_dataset_slice = ds.isel(time=slice(i, i + 1)).compute()
                    src_dataset.load()
                    print(f'comparing time step {i} or {dst_path} with {src_path}')
                    xr.testing.assert_allclose(dst_dataset_slice, src_dataset)

    # TODO: add real s3 tests using moto for boto mocking

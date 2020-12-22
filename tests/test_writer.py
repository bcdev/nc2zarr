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

import zarr.errors

from nc2zarr.writer import DatasetWriter
from tests.helpers import IOCollector
from tests.helpers import new_test_dataset
import botocore.exceptions

class DatasetWriterTest(unittest.TestCase, IOCollector):
    def setUp(self):
        self.reset_paths()

    def tearDown(self):
        self.delete_paths()

    def test_no_output_path(self):
        with self.assertRaises(ValueError) as cm:
            DatasetWriter('')
        self.assertEqual('output_path must be given', f'{cm.exception}')

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
        writer = DatasetWriter('my.zarr', output_overwrite=False, dry_run=False)
        ds = new_test_dataset(day=1)
        writer.write_dataset(ds)
        self.assertTrue(os.path.isdir('my.zarr'))

        ds = new_test_dataset(day=2)
        with self.assertRaises(zarr.errors.ContainsGroupError):
            writer.write_dataset(ds)

    def test_local_overwrite(self):
        self.add_path('my.zarr')
        writer = DatasetWriter('my.zarr', output_overwrite=True, dry_run=False)
        ds = new_test_dataset(day=1)
        writer.write_dataset(ds)
        self.assertTrue(os.path.isdir('my.zarr'))

        writer = DatasetWriter('my.zarr', output_overwrite=True, dry_run=False)
        ds = new_test_dataset(day=2)
        writer.write_dataset(ds)
        self.assertTrue(os.path.isdir('my.zarr'))

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

    # TODO: add real s3 tests using moto for boto mocking

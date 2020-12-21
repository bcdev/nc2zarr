import os.path
import unittest
import zarr.errors
from nc2zarr.writer import DatasetWriter
from tests.helpers import IOCollector
from tests.helpers import new_test_dataset


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

        ds = new_test_dataset(day=2)
        writer.write_dataset(ds)


    def test_s3(self):
        with self.assertRaises(FileNotFoundError):
            # We know this will fail, but we'll make sure it's the right exception
            DatasetWriter('mybucket/my.zarr',
                          output_s3_kwargs=dict(key='mykey', secret='mykey',
                                                client_kwargs=dict(endpoint_url='http://bibo.s3.com')))

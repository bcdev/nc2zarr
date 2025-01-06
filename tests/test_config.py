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

import yaml

from nc2zarr.config import load_config
from nc2zarr.error import ConverterError
from tests.helpers import IOCollector


class ConfigKwargsTest(unittest.TestCase):

    def test_defaults(self):
        self.assertEqual({}, load_config())

    def test_kwargs_to_config(self):
        self.assertEqual(
            {
                'dry_run': True,
                'verbosity': 2,
                'input': {
                    'paths': 'inputs/*.nc',
                    'multi_file': True,
                    'decode_cf': False,
                    'concat_dim': 'time',
                },
                'process': {
                    'rename': {'lons': 'lon'}
                },
                'output': {
                    'path': 'my.zarr',
                    'overwrite': False,
                    'append': True,
                },
            },
            load_config(input_paths='inputs/*.nc',
                        input_decode_cf=False,
                        input_concat_dim='time',
                        input_multi_file=True,
                        process_rename=dict(lons='lon'),
                        output_path='my.zarr',
                        output_overwrite=False,
                        output_append=True,
                        dry_run=True,
                        verbosity=2))

    def test_kwargs_to_kwargs(self):
        self.assertEqual(
            {
                'input_paths': 'inputs/*.nc',
                'input_multi_file': True,
                'input_decode_cf': False,
                'input_concat_dim': 'time',
                'process_rename': {'lons': 'lon'},
                'output_path': 'my.zarr',
                'output_overwrite': False,
                'output_append': True,
                'dry_run': True,
                'verbosity': 2,
            },
            load_config(return_kwargs=True,
                        input_paths='inputs/*.nc',
                        input_decode_cf=False,
                        input_concat_dim='time',
                        input_multi_file=True,
                        process_rename=dict(lons='lon'),
                        output_path='my.zarr',
                        output_overwrite=False,
                        output_append=True,
                        dry_run=True,
                        verbosity=2))


class ConfigFileTest(unittest.TestCase, IOCollector):
    config_1 = {
        'verbosity': 2,
        'input': {
            'paths': ['inputs/2009/*.nc'],
            'decode_cf': True,
            'concat_dim': 'time',
        },
        'output': {
            's3': {
                'key': 'mykey',
                'secret': 'mysecret',
            },
        },
    }

    config_2 = {
        'process': {
            'rename': {
                'longitude': 'lon',
                'latitude': 'lat',
            },
        },
        'output': {
            'path': 'mybucket/my.zarr'
        },
    }

    config_3 = {
        'input': {
            'paths': ['inputs/2010/*.nc'],
            'decode_cf': False,
        },
        'output': {
            'append': True
        },
    }

    def setUp(self):
        self.reset_paths()

        config_paths = [f'config_{i + 1}.yml' for i in range(3)]
        for config_path, config in zip(config_paths, (self.config_1, self.config_2, self.config_3)):
            self.add_path(config_path)
            with open(config_path, 'w') as fp:
                yaml.dump(config, fp)
        self.config_paths = config_paths

    def tearDown(self):
        self.delete_paths()

    def test_one_config_file_to_config(self):
        self.assertEqual(
            {
                'verbosity': 2,
                'input': {
                    'paths': ['inputs/2009/*.nc'],
                    'concat_dim': 'time',
                    'decode_cf': False,
                },
                'output': {
                    's3': {'key': 'mykey', 'secret': 'mysecret'},
                },
            },
            load_config(config_paths=[self.config_paths[0]],
                        input_decode_cf=False,
                        verbosity=2))

    def test_one_config_file_to_kwargs(self):
        self.assertEqual(
            {
                'verbosity': 2,
                'input_paths': ['inputs/2009/*.nc'],
                'input_concat_dim': 'time',
                'input_decode_cf': False,
                'output_s3': {'key': 'mykey', 'secret': 'mysecret'},
            },
            load_config(config_paths=[self.config_paths[0]],
                        return_kwargs=True,
                        input_decode_cf=False,
                        verbosity=2))

    def test_many_config_files_to_config(self):
        self.assertEqual(
            {
                'dry_run': False,
                'verbosity': 1,
                'input': {
                    'paths': [
                        'inputs/2009/*.nc',
                        'inputs/2010/*.nc',
                        'inputs/2011/*.nc',
                    ],
                    'concat_dim': 'time',
                    'decode_cf': False,
                },
                'process': {
                    'rename': {
                        'longitude': 'lon',
                        'latitude': 'lat',
                    },
                },
                'output': {
                    'append': True,
                    'path': 'mybucket/my.zarr',
                    's3': {'key': 'mykey', 'secret': 'mysecret'},
                },
            },
            load_config(config_paths=self.config_paths,
                        input_paths=['inputs/2011/*.nc'],
                        dry_run=False,
                        verbosity=1))

    def test_not_found(self):
        with self.assertRaises(ConverterError) as cm:
            load_config(config_paths=['bibo.yml'])
        self.assertEqual('Configuration not found: bibo.yml', f'{cm.exception}')

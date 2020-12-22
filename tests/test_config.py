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

import unittest

import yaml

from nc2zarr.config import load_config
from nc2zarr.error import ConverterError
from tests.helpers import IOCollector


class LoadConfigTest(unittest.TestCase, IOCollector):

    def setUp(self):
        self.reset_paths()

    def tearDown(self):
        self.delete_paths()

    def test_defaults(self):
        self.assertEqual({}, load_config())

    def test_without_config_file(self):
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

    def test_with_config_files(self):
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

        config_paths = [f'config_{i + 1}.yml' for i in range(3)]
        for config_path, config in zip(config_paths, (config_1, config_2, config_3)):
            self.add_path(config_path)
            with open(config_path, 'w') as fp:
                yaml.dump(config, fp)

        self.assertEqual(
            {
                'dry_run': False,
                'verbosity': 1,
                'input': {
                    'paths': [
                        'inputs/2009/*.nc',
                        'inputs/2010/*.nc',
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
            load_config(config_paths=config_paths,
                        dry_run=False,
                        verbosity=1))

    def test_not_found(self):
        with self.assertRaises(ConverterError) as cm:
            load_config(config_paths=['bibo.yml'])
        self.assertEqual('Configuration not found: bibo.yml', f'{cm.exception}')

# The MIT License (MIT)
# Copyright (c) 2020 by the ESA CCI Toolbox development team and contributors
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

import os
import shutil
from typing import Sequence, Union, Type

import s3fs
import xarray as xr
import yaml

S3_KEYWORDS = 'anon', 'key', 'secret', 'token'
S3_CLIENT_KEYWORDS = 'endpoint_url', 'region_name'


# noinspection PyUnusedLocal
def convert_netcdf_to_zarr(input_paths: Union[str, Sequence[str]] = None,
                           output_path: str = None,
                           config_path: str = None,
                           verbose: bool = False,
                           exception_type: Type[Exception] = ValueError):
    """
    Convert NetCDF files to Zarr format.

    :param input_paths:
    :param output_path:
    :param config_path:
    :param verbose:
    :param exception_type:
    """
    input_paths = input_paths or []
    for input_file in input_paths:
        print(f'Reading "{input_file}"...')

    config = {}
    if config_path is not None:
        with open(config_path) as fp:
            config = yaml.load(fp)

    input_config = config.get('input', {})
    output_config = config.get('output', {})

    input_paths = input_config.get('paths', input_paths)
    output_path = output_config.get('path', output_path)
    output_encoding = output_config.get('encoding')
    output_consolidated = output_config.get('consolidated', False)
    output_overwrite = output_config.get('overwrite', False)
    output_chunks = output_config.get('chunks')

    s3 = None
    s3_kwargs = {k: output_config[k] for k in S3_KEYWORDS if k in output_config}
    s3_client_kwargs = {k: output_config[k] for k in S3_CLIENT_KEYWORDS if k in output_config}
    if s3_kwargs or s3_client_kwargs:
        s3 = s3fs.S3FileSystem(**s3_kwargs, client_kwargs=s3_client_kwargs or None)

    if isinstance(input_paths, str):
        input_file = input_paths
        if '*' in input_paths:
            input_dataset = xr.open_mfdataset(input_paths, engine='netcdf4')
        else:
            input_dataset = xr.open_dataset(input_paths, engine='netcdf4')
    elif input_paths is not None and len(input_paths):
        input_dataset = xr.open_mfdataset(input_paths, engine='netcdf4')
    else:
        raise exception_type('input_paths must be given')

    if output_chunks:
        output_dataset = input_dataset.chunk(output_chunks)
    else:
        output_dataset = input_dataset.chunk(output_chunks)

    if s3 is not None:
        if output_overwrite and s3.isdir(output_path):
            s3.rm(output_path, recursive=True)
        output_path_or_store = s3fs.S3Map(output_path, s3=s3)
    else:
        if output_overwrite and os.path.isdir(output_path):
            shutil.rmtree(output_path, ignore_errors=False)
        output_path_or_store = output_path

    output_dataset.to_zarr(output_path_or_store,
                           mode='w',
                           encoding=output_encoding,
                           consolidated=output_consolidated)

    # Test by reopening the dataset from target location
    test_dataset = xr.open_zarr(output_path_or_store,
                                consolidated=output_consolidated)

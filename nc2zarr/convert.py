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

import glob
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
    config = {}
    if config_path is not None:
        with open(config_path) as fp:
            config = yaml.load(fp, Loader=yaml.SafeLoader)
            print(f'Configuration {config_path} loaded.')

    input_config = config.get('input', {})
    output_config = config.get('output', {})

    input_paths = input_config.get('paths', input_paths)
    input_variables = input_config.get('variables')
    input_concat_dim = input_config.get('concat_dim', 'time')
    input_engine = input_config.get('engine', 'netcdf4')
    output_path = output_config.get('path', output_path)
    output_encoding = output_config.get('encoding')
    output_consolidated = output_config.get('consolidated', False)
    output_overwrite = output_config.get('overwrite', False)
    output_chunks = output_config.get('chunks')
    output_s3_kwargs = {k: output_config[k]
                        for k in S3_KEYWORDS if k in output_config}
    output_s3_client_kwargs = {k: output_config[k]
                               for k in S3_CLIENT_KEYWORDS if k in output_config}

    input_files = []
    if isinstance(input_paths, str):
        input_files.extend(glob.glob(input_paths, recursive=True))
    elif input_paths is not None and len(input_paths):
        for input_path in input_paths:
            input_files.extend(glob.glob(input_path, recursive=True))

    if not input_files:
        raise exception_type('at least one input file must be given')

    # TODO: we may sort using the actual coordinates of
    #  input_concat_dim coordinate variable, use xcube code.
    input_files = sorted(input_files)

    def preprocess_input_dataset(input_dataset: xr.Dataset) -> xr.Dataset:
        input_file = input_dataset.encoding['source']
        print(f'Preprocessing {input_file}')
        if input_concat_dim not in input_dataset.dims:
            # TODO: try inserting time dimension from
            #  input_dataset.attrs or from input_file, use Cate code.
            raise exception_type(f'missing dimension "time" in dataset "{input_file}"')
        if input_variables:
            drop_variables = set(input_dataset.data_vars).difference(input_variables)
            input_dataset = input_dataset.drop_vars(drop_variables)
        return input_dataset

    output_dataset = xr.open_mfdataset(input_files,
                                       engine=input_engine,
                                       preprocess=preprocess_input_dataset,
                                       concat_dim=input_concat_dim)

    # TODO: update output_dataset.attrs to reflect actual extent
    #  of spatio-temporal coordinates, use xcube code.

    if output_chunks:
        output_dataset = output_dataset.chunk(output_chunks)

    if output_s3_kwargs or output_s3_client_kwargs:
        s3 = s3fs.S3FileSystem(**output_s3_kwargs,
                               client_kwargs=output_s3_client_kwargs or None)
        output_path_or_store = s3fs.S3Map(output_path, s3=s3, create=True)
    else:
        output_path_or_store = output_path

    output_dataset.to_zarr(output_path_or_store,
                           mode='w' if output_overwrite else 'w-',
                           encoding=output_encoding,
                           consolidated=output_consolidated)

    # Test by reopening the dataset from target location
    test_dataset = xr.open_zarr(output_path_or_store,
                                consolidated=output_consolidated)

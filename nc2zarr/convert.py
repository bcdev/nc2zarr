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

from .constants import DEFAULT_CONFIG_FILE
from .constants import DEFAULT_MODE
from .constants import DEFAULT_OUTPUT_FILE
from .constants import S3_CLIENT_KEYWORDS
from .constants import S3_KEYWORDS
from .logger import LOGGER
from .perf import measure_time
from .time import ensure_time_dim


# noinspection PyUnusedLocal
def convert_netcdf_to_zarr(input_paths: Union[str, Sequence[str]] = None,
                           output_path: str = None,
                           config_path: str = None,
                           mode: str = None,
                           verbose: bool = False,
                           exception_type: Type[Exception] = ValueError):
    """
    Convert NetCDF files to Zarr format.

    :param input_paths:
    :param output_path:
    :param config_path:
    :param mode: 'slices' or 'all'
    :param verbose:
    :param exception_type:
    """
    config = {}
    config_path = config_path or DEFAULT_CONFIG_FILE
    try:
        with open(config_path) as fp:
            config = yaml.load(fp, Loader=yaml.SafeLoader)
            LOGGER.info(f'Configuration {config_path} loaded.')
    except FileNotFoundError as e:
        if config_path != DEFAULT_CONFIG_FILE:
            raise exception_type(f'Configuration {config_path} not found')

    mode = mode or config.get('mode', DEFAULT_MODE)

    input_config = config.get('input', {})
    input_paths = input_paths or input_config.get('paths')
    input_variables = input_config.get('variables')
    input_concat_dim = input_config.get('concat_dim', 'time')
    input_engine = input_config.get('engine', 'netcdf4')

    process_config = config.get('process', {})
    process_rename = process_config.get('rename')
    process_rechunk = process_config.get('rechunk')

    output_config = config.get('output', {})
    output_path = output_path or output_config.get('path', DEFAULT_OUTPUT_FILE)
    output_encoding = output_config.get('encoding')
    output_consolidated = output_config.get('consolidated', False)
    output_overwrite = output_config.get('overwrite', False)
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

    if process_rechunk and input_concat_dim in process_rechunk and process_rechunk[input_concat_dim] > 1:
        batch_size = process_rechunk[input_concat_dim]
        num_batches = len(input_files) // batch_size
        # TODO: group and combine batches then exit
        # TODO: remove test code here...
        import subprocess
        import uuid
        job_id = str(uuid.uuid4())
        for batch_id in range(num_batches):
            batch_input_files = input_files[batch_id * num_batches: batch_id * (num_batches+1)]
            batch_output_path = f'{job_id}-{batch_id}.zarr'
            batch_exit_code = subprocess.call(['./nc2zarr',
                                               '-c', config_path,
                                               '-o', batch_output_path,
                                               *batch_input_files])
            if batch_exit_code != 0:
                raise exception_type(f'batch processing failed with exit code {batch_exit_code}')
        return

    if output_s3_kwargs or output_s3_client_kwargs:
        s3 = s3fs.S3FileSystem(**output_s3_kwargs,
                               client_kwargs=output_s3_client_kwargs or None)
        output_path_or_store = s3fs.S3Map(output_path, s3=s3)  # , create=True)
    else:
        output_path_or_store = output_path

    # TODO: we may sort using the actual coordinates of
    #  input_concat_dim coordinate variable, use xcube code.
    input_files = sorted(input_files)

    first_dataset_shown = False

    def preprocess_input(input_dataset: xr.Dataset) -> xr.Dataset:
        nonlocal first_dataset_shown
        input_file = input_dataset.encoding['source']
        with measure_time(f'Preprocessing {input_file}', verbose=verbose):
            input_dataset = ensure_time_dim(input_dataset)
            if input_variables:
                drop_variables = set(input_dataset.data_vars).difference(input_variables)
                input_dataset = input_dataset.drop_vars(drop_variables)
        if verbose and not first_dataset_shown:
            LOGGER.info(f'First input dataset:\n{input_dataset}')
            first_dataset_shown = True
        return input_dataset

    read_and_write = read_and_write_in_slices if mode == 'slices' else read_and_write_in_one_go
    read_and_write(input_files,
                   input_engine,
                   input_concat_dim,
                   preprocess_input,
                   process_rename,
                   process_rechunk,
                   output_path,
                   output_path_or_store,
                   output_overwrite,
                   output_consolidated,
                   output_encoding)

    # Test by reopening the dataset from target location
    # test_dataset = xr.open_zarr(output_path_or_store,
    #                             consolidated=output_consolidated)


def read_and_write_in_slices(input_files, input_engine, input_concat_dim, preprocess_input, process_rename,
                             process_rechunk, output_path, output_path_or_store, output_overwrite, output_consolidated,
                             output_encoding):
    n = len(input_files)
    for i in range(n):
        input_file = input_files[i]
        with measure_time(f'Opening slice {i + 1} of {n}: {input_file}'):
            input_dataset = xr.open_dataset(input_file, engine=input_engine)
        input_dataset = preprocess_input(input_dataset)
        if process_rename:
            input_dataset = input_dataset.rename(process_rename)
        if process_rechunk:
            input_dataset = input_dataset.chunk(process_rechunk)
        if i == 0:
            with measure_time(f'Writing first slice to {output_path}'):
                input_dataset.to_zarr(output_path_or_store,
                                      mode='w' if output_overwrite else 'w-',
                                      encoding=output_encoding,
                                      consolidated=output_consolidated)
        else:
            with measure_time(f'Appending slice {i + 1} of {n} to {output_path}'):
                input_dataset.to_zarr(output_path_or_store,
                                      append_dim=input_concat_dim,
                                      encoding=output_encoding,
                                      consolidated=output_consolidated)
        input_dataset.close()


def read_and_write_in_one_go(input_files, input_engine, input_concat_dim, preprocess_input, process_rename,
                             process_rechunk, output_path, output_path_or_store, output_overwrite, output_consolidated,
                             output_encoding):
    with measure_time(f'Opening {len(input_files)} file(s)'):
        output_dataset = xr.open_mfdataset(input_files,
                                           engine=input_engine,
                                           preprocess=preprocess_input,
                                           concat_dim=input_concat_dim)
    if process_rename:
        output_dataset = output_dataset.rename(process_rename)
    if process_rechunk:
        output_dataset = output_dataset.chunk(process_rechunk)
    with measure_time(f'Writing dataset to {output_path}'):
        output_dataset.to_zarr(output_path_or_store,
                               mode='w' if output_overwrite else 'w-',
                               encoding=output_encoding,
                               consolidated=output_consolidated)
    output_dataset.close()

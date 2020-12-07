import glob
import os.path
import shutil
from typing import Sequence, Union, Type

import s3fs
import xarray as xr
import yaml

from .batch import run_batch_mode
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
                           batch_size: int = None,
                           mode: str = None,
                           verbose: bool = False,
                           exception_type: Type[Exception] = ValueError):
    """
    Convert NetCDF files to Zarr format.

    :param input_paths:
    :param output_path:
    :param config_path:
    :param mode: 'slices' or 'one_go'
    :param verbose:
    :param exception_type:
    """
    config = {}
    try:
        effective_config_path = config_path or DEFAULT_CONFIG_FILE
        with open(effective_config_path) as fp:
            config = yaml.load(fp, Loader=yaml.SafeLoader)
            LOGGER.info(f'Configuration {effective_config_path} loaded.')
    except FileNotFoundError as e:
        if config_path is not None:
            raise exception_type(f'Configuration {config_path} not found')

    mode = mode or config.get('mode', DEFAULT_MODE)

    input_config = config.get('input', {})
    input_paths = input_paths or input_config.get('paths')
    input_variables = input_config.get('variables')
    input_concat_dim = input_config.get('concat_dim', 'time')
    input_engine = input_config.get('engine', 'netcdf4')
    input_batch_size = batch_size or input_config.get('batch_size')

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

    input_files = get_input_files(input_paths)
    if not input_files:
        raise exception_type('at least one input file must be given')

    if batch_size:
        run_batch_mode(input_files, batch_size, config_path, exception_type=exception_type)
        return

    if output_s3_kwargs or output_s3_client_kwargs:
        s3 = s3fs.S3FileSystem(**output_s3_kwargs,
                               client_kwargs=output_s3_client_kwargs or None)
        if output_overwrite and s3.isdir(output_path):
            with measure_time(f'Removing existing {output_path}'):
                s3.rm(output_path, recursive=True)
        output_path_or_store = s3fs.S3Map(output_path, s3=s3)  # , create=True)
    else:
        if output_overwrite and os.path.isdir(output_path):
            with measure_time(f'Removing existing {output_path}'):
                shutil.rmtree(output_path)
        output_path_or_store = output_path

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


def read_and_write_in_slices(input_files,
                             input_engine,
                             input_concat_dim,
                             preprocess_input,
                             process_rename,
                             process_rechunk,
                             output_path,
                             output_path_or_store,
                             output_overwrite,
                             output_consolidated,
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


def read_and_write_in_one_go(input_files,
                             input_engine,
                             input_concat_dim,
                             preprocess_input,
                             process_rename,
                             process_rechunk,
                             output_path,
                             output_path_or_store,
                             output_overwrite,
                             output_consolidated,
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


def get_input_files(input_paths: Sequence[str]) -> Sequence[str]:
    input_files = []
    if isinstance(input_paths, str):
        input_files.extend(glob.glob(input_paths, recursive=True))
    elif input_paths is not None and len(input_paths):
        for input_path in input_paths:
            input_files.extend(glob.glob(input_path, recursive=True))

    # TODO: we may sort using the actual coordinates of
    #  input_concat_dim coordinate variable, use xcube code.
    return sorted(input_files)

import glob
import os.path
import shutil
from typing import Sequence, Union, Type, Any, Tuple, Dict, List, Optional

import s3fs
import xarray as xr
import yaml

from .append import ensure_append_dim
from .constants import DEFAULT_MODE
from .constants import DEFAULT_OUTPUT_FILE
from .constants import S3_KEYWORDS
from .logger import LOGGER
from .perf import measure_time


# noinspection PyUnusedLocal
def convert_netcdf_to_zarr(input_paths: Union[str, Sequence[str]] = None,
                           output_path: str = None,
                           config_paths: List[str] = None,
                           batch_size: int = None,
                           mode: str = None,
                           decode_cf: bool = False,
                           dry_run: bool = False,
                           verbose: bool = False,
                           exception_type: Type[Exception] = ValueError):
    """
    Convert NetCDF files to Zarr format.

    :param input_paths:
    :param output_path:
    :param config_paths:
    :param batch_size:
    :param mode: 'slices' or 'one_go'
    :param decode_cf:
    :param dry_run:
    :param verbose:
    :param exception_type:
    """

    arg_config = dict(input=dict(), process=dict(), output=dict())
    if mode is not None:
        arg_config['mode'] = mode
    if dry_run:
        arg_config['dry_run'] = True
    if decode_cf:
        arg_config['input']['decode_cf'] = True
    if batch_size is not None:
        arg_config['input']['batch_size'] = batch_size
    if input_paths:
        arg_config['input']['paths'] = input_paths
    if output_path:
        arg_config['output']['path'] = output_path

    configs = [_load_config(config_path, exception_type)
               for config_path in config_paths] + [arg_config]

    effective_request = _merge_configs(configs)

    _convert_netcdf_to_zarr(effective_request, verbose, exception_type)


def _convert_netcdf_to_zarr(effective_request: Dict[str, Any],
                            verbose: bool,
                            exception_type: Type[Exception]):
    mode = effective_request.get('mode', DEFAULT_MODE)

    dry_run = effective_request.get('dry_run', False)
    if dry_run:
        LOGGER.warn('Dry run!')

    input_config = effective_request.get('input', {})
    input_paths = input_config.get('paths')
    input_variables = input_config.get('variables')
    input_append_dim = input_config.get('append_dim', 'time')
    input_engine = input_config.get('engine', 'netcdf4')
    input_decode_cf = input_config.get('decode_cf', False)
    input_sort_by = input_config.get('sort_by', 'path')
    input_batch_size = input_config.get('batch_size')
    if input_batch_size is not None:
        raise NotImplementedError('batch processing not supported yet')

    process_config = effective_request.get('process', {})
    process_rename = process_config.get('rename')
    process_rechunk = process_config.get('rechunk')

    output_config = effective_request.get('output', {})
    output_path = output_config.get('path', DEFAULT_OUTPUT_FILE)
    output_encoding = output_config.get('encoding')
    output_consolidated = output_config.get('consolidated', False)
    output_overwrite = output_config.get('overwrite', False)
    output_s3_kwargs = {k: output_config[k]
                        for k in S3_KEYWORDS if k in output_config}

    input_files = _get_input_files(input_paths, input_sort_by, exception_type)
    if not input_files:
        raise exception_type('at least one input file must be given')
    LOGGER.info(f'{len(input_files)} input file(s) given.')
    if verbose:
        LOGGER.info('Input file(s):\n'
                    + ('\n'.join(map(lambda f: f'  {f[0]}: ' + f[1],
                                     zip(range(len(input_files)), input_files)))))

    if output_s3_kwargs:
        s3 = s3fs.S3FileSystem(**output_s3_kwargs)
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
            input_dataset = ensure_append_dim(input_dataset, input_append_dim)
            if input_variables:
                drop_variables = set(input_dataset.variables).difference(input_variables)
                input_dataset = input_dataset.drop_vars(drop_variables)
        if verbose and not first_dataset_shown:
            LOGGER.info(f'First input dataset:\n{input_dataset}')
            first_dataset_shown = True
        return input_dataset

    read_and_write = _read_and_write_in_slices if mode == 'slices' else _read_and_write_in_one_go
    read_and_write(input_files,
                   input_engine,
                   input_append_dim,
                   input_decode_cf,
                   preprocess_input,
                   process_rename,
                   process_rechunk,
                   output_path,
                   output_path_or_store,
                   output_overwrite,
                   output_consolidated,
                   output_encoding,
                   dry_run)

    # Test by reopening the dataset from target location
    # test_dataset = xr.open_zarr(output_path_or_store,
    #                             consolidated=output_consolidated)


def _read_and_write_in_slices(input_files,
                              input_engine,
                              input_append_dim,
                              input_decode_cf,
                              preprocess_input,
                              process_rename,
                              process_rechunk,
                              output_path,
                              output_path_or_store,
                              output_overwrite,
                              output_consolidated,
                              output_encoding,
                              dry_run):
    n = len(input_files)
    for i in range(n):
        input_file = input_files[i]
        with measure_time(f'Opening slice {i + 1} of {n}: {input_file}'):
            input_dataset = xr.open_dataset(input_file,
                                            engine=input_engine,
                                            decode_cf=input_decode_cf)
        input_dataset = preprocess_input(input_dataset)
        output_dataset, output_encoding = _process_dataset(input_dataset,
                                                           process_rechunk,
                                                           process_rename,
                                                           output_encoding,
                                                           i > 0 and not input_decode_cf)

        if i == 0:
            with measure_time(f'Writing first slice to {output_path}'):
                if not dry_run:
                    output_dataset.to_zarr(output_path_or_store,
                                           mode='w' if output_overwrite else 'w-',
                                           encoding=output_encoding,
                                           consolidated=output_consolidated)
                else:
                    LOGGER.warn('Writing disabled, dry run!')
        else:
            with measure_time(f'Appending slice {i + 1} of {n} to {output_path}'):
                if not dry_run:
                    output_dataset.to_zarr(output_path_or_store,
                                           append_dim=input_append_dim,
                                           consolidated=output_consolidated)
                else:
                    LOGGER.warn('Writing disabled, dry run!')

        input_dataset.close()


def _read_and_write_in_one_go(input_files,
                              input_engine,
                              input_append_dim,
                              input_decode_cf,
                              preprocess_input,
                              process_rename,
                              process_rechunk,
                              output_path,
                              output_path_or_store,
                              output_overwrite,
                              output_consolidated,
                              output_encoding,
                              dry_run):
    with measure_time(f'Opening {len(input_files)} file(s)'):
        output_dataset = xr.open_mfdataset(input_files,
                                           engine=input_engine,
                                           preprocess=preprocess_input,
                                           concat_dim=input_append_dim,
                                           decode_cf=input_decode_cf)
    output_dataset, output_encoding = _process_dataset(output_dataset,
                                                       process_rechunk,
                                                       process_rename,
                                                       output_encoding)
    with measure_time(f'Writing dataset to {output_path}'):
        if not dry_run:
            output_dataset.to_zarr(output_path_or_store,
                                   mode='w' if output_overwrite else 'w-',
                                   encoding=output_encoding,
                                   consolidated=output_consolidated)
        else:
            LOGGER.warn('Writing disabled, dry run!')
    output_dataset.close()


def _get_input_files(input_paths: List[str],
                     sort_by: Optional[str],
                     exception_type) -> List[str]:
    input_files = []
    if isinstance(input_paths, str):
        input_files.extend(glob.glob(input_paths, recursive=True))
    elif input_paths is not None and len(input_paths):
        for input_path in input_paths:
            input_files.extend(glob.glob(input_path, recursive=True))

    if sort_by:
        # Get rid of doubles and sort
        input_files = set(input_files)
        if sort_by == 'path' or sort_by is True:
            return sorted(input_files)
        if sort_by == 'name':
            return sorted(input_files, key=os.path.basename)
        raise exception_type(f'Cannot sort by "{sort_by}".')
    else:
        # Get rid of doubles, but preserve order
        seen_input_files = set()
        unique_input_files = []
        for input_file in input_files:
            if input_file not in seen_input_files:
                unique_input_files.append(input_file)
                seen_input_files.add(input_file)
        return unique_input_files


def _process_dataset(ds: xr.Dataset,
                     process_rechunk: Dict[str, int] = None,
                     process_rename: Dict[str, str] = None,
                     output_encoding: Dict[str, Dict[str, Any]] = None,
                     prepare_for_append: bool = False) \
        -> Tuple[xr.Dataset, Dict[str, Dict[str, Any]]]:
    if process_rename:
        ds = ds.rename(process_rename)
    # fill_value_encoding = dict()
    if process_rechunk:
        chunk_encoding = _get_chunk_encodings(ds, process_rechunk)
    else:
        chunk_encoding = dict()
    if prepare_for_append:
        # This will only take place in "slice" mode.
        # For all slices except the first we must remove encoding attributes e.g. "_FillValue" .
        ds = _remove_variable_attrs(ds)
    return ds, _merge_encodings(ds,
                                chunk_encoding,
                                output_encoding or {})


def _remove_variable_attrs(ds: xr.Dataset) -> xr.Dataset:
    ds = ds.copy()
    for k, v in ds.variables.items():
        v.attrs = dict()
    return ds


def _get_chunk_encodings(ds: xr.Dataset,
                         process_rechunk: Dict[str, int]) \
        -> Dict[str, Dict[str, Any]]:
    output_encoding = dict()
    for k, v in ds.variables.items():
        var_name = str(k)
        chunks = []
        for dim_index in range(len(v.dims)):
            dim_name = v.dims[dim_index]
            if dim_name in process_rechunk:
                chunks.append(process_rechunk[dim_name])
            else:
                chunks.append(v.chunks[dim_index]
                              if v.chunks is not None else v.sizes[dim_name])
        output_encoding[var_name] = dict(chunks=tuple(chunks))
    return output_encoding


def _merge_encodings(ds: xr.Dataset,
                     *encodings: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    output_encoding = dict()
    for encoding in encodings:
        for k, v in ds.variables.items():
            var_name = str(k)
            if var_name in encoding:
                if var_name not in output_encoding:
                    output_encoding[var_name] = dict(encoding[var_name])
                else:
                    output_encoding[var_name].update(encoding[var_name])
    return output_encoding


def _load_config(path: str, exception_type) -> Dict[str, Any]:
    try:
        with open(path) as fp:
            config = yaml.load(fp, Loader=yaml.SafeLoader)
            LOGGER.info(f'Configuration {path} loaded.')
        return config
    except FileNotFoundError as e:
        raise exception_type(f'{path} not found.') from e


def _merge_configs(configs: List[Dict[str, Any]]) -> Dict[str, Any]:
    effective_request = dict()
    for config in configs:
        effective_request.update(**config)
    for config in configs:
        for k in ('input', 'process', 'output'):
            if k in config:
                if k not in effective_request:
                    effective_request[k] = dict()
                effective_request[k].update(**config[k])
    return effective_request

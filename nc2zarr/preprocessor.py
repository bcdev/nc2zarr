import re
from datetime import datetime
from typing import List
from typing import Tuple, Optional

import pandas as pd
import xarray as xr

from .log import LOGGER


class DatasetPreProcessor:
    def __init__(self,
                 input_variables: List[str] = None,
                 input_concat_dim: str = None,
                 verbosity: int = None,
                 ):
        self._input_variables = input_variables
        self._input_concat_dim = input_concat_dim
        self._verbosity = verbosity
        self._first_dataset_shown = False

    def preprocess_dataset(self, ds: xr.Dataset) -> xr.Dataset:
        if self._input_variables:
            drop_variables = set(ds.variables).difference(self._input_variables)
            ds = ds.drop_vars(drop_variables)
        if self._input_concat_dim:
            ds = ensure_dataset_has_concat_dim(ds, self._input_concat_dim)
        if self._verbosity and not self._first_dataset_shown:
            LOGGER.info(f'First input dataset:\n{ds}')
            self._first_dataset_shown = True
        return ds


_RE_TO_DATETIME_FORMATS = [(re.compile(14 * '\\d'), '%Y%m%d%H%M%S'),
                           (re.compile(12 * '\\d'), '%Y%m%d%H%M'),
                           (re.compile(8 * '\\d'), '%Y%m%d'),
                           (re.compile(6 * '\\d'), '%Y%m'),
                           (re.compile(4 * '\\d'), '%Y')]


def ensure_dataset_has_concat_dim(ds: xr.Dataset, concat_dim_name: str) -> xr.Dataset:
    """
    :param ds: Dataset to adjust
    :param concat_dim_name: Name of dimension to be appended
    :return: Adjusted dataset
    """
    concat_dim_var = None
    if concat_dim_name in ds:
        concat_dim_var = ds[concat_dim_name]

    if concat_dim_var is not None:
        if not concat_dim_var.dims:
            # if the concat_dim_var does not yet have a dimension, add it
            ds = ds.assign_coords({concat_dim_name: xr.DataArray(concat_dim_var, dims=(concat_dim_name,))})
    elif concat_dim_name == 'time':
        time_coverage_start, time_coverage_end = get_time_coverage_from_ds(ds)
        if not time_coverage_start and not time_coverage_end:
            # Can't do anything
            raise ValueError(f'cannot determine "{concat_dim_name}" coordinate')

        time_coverage_start = time_coverage_start or time_coverage_end
        time_coverage_end = time_coverage_end or time_coverage_start
        ds = ds.assign_coords(
            time=xr.DataArray([time_coverage_start + 0.5 * (time_coverage_end - time_coverage_start)],
                              dims=('time',),
                              attrs=dict(bounds='time_bnds')),
            time_bnds=xr.DataArray([[time_coverage_start, time_coverage_end]],
                                   dims=('time', 'bnds'))
        )
        concat_dim_var = ds.time
    else:
        # Can't do anything
        raise ValueError(f'cannot determine "{concat_dim_name}" coordinate')

    is_concat_dim_used = any((concat_dim_name in ds[var_name].dims) for var_name in ds.data_vars)
    if not is_concat_dim_used:
        concat_dim_bnds_name = concat_dim_var.attrs.get('bounds', f'{concat_dim_name}_bnds')
        concat_dim_bnds_var = ds[concat_dim_bnds_name] if concat_dim_bnds_name in ds else None

        # ds.expand_dims() will raise if coordinates exist, so remove them temporarily
        if concat_dim_bnds_var is not None:
            ds = ds.drop_vars([concat_dim_name, concat_dim_bnds_name])
        else:
            ds = ds.drop_vars(concat_dim_name)

        # if concat_dim_name is still a dimension, drop it too
        if concat_dim_name in ds.dims:
            ds = ds.drop_dims(concat_dim_name)

        # expand dataset by concat_dim_name/concat_dim_var, this will add the dimension and the coordinate
        ds = ds.expand_dims({concat_dim_name: concat_dim_var})
        # also (re)assign bounds coordinates
        if concat_dim_bnds_var is not None:
            ds = ds.assign_coords(time_bnds=concat_dim_bnds_var)

    # TODO: update output_dataset.attrs to reflect actual extent
    #  of spatio-temporal coordinates, use xcube code.

    return ds


def get_time_coverage_from_ds(ds: xr.Dataset) -> Tuple[datetime, datetime]:
    time_coverage_start = ds.attrs.get('time_coverage_start')
    if time_coverage_start is not None:
        time_coverage_start = get_timestamp_from_string(time_coverage_start)

    time_coverage_end = ds.attrs.get('time_coverage_end')
    if time_coverage_end is not None:
        time_coverage_end = get_timestamp_from_string(time_coverage_end)

    if time_coverage_start or time_coverage_end:
        return time_coverage_start, time_coverage_end

    filename = ds.encoding.get('source', '').split('/')[-1]
    return get_timestamps_from_string(filename)


def find_datetime_format(filename: str) -> Tuple[Optional[str], int, int]:
    for regex, time_format in _RE_TO_DATETIME_FORMATS:
        searcher = regex.search(filename)
        if searcher:
            p1, p2 = searcher.span()
            return time_format, p1, p2
    return None, -1, -1


def get_timestamp_from_string(string: str) -> Optional[datetime]:
    time_format, p1, p2 = find_datetime_format(string)
    if not time_format:
        return None
    try:
        return pd.to_datetime(string[p1:p2], format=time_format)
    except ValueError:
        return None


def get_timestamps_from_string(string: str) -> Tuple[datetime, datetime]:
    first_time = None
    second_time = None
    time_format, p1, p2 = find_datetime_format(string)
    try:
        if time_format:
            first_time = pd.to_datetime(string[p1:p2], format=time_format)
        string_rest = string[p2:]
        time_format, p1, p2 = find_datetime_format(string_rest)
        if time_format:
            second_time = pd.to_datetime(string_rest[p1:p2], format=time_format)
    except ValueError:
        pass
    return first_time, second_time

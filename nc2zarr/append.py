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

import re
from datetime import datetime
from typing import Tuple, Optional

import pandas as pd
import xarray as xr

_RE_TO_DATETIME_FORMATS = [(re.compile(14 * '\\d'), '%Y%m%d%H%M%S'),
                           (re.compile(12 * '\\d'), '%Y%m%d%H%M'),
                           (re.compile(8 * '\\d'), '%Y%m%d'),
                           (re.compile(6 * '\\d'), '%Y%m'),
                           (re.compile(4 * '\\d'), '%Y')]


def ensure_append_dim(ds: xr.Dataset, append_dim_name: str) -> xr.Dataset:
    """
    :param ds: Dataset to adjust
    :param append_dim_name: Name of dimension to be appended
    :return: Adjusted dataset
    """
    append_dim_var = None
    if append_dim_name in ds:
        append_dim_var = ds[append_dim_name]

    if append_dim_var is not None:
        if not append_dim_var.dims:
            # if the append_dim_var does not yet have a dimension, add it
            ds = ds.assign_coords({append_dim_name: xr.DataArray(append_dim_var, dims=(append_dim_name,))})
    elif append_dim_name == 'time':
        time_coverage_start, time_coverage_end = get_time_coverage_from_ds(ds)
        if not time_coverage_start and not time_coverage_end:
            # Can't do anything
            raise ValueError(f'cannot determine "{append_dim_name}" coordinate')

        time_coverage_start = time_coverage_start or time_coverage_end
        time_coverage_end = time_coverage_end or time_coverage_start
        ds = ds.assign_coords(
            time=xr.DataArray([time_coverage_start + 0.5 * (time_coverage_end - time_coverage_start)],
                              dims=('time',),
                              attrs=dict(bounds='time_bnds')),
            time_bnds=xr.DataArray([[time_coverage_start, time_coverage_end]],
                                   dims=('time', 'bnds'))
        )
        append_dim_var = ds.time
    else:
        # Can't do anything
        raise ValueError(f'cannot determine "{append_dim_name}" coordinate')

    is_append_dim_used = any((append_dim_name in ds[var_name].dims) for var_name in ds.data_vars)
    if not is_append_dim_used:
        append_dim_bnds_name = append_dim_var.attrs.get('bounds', f'{append_dim_name}_bnds')
        append_dim_bnds_var = ds[append_dim_bnds_name] if append_dim_bnds_name in ds else None

        # ds.expand_dims() will raise if coordinates exist, so remove them temporarily
        if append_dim_bnds_var is not None:
            ds = ds.drop_vars([append_dim_name, append_dim_bnds_name])
        else:
            ds = ds.drop_vars(append_dim_name)

        # if append_dim_name is still a dimension, drop it too
        if append_dim_name in ds.dims:
            ds = ds.drop_dims(append_dim_name)

        # expand dataset by append_dim_name/append_dim_var, this will add the dimension and the coordinate
        ds = ds.expand_dims({append_dim_name: append_dim_var})
        # also (re)assign bounds coordinates
        if append_dim_bnds_var is not None:
            ds = ds.assign_coords(time_bnds=append_dim_bnds_var)

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

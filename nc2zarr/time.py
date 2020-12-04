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
import warnings
from datetime import datetime
from typing import Tuple, Optional

import numpy as np
import pandas as pd
import xarray as xr

_RE_TO_DATETIME_FORMATS = [(re.compile(14 * '\\d'), '%Y%m%d%H%M%S'),
                           (re.compile(12 * '\\d'), '%Y%m%d%H%M'),
                           (re.compile(8 * '\\d'), '%Y%m%d'),
                           (re.compile(6 * '\\d'), '%Y%m'),
                           (re.compile(4 * '\\d'), '%Y')]


def ensure_time_dim(ds: xr.Dataset) -> xr.Dataset:
    """
    Add a time coordinate variable and their associated bounds coordinate variables
    if either temporal CF attributes ``time_coverage_start`` and ``time_coverage_end``
    are given or time information can be extracted from the file name but the time dimension is missing.

    In case the time information is given by a variable called 't' instead of 'time', it will be renamed into 'time'.

    The new time coordinate variable will be named ``time`` with dimension ['time'] and shape [1].
    The time bounds coordinates variable will be named ``time_bnds`` with dimensions ['time', 'bnds'] and shape [1,2].
    Both are of data type ``datetime64``.

    :param ds: Dataset to adjust
    :return: Adjusted dataset
    """
    time = None
    if 'time' in ds and isinstance(ds.time.values[0], (datetime, np.datetime64)):
        time = ds.time
    elif 't' in ds and isinstance(ds.t.values[0], (datetime, np.datetime64)):
        # if 't' in ds.t.dims:
        #     ds = ds.rename_dims({"t": "time"})
        ds = ds.rename_vars({"t": "time"})
        time = ds.time

    if time is not None:
        if 'time' not in ds.coords or not time.dims:
            ds = ds.assign_coords(time=xr.DataArray(ds.time, dims=('time',)))
    else:
        time_coverage_start, time_coverage_end = get_time_coverage_from_ds(ds)
        if not time_coverage_start and not time_coverage_end:
            # Can't do anything
            return ds

        time_coverage_start = time_coverage_start or time_coverage_end
        time_coverage_end = time_coverage_end or time_coverage_start
        ds = ds.assign_coords(
            time=xr.DataArray([time_coverage_start + 0.5 * (time_coverage_end - time_coverage_start)], dims=('time',)),
            time_bnds=xr.DataArray([[time_coverage_start, time_coverage_end]], dims=('time', 'bnds'))
        )

    is_time_used_as_dim = any(('time' in ds[var_name].dims) for var_name in ds.data_vars)
    if not is_time_used_as_dim:
        time = ds.time
        time_bnds = ds.time_bnds if 'time_bnds' in ds else None

        if time_bnds is not None:
            ds = ds.drop_vars(['time', 'time_bnds'])
        else:
            ds = ds.drop_vars('time')

        if 'time' in ds.dims:
            ds = ds.drop_dims('time')

        try:
            ds = ds.expand_dims(time=time)
        except BaseException as e:
            warnings.warn(f'failed adding time dimension: {e}')

        if time_bnds is not None:
            ds = ds.assign_coords(time_bnds=time_bnds)

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

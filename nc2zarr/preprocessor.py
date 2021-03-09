# The MIT License (MIT)
# Copyright (c) 2021 by Brockmann Consult GmbH and contributors
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

from datetime import datetime
from typing import List
from typing import Tuple, Optional

import pandas as pd
import xarray as xr

from .custom import load_custom_func
from .error import ConverterError
from .log import LOGGER


class DatasetPreProcessor:
    def __init__(self,
                 input_variables: List[str] = None,
                 input_custom_preprocessor: str = None,
                 input_concat_dim: str = None,
                 input_datetime_format: str = None):
        self._input_variables = input_variables
        self._input_custom_preprocessor = load_custom_func(input_custom_preprocessor) \
            if input_custom_preprocessor else None
        self._input_concat_dim = input_concat_dim
        self._input_datetime_format = input_datetime_format
        self._first_dataset_shown = False

    def preprocess_dataset(self, ds: xr.Dataset) -> xr.Dataset:
        if self._input_variables:
            drop_variables = set(ds.variables).difference(self._input_variables)
            ds = ds.drop_vars(drop_variables)
        if self._input_custom_preprocessor is not None:
            ds = self._input_custom_preprocessor(ds)
        if self._input_concat_dim:
            ds = ensure_dataset_has_concat_dim(ds, self._input_concat_dim,
                                               datetime_format=self._input_datetime_format)
        if not self._first_dataset_shown:
            LOGGER.debug(f'First input dataset:\n{ds}')
            self._first_dataset_shown = True
        return ds


def ensure_dataset_has_concat_dim(ds: xr.Dataset,
                                  concat_dim_name: str,
                                  datetime_format: str = None) -> xr.Dataset:
    """
    Ensure dataset *ds* has dimension *concat_dim*.

    :param ds: Dataset to adjust
    :param concat_dim_name: Name of dimension to be appended
    :param datetime_format: Name of dimension to be appended
    :return: Adjusted dataset
    """
    concat_dim_var = None
    if concat_dim_name in ds:
        concat_dim_var = ds[concat_dim_name]

    if concat_dim_var is not None:
        if not concat_dim_var.dims:
            # if the concat_dim_var does not yet have a dimension, add it
            ds = ds.assign_coords({
                concat_dim_name: xr.DataArray(concat_dim_var, dims=(concat_dim_name,))
            })
    elif concat_dim_name == 'time':
        time_coverage_start, time_coverage_end = \
            get_time_coverage_from_ds(ds, datetime_format=datetime_format)

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
        raise ConverterError(f'Missing (coordinate) variable "{concat_dim_name}" for dimension "{concat_dim_name}".')

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

    return ds


def get_time_coverage_from_ds(ds: xr.Dataset,
                              datetime_format: str = None) -> Tuple[datetime, datetime]:
    time_coverage_start = ds.attrs.get('time_coverage_start')
    if time_coverage_start is not None:
        time_coverage_start = parse_timestamp(time_coverage_start, datetime_format=datetime_format)

    time_coverage_end = ds.attrs.get('time_coverage_end')
    if time_coverage_end is not None:
        time_coverage_end = parse_timestamp(time_coverage_end, datetime_format=datetime_format)

    time_coverage_start = time_coverage_start or time_coverage_end
    time_coverage_end = time_coverage_end or time_coverage_start
    if time_coverage_start and time_coverage_end:
        return time_coverage_start, time_coverage_end

    # TODO: use special parameters to parse time_coverage_start, time_coverage_end from source_path
    # source_path = ds.encoding.get('source', '')
    raise ConverterError('Missing time_coverage_start and/or time_coverage_end in dataset attributes.')


def parse_timestamp(string: str, datetime_format: str = None) \
        -> Optional[datetime]:
    try:
        return pd.to_datetime(string, format=datetime_format)
    except ValueError as e:
        raise ConverterError(f'Cannot parse timestamp from "{string}".') from e

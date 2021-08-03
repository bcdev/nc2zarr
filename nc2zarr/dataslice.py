# The MIT License (MIT)
# Copyright (c) 2021 by Brockmann Consult GmbH and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.


import tempfile
from collections.abc import MutableMapping
from typing import Dict, Union, Tuple

import numpy as np
import xarray as xr
import zarr
from zarr.errors import GroupNotFoundError
from zarr.errors import PathNotFoundError


# TODO: Following the general nc2zarr software design, the functions in
#  dataslice module should instead be combined in a class DataSliceAppender
#  that implements the various append modes using the strategy OO pattern.
#  (See https://github.com/bcdev/nc2zarr/pull/44 )

DEFAULT_EPSILON = np.array(1000 * 1000, dtype='timedelta64[ns]')


def find_slice(store: Union[str, MutableMapping],
               target_value,
               dimension: str,
               epsilon=DEFAULT_EPSILON) \
        -> Tuple[int, str]:
    """
    Find index and update mode for *target_value* in Zarr dataset specified by
    *store*.

    :param store: A Zarr store.
    :param target_value: the position along the specified dimension at which
                         to insert or replace a new data slice. Must have the
                         same type as the dimension variable.
    :param dimension: the name of the dimension perpendicular to the new slice
    :param epsilon: epsilon for equality comparison. Must have the same type
                    as the dimension variable. Defaults to 1 millisecond
                    represented as a np.timedelta64.
    :return: A tuple (insert_index, 'insert') or (insert_index, 'replace') if an
             index was found, (-1, 'create') or (-1, 'append') otherwise.
    """
    try:
        cube = xr.open_dataset(store, engine="zarr")
    except (GroupNotFoundError, PathNotFoundError):
        # zarr directory does not exist
        return -1, 'create'

    # TODO (forman): optimise following naive search by bi-sectioning or so
    for i in range(cube[dimension].size):
        value = cube[dimension][i]
        if abs(target_value - value) < epsilon:
            return i, 'replace'
        if target_value < value:
            return i, 'insert'

    return -1, 'append'


def append_slice(store: Union[str, MutableMapping],
                 dataslice: xr.Dataset,
                 dimension: str = "time") -> None:
    """
    Append data slice to existing zarr dataset.

    :param store: A zarr store.
    :param dataslice: Data slice to insert
    :param dimension: name of dimension perpendicular to the slice
    """

    # Unfortunately slice.to_zarr(store, mode='a', append_dim='time') will
    # replace global attributes of store with attributes of slice (xarray
    # bug?), which are usually empty in our case. Hence, we must save our old
    # attributes in a copy of slice.
    ds = zarr.open_group(store, mode='r')
    dataslice = dataslice.copy()
    dataslice.attrs.update(ds.attrs)
    if 'coordinates' in dataslice.attrs:
        # Remove 'coordinates', otherwise we get ValueError: cannot serialize
        # coordinates because the global attribute 'coordinates' already
        # exists from next slice.to_zarr(...) call.
        dataslice.attrs.pop('coordinates')

    dataslice.to_zarr(store, mode='a', append_dim=dimension)


def update_slice(store: Union[str, MutableMapping],
                 insert_index: int,
                 dataslice: xr.Dataset,
                 mode: str,
                 dimension: str = "time") -> None:
    """
    Update existing Zarr dataset with new data slice.

    :param store: A Zarr store.
    :param insert_index: index at which to insert
    :param dataslice: slice to insert
    :param mode: Update mode, 'insert' or 'replace'
    :param dimension: name of dimension perpendicular to slice
    """

    if mode not in ('insert', 'replace'):
        raise ValueError(f'illegal mode value: {mode!r}')

    insert_mode = mode == 'insert'

    append_dim_var_names = []
    encoding = {}

    consolidated = True
    try:
        _ = zarr.open_consolidated(store)
    except KeyError:
        consolidated = False

    with xr.open_zarr(store, consolidated=consolidated) as ds:
        for var_name in ds.variables:
            var = ds[var_name]
            if var.ndim >= 1 and dimension in var.dims:
                if var.dims[0] != dimension:
                    # TODO: Remove this restriction -- it's not fundamentally
                    #   necessary. Removal should be accompanied by appropriate
                    #   unit tests and the addition of a warning to the user
                    #   about potential slowness / inefficiency.
                    raise ValueError(
                        f"dimension '{dimension}' of variable "
                        f"{var_name!r} must be first dimension")
                append_dim_var_names.append(var_name)
                enc = dict(ds[var_name].encoding)
                # xarray 0.17+ supports engine preferred chunks if exposed by
                # the backend zarr does that, but when we use the new
                # 'preferred_chunks' when writing to zarr it raises and says,
                # 'preferred_chunks' is an unsupported encoding
                if 'preferred_chunks' in enc:
                    del enc['preferred_chunks']
                encoding[var_name] = enc

    temp_dir = tempfile.TemporaryDirectory(prefix='nc2zarr-slice-',
                                           suffix='.zarr')
    dataslice.to_zarr(temp_dir.name, encoding=encoding)
    slice_root_group = zarr.open(temp_dir.name, mode='r')
    slice_arrays = dict(slice_root_group.arrays())

    root_group = zarr.open(store, mode='r+')
    for var_name, var_array in root_group.arrays():
        if var_name in append_dim_var_names:
            slice_array = slice_arrays[var_name]
            if insert_mode:
                # Add one empty step
                empty = zarr.creation.empty(slice_array.shape,
                                            dtype=var_array.dtype)
                var_array.append(empty, axis=0)
                # Shift contents
                var_array[insert_index + 1:, ...] = \
                    var_array[insert_index:-1, ...]
            # Replace slice
            var_array[insert_index, ...] = slice_array[0]

    if consolidated:
        zarr.consolidate_metadata(store)

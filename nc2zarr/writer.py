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

import datetime
import json
import os.path
from typing import Dict, Any, Sequence

import fsspec
import fsspec.implementations.local
import numpy as np
import pandas as pd
import retry.api
import xarray as xr
import zarr

from .constants import DEFAULT_OUTPUT_APPEND_DIM_NAME
from .constants import DEFAULT_OUTPUT_RETRY_KWARGS
from .custom import load_custom_func
from .log import LOGGER
from .log import log_duration
from .version import version


class DatasetWriter:
    def __init__(self,
                 output_path: str,
                 *,
                 output_custom_postprocessor: str = None,
                 output_consolidated: bool = False,
                 output_encoding: Dict[str, Dict[str, Any]] = None,
                 output_overwrite: bool = False,
                 output_append: bool = False,
                 output_append_dim: str = None,
                 output_adjust_metadata: bool = False,
                 output_metadata: Dict[str, Any] = None,
                 output_s3_kwargs: Dict[str, Any] = None,
                 output_retry_kwargs: Dict[str, Any] = None,
                 input_decode_cf: bool = False,
                 input_paths: Sequence[str] = None,
                 finalize_only: bool = False,
                 dry_run: bool = False):
        if not output_path:
            raise ValueError('output_path must be given')
        if output_append and output_custom_postprocessor:
            raise ValueError('output_append and output_custom_postprocessor'
                             ' cannot be given both')
        self._output_path = output_path
        self._output_custom_postprocessor = load_custom_func(output_custom_postprocessor) \
            if output_custom_postprocessor else None
        self._output_consolidated = output_consolidated
        self._output_encoding = output_encoding
        self._output_overwrite = output_overwrite
        self._output_append = output_append
        self._output_append_dim = output_append_dim or DEFAULT_OUTPUT_APPEND_DIM_NAME
        self._output_adjust_metadata = output_adjust_metadata
        self._output_metadata = output_metadata
        self._output_s3_kwargs = output_s3_kwargs
        self._output_retry_kwargs = output_retry_kwargs or DEFAULT_OUTPUT_RETRY_KWARGS
        self._input_decode_cf = input_decode_cf
        self._input_paths = input_paths
        self._finalize_only = finalize_only
        self._dry_run = dry_run
        if output_s3_kwargs or output_path.startswith('s3://'):
            self._fs = fsspec.filesystem('s3', **(output_s3_kwargs or {}))
        else:
            self._fs = fsspec.filesystem('file')
            self._output_path = os.path.expanduser(self._output_path)
        self._output_store = None
        self._output_path_exists = None

    def write_dataset(self,
                      ds: xr.Dataset,
                      encoding: Dict[str, Any] = None,
                      append: bool = None):
        if self._finalize_only:
            raise RuntimeError('internal error: '
                               'cannot write/append datasets when '
                               'in finalize-only mode')
        if self._output_custom_postprocessor is not None:
            ds = self._output_custom_postprocessor(ds)
        retry.api.retry_call(self._write_dataset,
                             fargs=[ds],
                             fkwargs=dict(encoding=encoding, append=append),
                             logger=LOGGER,
                             **self._output_retry_kwargs)

    def finalize_dataset(self):
        retry.api.retry_call(self._finalize_dataset,
                             logger=LOGGER,
                             **self._output_retry_kwargs)

    def _write_dataset(self,
                       ds: xr.Dataset,
                       encoding: Dict[str, Any] = None,
                       append: bool = None):
        encoding = encoding if encoding is not None else self._output_encoding
        append = append if append is not None else self._output_append
        self._ensure_store()
        if not append or not self._output_path_exists:
            self._create_dataset(ds, encoding)
        else:
            self._append_dataset(ds)

    def _ensure_store(self):
        if self._output_store is None:
            self._output_path_exists = self._fs.isdir(self._output_path)
            if self._finalize_only:
                if not self._output_path_exists:
                    raise FileNotFoundError(f'output path not found:'
                                            f' {self._output_path}')
            else:
                if self._output_overwrite and self._output_path_exists:
                    self._remove_dataset()
            self._output_store = self._fs.get_mapper(self._output_path)

    def _create_dataset(self, ds, encoding):
        with log_duration(f'Writing dataset'):
            if not self._dry_run:
                ds.to_zarr(self._output_store,
                           mode='w' if self._output_overwrite else 'w-',
                           encoding=encoding,
                           consolidated=self._output_consolidated)
            else:
                LOGGER.warning('Writing disabled, dry run!')
            self._output_path_exists = True

    def _append_dataset(self, ds: xr.Dataset):
        with log_duration('Appending dataset'):
            # Fix for https://github.com/bcdev/nc2zarr/issues/38
            # Get rid of variables that lack append_dim dimension:
            append_dim = self._output_append_dim
            ds = ds.drop_vars([var_name
                               for var_name, var in ds.data_vars.items()
                               if append_dim not in var.dims
                               and np.issubdtype(var.dtype, 'S')])

            if not self._input_decode_cf:
                # Fix for https://github.com/bcdev/nc2zarr/issues/35
                #
                # xarray 0.18.2 always CF-encodes variable data according to
                # encoding info of existing variables before it appends it.
                # If the data to be appended is already encoded (because we
                # read it by default with decode_cf=False) then this leads
                # to entirely corrupt data.
                # This hack decodes the data xarray if it was not decoded
                # on reading, making the encoded, written values correct;
                # however, this is fully redundant, costs extra CPU, and
                # may reduce precision.
                #
                # TODO: remove this hack once issue is fixed in xarray.
                ds = xr.decode_cf(ds)
                # For all slices except the first we must remove
                # encoding attributes e.g. "_FillValue" .
                ds = self._remove_variable_attrs(ds)

            if not self._dry_run:
                ds.to_zarr(self._output_store,
                           append_dim=append_dim,
                           consolidated=self._output_consolidated)
            else:
                LOGGER.warning('Appending disabled, dry run!')

    def _remove_dataset(self):
        with log_duration(f'Removing dataset {self._output_path}'):
            if not self._dry_run:
                self._fs.delete(self._output_path, recursive=True)
            else:
                LOGGER.warning('Removal disabled, dry run!')
            self._output_path_exists = False

    @classmethod
    def _remove_variable_attrs(cls, ds: xr.Dataset) -> xr.Dataset:
        ds = ds.copy()
        for k, v in ds.variables.items():
            v.attrs = dict()
        return ds

    def _finalize_dataset(self):
        if not self._output_adjust_metadata and not self._output_metadata:
            return

        with log_duration('Finalizing dataset'):
            adjusted_metadata = {}

            if self._output_adjust_metadata:
                self._ensure_store()
                # Get new attribute values
                with xr.open_zarr(self._output_store, decode_cf=True) as dataset:
                    history = self._get_history_metadata(dataset)
                    source = self._get_source_metadata(dataset)
                    time_coverage_start, time_coverage_end = \
                        self._get_time_coverage_metadata(dataset)
                    adjusted_data = (
                        ('history', history),
                        ('source', source),
                        ('time_coverage_start', time_coverage_start),
                        ('time_coverage_end', time_coverage_end),
                    )
                    adjusted_metadata = {k: v
                                         for k, v in adjusted_data
                                         if v is not None}
            if self._output_metadata:
                adjusted_metadata.update(self._output_metadata)

            LOGGER.info('Metadata update:\n', json.dumps(adjusted_metadata, indent=2))

            if not self._dry_run:
                self._ensure_store()
                # Externally modify attributes
                with zarr.open_group(self._output_store, cache_attrs=False) as group:
                    group.attrs.update(adjusted_metadata)
                if self._output_consolidated:
                    zarr.convenience.consolidate_metadata(self._output_store)
            else:
                LOGGER.warning('Updating of metadata disabled, dry run!')

    def _get_source_metadata(self, dataset: xr.Dataset):
        source = None
        if self._input_paths:
            # Note, currently we only name root sources, our NetCDF files.
            nc_paths = ', '.join(path for path in self._input_paths if path.endswith('.nc'))
            source = dataset.attrs.get('source')
            source = ((source + ',\n') if source else '') + nc_paths
        return source

    @classmethod
    def _get_history_metadata(cls, dataset: xr.Dataset):
        now = _np_timestamp_to_str(np.array(datetime.datetime.utcnow(), dtype=np.datetime64))
        present = f"{now}: converted by nc2zarr, version {version}"
        history = dataset.attrs.get("history")
        return ((history + '\n') if history else '') + present

    @classmethod
    def _get_time_coverage_metadata(cls, dataset: xr.Dataset):
        time_coverage_start, time_coverage_end = None, None
        if 'time' in dataset:
            time = dataset['time']
            bounds = time.attrs.get('bounds', 'time_bnds')
            if bounds in dataset \
                    and dataset[bounds].ndim == 2 \
                    and dataset[bounds].shape[1] == 2:
                time_bnds = dataset[bounds]
                time_coverage_start = _xr_timestamp_to_str(time_bnds[0, 0])
                time_coverage_end = _xr_timestamp_to_str(time_bnds[-1, 1])
            else:
                time_coverage_start = _xr_timestamp_to_str(time[0])
                time_coverage_end = _xr_timestamp_to_str(time[-1])
        return time_coverage_start, time_coverage_end


def _xr_timestamp_to_str(time_scalar: xr.DataArray):
    return _np_timestamp_to_str(time_scalar.values.item())


def _np_timestamp_to_str(time_scalar: np.ndarray):
    # noinspection PyTypeChecker
    return pd.to_datetime(time_scalar, utc=True) \
        .strftime("%Y-%m-%d %H:%M:%S")

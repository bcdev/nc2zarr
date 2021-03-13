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

from typing import Dict, Any

import fsspec
import fsspec.implementations.local
import retry.api
import xarray as xr

from .constants import DEFAULT_OUTPUT_APPEND_DIM_NAME
from .constants import DEFAULT_OUTPUT_RETRY_KWARGS
from .custom import load_custom_func
from .log import LOGGER
from .log import log_duration


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
                 output_s3_kwargs: Dict[str, Any] = None,
                 output_retry_kwargs: Dict[str, Any] = None,
                 reset_attrs: bool = False,
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
        self._output_s3_kwargs = output_s3_kwargs
        self._output_retry_kwargs = output_retry_kwargs or DEFAULT_OUTPUT_RETRY_KWARGS
        self._reset_attrs = reset_attrs
        self._dry_run = dry_run
        if output_s3_kwargs or output_path.startswith('s3://'):
            self._fs = fsspec.filesystem('s3', **(output_s3_kwargs or {}))
        else:
            self._fs = fsspec.filesystem('file')
        self._output_store = None
        self._output_path_exists = None

    def write_dataset(self,
                      ds: xr.Dataset,
                      encoding: Dict[str, Any] = None,
                      append: bool = None):
        if self._output_custom_postprocessor is not None:
            ds = self._output_custom_postprocessor(ds)
        retry.api.retry_call(self._write_dataset,
                             fargs=[ds],
                             fkwargs=dict(encoding=encoding, append=append),
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

    # def close(self):
    #     if self._output_store is not None \
    #             and hasattr(self._output_store, 'close') \
    #             and callable(self._output_store.close):
    #         self._output_store.close()

    def _ensure_store(self):
        if self._output_store is None:
            self._output_path_exists = self._fs.isdir(self._output_path)
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

    def _append_dataset(self, ds):
        with log_duration('Appending dataset'):
            if self._reset_attrs:
                # For all slices except the first we must remove
                # encoding attributes e.g. "_FillValue" .
                ds = self._remove_variable_attrs(ds)

            if not self._dry_run:
                ds.to_zarr(self._output_store,
                           append_dim=self._output_append_dim,
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

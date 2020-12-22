from typing import Dict, Any

import fsspec
import fsspec.implementations.local
import xarray as xr

from .constants import DEFAULT_CONCAT_DIM_NAME
from .log import LOGGER
from .log import log_duration


class DatasetWriter:
    def __init__(self,
                 output_path: str,
                 output_append_dim: str = DEFAULT_CONCAT_DIM_NAME,
                 output_consolidated: bool = False,
                 output_encoding: Dict[str, Dict[str, Any]] = None,
                 output_overwrite: bool = False,
                 output_append: bool = False,
                 output_s3_kwargs: Dict[str, Any] = None,
                 reset_attrs: bool = False,
                 dry_run: bool = False):
        if not output_path:
            raise ValueError('output_path must be given')
        self._output_path = output_path
        self._output_consolidated = output_consolidated
        self._output_encoding = output_encoding
        self._output_overwrite = output_overwrite
        self._output_append = output_append
        self._output_append_dim = output_append_dim
        self._output_s3_kwargs = output_s3_kwargs
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
            if self._output_overwrite and self._output_path_exists:
                self._remove_dataset()
            self._output_store = self._fs.get_mapper(self._output_path, check=False, create=False)

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

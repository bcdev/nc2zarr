import os.path
import shutil
from typing import Dict, Any

import s3fs
import xarray as xr

from .constants import DEFAULT_CONCAT_DIM
from .log import LOGGER
from .log import log_duration


class DatasetWriter:
    def __init__(self,
                 output_path: str,
                 output_append_dim: str = DEFAULT_CONCAT_DIM,
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
        self._output_store = None
        self._output_store_exists = None
        self._init_output_store()

    def _init_output_store(self):
        # TODO (forman): use fsspec here so we can get rid of following 2 code blocks
        if self._output_s3_kwargs:
            s3 = s3fs.S3FileSystem(**self._output_s3_kwargs)
            self._output_store_exists = s3.isdir(self._output_path)
            if self._output_overwrite and self._output_store_exists:
                with log_duration(f'Removing existing {self._output_path}'):
                    s3.rm(self._output_path, recursive=True)
                    self._output_store_exists = False
            self._output_store = s3fs.S3Map(self._output_path, s3=s3, create=True)
        else:
            self._output_store_exists = os.path.isdir(self._output_path)
            if self._output_overwrite and self._output_store_exists:
                with log_duration(f'Removing existing {self._output_path}'):
                    shutil.rmtree(self._output_path)
                    self._output_store_exists = False
            self._output_store = self._output_path

    def write_dataset(self,
                      ds: xr.Dataset,
                      encoding: Dict[str, Any] = None,
                      append: bool = None):
        encoding = encoding if encoding is not None else self._output_encoding
        append = append if append is not None else self._output_append
        if not append or not self._output_store_exists:
            with log_duration(f'Writing dataset'):
                if not self._dry_run:
                    ds.to_zarr(self._output_store,
                               mode='w' if self._output_overwrite else 'w-',
                               encoding=encoding,
                               consolidated=self._output_consolidated)
                else:
                    LOGGER.warning('Writing disabled, dry run!')
                self._output_store_exists = True
        else:
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

    @classmethod
    def _remove_variable_attrs(cls, ds: xr.Dataset) -> xr.Dataset:
        ds = ds.copy()
        for k, v in ds.variables.items():
            v.attrs = dict()
        return ds

# The MIT License (MIT)
# Copyright (c) 2022 by Brockmann Consult GmbH and contributors
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

import glob
import os.path
import warnings
from typing import List, Optional, Iterator, Callable, Union, Dict, Hashable

import s3fs

import xarray as xr

from .error import ConverterError
from .log import LOGGER
from .log import log_duration


class DatasetOpener:

    # TODO: passing anon value from config file
    s3 = s3fs.S3FileSystem(anon=False)

    def __init__(self,
                 input_paths: Union[str, List[str]],
                 *,
                 input_multi_file: bool = False,
                 input_sort_by: str = None,
                 input_decode_cf: bool = False,
                 input_concat_dim: str = None,
                 input_engine: str = None,
                 input_prefetch_chunks: bool = False):
        """Instantiate a new DatasetOpener object

        :param input_paths: paths of files to open
        :param input_multi_file: True to read all input files as one block,
               using xarray.open_mfdataset
        :param input_sort_by: how to sort input paths: "name", "path", or None
        :param input_decode_cf: True to decode inputs according to CF
               conventions
        :param input_concat_dim: name of dimension to be used for concatenation
               if input_multi_file is true. If concat_dim is set, opened
               files will be combined using xarray.open_mfdataset's "nested"
               mode; if concat_dim is omitted or set to None,
               xarray.open_mfdataset's "by_coords" mode will be used instead.
        :param input_engine: xarray engine used for opening the dataset
        :param input_prefetch_chunks:  Open one input to fetch internal
               chunking, if any. Then use this chunking to open all input files
               (and force using Dask arrays). This may slow down the process
               slightly, but may be required to avoid memory problems for very
               large inputs.
        """
        self._input_paths = input_paths
        self._input_multi_file = input_multi_file
        self._input_sort_by = input_sort_by
        self._input_decode_cf = input_decode_cf
        self._input_concat_dim = input_concat_dim
        self._input_engine = input_engine
        self._input_prefetch_chunks = input_prefetch_chunks

    def open_datasets(self,
                      preprocess: Callable[[xr.Dataset], xr.Dataset] = None) \
            -> Iterator[xr.Dataset]:
        input_paths = self._resolve_input_paths()
        chunks = self._prefetch_chunk_sizes(input_paths[0])
        if self._input_multi_file:
            return self._open_mfdataset(input_paths, chunks, preprocess)
        else:
            return self._open_datasets(input_paths, chunks, preprocess)

    def _open_mfdataset(
            self,
            input_paths: List[str],
            chunks: Optional[Dict[Hashable, int]],
            preprocess: Callable[[xr.Dataset], xr.Dataset] = None
    ) -> xr.Dataset:
        with log_duration(f'Opening {len(input_paths)} file(s)'):
            combine = 'nested'
            if not self._input_concat_dim:
                combine = 'by_coords'
                warnings.warn(f'input/concat_dim is not specified, '
                              f'combining by coordinates')
            # TODO: this bit of the code needs more work for different cases
            # e.g mixed sources - s3, local, glob are declared in `paths`
            if ('s3://' in input_path for input_path in input_paths):
                fileset = [self.s3.open(input_path) for input_path in input_paths]
                engine = 'h5netcdf'
            else:
                fileset = input_paths
                engine = self._input_engine
            ds = xr.open_mfdataset(
                fileset,
                engine=engine,
                preprocess=preprocess,
                concat_dim=self._input_concat_dim,
                decode_cf=self._input_decode_cf,
                chunks=chunks,
                combine=combine
            )
        yield ds

    def _open_datasets(self,
                       input_paths: List[str],
                       chunks: Optional[Dict[Hashable, int]],
                       preprocess: Callable[[xr.Dataset], xr.Dataset] = None) \
            -> Iterator[xr.Dataset]:
        n = len(input_paths)
        for i in range(n):
            if 's3://' in input_paths[i]:
                input_file = self.s3.open(input_paths[i])
                engine = 'h5netcdf'
            else:
                input_file = input_paths[i]
                engine = self._get_engine(input_file)
            LOGGER.info(f'Processing input {i + 1} of {n}: {input_paths[i]}')
            with log_duration('Opening'):
                ds = xr.open_dataset(input_file,
                                     engine=engine,
                                     decode_cf=self._input_decode_cf,
                                     chunks=chunks)
                if preprocess:
                    ds = preprocess(ds)
            yield ds

    def _prefetch_chunk_sizes(self, input_file: str) -> Optional[Dict[Hashable, int]]:
        if not self._input_prefetch_chunks:
            return None
        with log_duration('Pre-fetching chunks'):
            if 's3://' in input_file:
                file = self.s3.open(input_file)
                engine = 'h5netcdf'
            else:
                file = input_file
                engine = self._get_engine(input_file)
            with xr.open_dataset(file,
                                 engine=engine,
                                 decode_cf=self._input_decode_cf) as ds:
                chunk_sizes = dict()
                for var in ds.data_vars.values():
                    sizes = var.encoding.get('chunksizes')
                    if sizes and len(sizes) == len(var.dims):
                        for dim, size in zip(var.dims, sizes):
                            chunk_sizes[dim] = max(size, chunk_sizes.get(dim, 0))
                return chunk_sizes

    def _get_engine(self, input_file: str) -> Optional[str]:
        engine = self._input_engine
        if not engine and input_file.endswith('.zarr') and os.path.isdir(input_file):
            engine = 'zarr'
        return engine

    def _resolve_input_paths(self) -> List[str]:
        input_files = self.resolve_input_paths(self._input_paths, self._input_sort_by)
        if not input_files:
            raise ConverterError('No inputs given.')
        LOGGER.info(f'{len(input_files)} input(s) found:\n'
                    + ('\n'.join(map(lambda f: f'  {f[0]}: ' + f[1],
                                     zip(range(len(input_files)), input_files)))))
        return input_files

    @classmethod
    def resolve_input_paths(cls,
                            input_paths: Union[str, List[str]],
                            sort_by: str = None) -> List[str]:
        if not input_paths:
            return []

        if isinstance(input_paths, str):
            input_paths = [input_paths]

        resolved_input_files = []
        for input_path in input_paths:
            input_path = os.path.expanduser(input_path)
            if 's3://' in input_path:
                if '*' in input_path or '?' in input_path:
                    glob_result = cls.s3.glob(input_path)
                    if not glob_result:
                        raise ConverterError(f'No S3 inputs found for wildcard: "{input_path}"')
                    resolved_input_files.extend(['s3://' + path for path in glob_result])
                else:
                    try:
                        cls.s3.ls(input_path)
                        resolved_input_files.append(input_path)
                    except FileNotFoundError:
                        raise ConverterError(f'S3 input not found: "{input_path}"')
            else:
                if '*' in input_path or '?' in input_path:
                    glob_result = glob.glob(input_path, recursive=True)
                    if not glob_result:
                        raise ConverterError(f'No inputs found for wildcard: "{input_path}"')
                    resolved_input_files.extend(glob_result)
                else:
                    if not os.path.exists(input_path):
                        raise ConverterError(f'Input not found: "{input_path}"')
                    resolved_input_files.append(input_path)

        if sort_by:
            # Get rid of doubles and sort
            resolved_input_files = set(resolved_input_files)
            if sort_by == 'path' or sort_by is True:
                return sorted(resolved_input_files)
            if sort_by == 'name':
                return sorted(resolved_input_files, key=_sort_by_name_key)
            raise ConverterError(f'Can sort by "path" or "name" only, got "{sort_by}".')
        else:
            # Get rid of doubles, but preserve order
            seen_input_files = set()
            unique_input_files = []
            for input_file in resolved_input_files:
                if input_file not in seen_input_files:
                    unique_input_files.append(input_file)
                    seen_input_files.add(input_file)
            return unique_input_files


def _sort_by_name_key(path: str) -> str:
    while path.endswith('/') or path.endswith(os.path.sep):
        path = path[0:-1]
    return os.path.basename(path)

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

from typing import Any, Tuple, Dict

import xarray as xr

from .custom import load_custom_func


class DatasetProcessor:
    def __init__(self, *,
                 process_rename: Dict[str, str] = None,
                 process_rechunk: Dict[str, Any] = None,
                 process_custom_processor: str = None,
                 output_encoding: Dict[str, Dict[str, Any]] = None):
        self._process_rename = process_rename
        self._process_rechunk = process_rechunk
        self._process_custom_processor = load_custom_func(process_custom_processor) \
            if process_custom_processor else None
        self._output_encoding = output_encoding

    def process_dataset(self, ds: xr.Dataset) -> Tuple[xr.Dataset, Dict[str, Dict[str, Any]]]:
        if self._process_rename:
            ds = ds.rename(self._process_rename)
        if self._process_custom_processor is not None:
            ds = self._process_custom_processor(ds)
        if self._process_rechunk:
            ds, chunk_encoding = self._rechunk_dataset(ds, self._process_rechunk)
        else:
            chunk_encoding = dict()
        return ds, self._merge_encodings(ds,
                                         chunk_encoding,
                                         self._output_encoding or {})

    @classmethod
    def _rechunk_dataset(cls,
                         ds: xr.Dataset,
                         process_rechunk: Dict[str, Dict[str, int]]) \
            -> Tuple[xr.Dataset, Dict[str, Dict[str, Any]]]:
        ds_rechunked = ds.copy()
        output_encoding = dict()
        all_dim_chunk_sizes = process_rechunk.get('*', {})
        for k, v in ds.variables.items():
            var_name = str(k)
            # Compute default chunk sizes for dims of v
            dim_chunk_sizes = dict(all_dim_chunk_sizes)
            if var_name in process_rechunk:
                dim_chunk_sizes_update = process_rechunk[var_name]
                if dim_chunk_sizes_update is None \
                        or isinstance(dim_chunk_sizes_update, int) \
                        or dim_chunk_sizes_update == 'input':
                    dim_chunk_sizes_update = {dim_name: dim_chunk_sizes_update for dim_name in v.dims}
                elif isinstance(dim_chunk_sizes_update, dict):
                    dim_chunk_sizes_update = {dim_name: dim_chunk_sizes_update.get(dim_name) for dim_name in v.dims}
                # Update chunk sizes with defaults for v
                dim_chunk_sizes.update(dim_chunk_sizes_update)
            # Now loop through all dims of variable to
            # resolve each dimension's integer chunk size
            chunks = []
            for dim_index in range(len(v.dims)):
                dim_name = v.dims[dim_index]
                dim_chunk_size = dim_chunk_sizes.get(dim_name, 'input')
                if dim_chunk_size == 'input':
                    dim_chunk_size = max(*v.chunks[dim_index]) if v.chunks is not None else v.sizes[dim_name]
                elif dim_chunk_size is None:
                    dim_chunk_size = v.sizes[dim_name]
                elif not isinstance(dim_chunk_size, int):
                    raise ValueError(f'invalid chunk size: {dim_chunk_size}')
                chunks.append(dim_chunk_size)
            if chunks:
                chunks = tuple(chunks)
                ds_rechunked[k] = v.chunk(chunks)
                output_encoding[var_name] = dict(chunks=chunks)
        return ds_rechunked, output_encoding

    @classmethod
    def _merge_encodings(cls,
                         ds: xr.Dataset,
                         *encodings: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        output_encoding = dict()
        for encoding in encodings:
            for k, v in ds.variables.items():
                var_name = str(k)
                if var_name in encoding:
                    if var_name not in output_encoding:
                        output_encoding[var_name] = dict(encoding[var_name])
                    else:
                        output_encoding[var_name].update(encoding[var_name])
        return output_encoding

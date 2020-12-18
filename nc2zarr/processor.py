from typing import Any, Tuple, Dict

import xarray as xr


class DatasetProcessor:
    def __init__(self,
                 process_rename: Dict[str, str] = None,
                 process_rechunk: Dict[str, Dict[str, int]] = None,
                 output_encoding: Dict[str, Dict[str, Any]] = None):
        self._process_rename = process_rename
        self._process_rechunk = process_rechunk
        self._output_encoding = output_encoding

    def process_dataset(self, ds: xr.Dataset) -> Tuple[xr.Dataset, Dict[str, Dict[str, Any]]]:
        if self._process_rename:
            ds = ds.rename(self._process_rename)
        if self._process_rechunk:
            chunk_encoding = self._get_chunk_encodings(ds, self._process_rechunk)
        else:
            chunk_encoding = dict()
        return ds, self._merge_encodings(ds,
                                         chunk_encoding,
                                         self._output_encoding or {})

    @classmethod
    def _get_chunk_encodings(cls,
                             ds: xr.Dataset,
                             process_rechunk: Dict[str, Dict[str, int]]) \
            -> Dict[str, Dict[str, Any]]:
        output_encoding = dict()
        all_chunk_sizes = process_rechunk.get('*', {})
        for k, v in ds.variables.items():
            var_name = str(k)
            var_chunk_sizes = dict(all_chunk_sizes)
            var_chunk_sizes.update(process_rechunk.get(var_name, {}))
            chunks = []
            for dim_index in range(len(v.dims)):
                dim_name = v.dims[dim_index]
                if dim_name in var_chunk_sizes:
                    chunks.append(var_chunk_sizes[dim_name])
                else:
                    chunks.append(v.chunks[dim_index]
                                  if v.chunks is not None else v.sizes[dim_name])
            output_encoding[var_name] = dict(chunks=tuple(chunks))
        return output_encoding

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

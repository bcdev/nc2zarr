import glob
import os.path
from typing import List, Optional, Iterator, Callable

import xarray as xr

from .error import ConverterError
from .log import LOGGER
from .log import log_duration


class DatasetOpener:
    def __init__(self,
                 input_paths: List[str],
                 input_sort_by: str = None,
                 input_decode_cf: bool = False,
                 input_concat_dim: str = None,
                 input_engine: str = None,
                 verbosity: int = None):
        self._input_paths = input_paths
        self._input_sort_by = input_sort_by
        self._input_decode_cf = input_decode_cf
        self._input_concat_dim = input_concat_dim
        self._input_engine = input_engine

        input_files = self.get_input_files(self._input_paths, self._input_sort_by)
        if not input_files:
            raise ConverterError('at least one input file must be given')
        LOGGER.info(f'{len(input_files)} input file(s) given.')
        if verbosity:
            LOGGER.info('Input file(s):\n'
                        + ('\n'.join(map(lambda f: f'  {f[0]}: ' + f[1],
                                         zip(range(len(input_files)), input_files)))))
        self._input_files = input_files

    def open_slices(self) -> Iterator[xr.Dataset]:
        n = len(self._input_files)
        for i in range(n):
            input_file = self._input_files[i]
            LOGGER.info(f'Processing slice {i + 1} of {n}: {input_file}')
            with log_duration('Opening'):
                yield xr.open_dataset(input_file,
                                      engine=self._input_engine,
                                      decode_cf=self._input_decode_cf)

    def open_dataset(self, pre_process: Callable[[xr.Dataset], xr.Dataset] = None) -> xr.Dataset:
        with log_duration(f'Opening {len(self._input_files)} file(s)'):
            return xr.open_mfdataset(self._input_files,
                                     engine=self._input_engine,
                                     preprocess=pre_process,
                                     concat_dim=self._input_concat_dim,
                                     decode_cf=self._input_decode_cf)

    @classmethod
    def get_input_files(cls,
                        input_paths: List[str],
                        sort_by: Optional[str]) -> List[str]:
        input_files = []
        if isinstance(input_paths, str):
            input_files.extend(glob.glob(input_paths, recursive=True))
        elif input_paths is not None and len(input_paths):
            for input_path in input_paths:
                input_files.extend(glob.glob(input_path, recursive=True))

        if sort_by:
            # Get rid of doubles and sort
            input_files = set(input_files)
            if sort_by == 'path' or sort_by is True:
                return sorted(input_files)
            if sort_by == 'name':
                return sorted(input_files, key=os.path.basename)
            raise ConverterError(f'Cannot sort by "{sort_by}".')
        else:
            # Get rid of doubles, but preserve order
            seen_input_files = set()
            unique_input_files = []
            for input_file in input_files:
                if input_file not in seen_input_files:
                    unique_input_files.append(input_file)
                    seen_input_files.add(input_file)
            return unique_input_files

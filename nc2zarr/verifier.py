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
import xarray as xr

from .log import LOGGER
from .log import log_duration


class DatasetVerifier:
    def __init__(self,
                 output_path: str,
                 output_s3_kwargs: Dict[str, Any] = None,
                 verify_enabled: Dict[str, Any] = None,
                 verify_open_params: Dict[str, Any] = None,
                 dry_run: bool = False):
        if not output_path:
            raise ValueError('output_path must be given')
        self._output_path = output_path
        self._output_s3_kwargs = output_s3_kwargs
        self._verify_enabled = verify_enabled
        self._verify_open_params = verify_open_params or {}
        self._dry_run = dry_run

    def verify_dataset(self):

        if not self._verify_enabled:
            LOGGER.info('Dataset verification disabled.')
            return

        with log_duration(f'Verifying dataset'):
            if not self._dry_run:
                if self._output_s3_kwargs or self._output_path.startswith('s3://'):
                    fs = fsspec.filesystem('s3', **(self._output_s3_kwargs or {}))
                else:
                    fs = fsspec.filesystem('file')
                store = fs.get_mapper(self._output_path, check=False, create=False)
                # noinspection PyBroadException
                try:
                    dataset = xr.open_zarr(store, **self._verify_open_params)
                    LOGGER.info(dataset)
                    LOGGER.info('Dataset verification passed.')
                except BaseException as e:
                    LOGGER.error('Dataset verification failed!')
            else:
                LOGGER.info('Dataset verification skipped, it is a dry run.')

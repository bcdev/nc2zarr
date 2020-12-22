# The MIT License (MIT)
# Copyright (c) 2020 by Brockmann Consult GmbH and contributors
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

import contextlib
import logging
import sys
import time
from typing import Union

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: %(levelname)s: %(name)s: %(message)s',
    stream=sys.stderr,
)

LOGGER = logging.getLogger('nc2zarr')


class log_duration(contextlib.AbstractContextManager):

    def __init__(self, tag: str = None, verbose: Union[bool, int] = True):
        self.tag = tag or 'task'
        self.verbose = int(verbose)
        self.start = None
        self.duration = None

    def __enter__(self):
        self.start = time.perf_counter()
        if self.verbose > 1:
            LOGGER.info(f'{self.tag}...')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.duration = time.perf_counter() - self.start
        if self.verbose > 0 and exc_type is None:
            LOGGER.info(f'{self.tag} done: took {self.duration:,.2f} seconds')

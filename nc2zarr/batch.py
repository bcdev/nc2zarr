# The MIT License (MIT)
# Copyright (c) 2020 by the ESA CCI Toolbox development team and contributors
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

import subprocess
import sys
import uuid
from typing import Sequence, Type

from nc2zarr.logger import LOGGER


def run_batch_mode(input_files: Sequence[str],
                   batch_size: int,
                   config_path: str = None,
                   exception_type: Type[Exception] = OSError):
    import nc2zarr.cli

    # TODO: implement me: group and combine batches then exit, this is just test code
    num_input_files = len(input_files)
    num_batches = num_input_files // batch_size

    LOGGER.info(f'Processing {num_input_files} file(s) in {num_batches} batch(es) of size {batch_size}')

    job_id = str(uuid.uuid4())
    for batch_index in range(num_batches):
        batch_input_files = input_files[batch_index * batch_size: (batch_index + 1) * batch_size]
        batch_output_path = f'{job_id}-{batch_index}.zarr'
        command = [sys.executable, nc2zarr.cli.__file__]
        if config_path is not None:
            command.extend(['-c', config_path])
        command.extend(['-o', batch_output_path])
        command.extend(batch_input_files)
        batch_exit_code = subprocess.call(command)
        if batch_exit_code != 0:
            raise exception_type(f'batch processing failed with exit code {batch_exit_code}')

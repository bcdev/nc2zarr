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

from typing import Sequence, Union, Any, Dict, List

from .constants import DEFAULT_CONCAT_DIM_NAME
from .constants import DEFAULT_OUTPUT_PATH
from .error import ConverterError
from .log import LOGGER
from .log import set_verbosity
from .opener import DatasetOpener
from .preprocessor import DatasetPreProcessor
from .processor import DatasetProcessor
from .writer import DatasetWriter


def nc2zarr(input_paths: Union[str, Sequence[str]] = None,
            input_sort_by: str = None,
            input_variables: List[str] = None,
            input_multi_file: bool = False,
            input_concat_dim: str = None,
            input_engine: str = None,
            input_decode_cf: bool = False,
            input_datetime_format: str = None,
            process_rename: Dict[str, str] = None,
            process_rechunk: Dict[str, Dict[str, int]] = None,
            output_path: str = None,
            output_encoding: Dict[str, Dict[str, Any]] = None,
            output_consolidated: bool = False,
            output_overwrite: bool = False,
            output_append: bool = False,
            output_append_dim: str = None,
            output_s3: Dict[str, Any] = None,
            dry_run: bool = False,
            verbosity: int = None):
    """
    TODO: describe me any py params.

    :param input_paths:
    :param input_sort_by:
    :param input_variables:
    :param input_multi_file:
    :param input_concat_dim:
    :param input_engine:
    :param input_decode_cf:
    :param input_datetime_format:
    :param process_rename:
    :param process_rechunk:
    :param output_path:
    :param output_encoding:
    :param output_consolidated:
    :param output_overwrite:
    :param output_append:
    :param output_append_dim:
    :param output_s3:
    :param dry_run:
    :param verbosity:
    """
    input_paths = [input_paths] if isinstance(input_paths, str) else input_paths
    output_path = output_path or DEFAULT_OUTPUT_PATH
    if input_concat_dim is None and output_append_dim is None:
        input_concat_dim = output_append_dim = DEFAULT_CONCAT_DIM_NAME
    elif input_concat_dim is None or output_append_dim is None:
        input_concat_dim = output_append_dim = input_concat_dim or output_append_dim

    if output_overwrite and output_append:
        raise ConverterError('Output overwrite and append flags cannot be given both.')

    if isinstance(verbosity, int):
        set_verbosity(verbosity)

    if dry_run:
        LOGGER.warning('Dry run!')

    opener = DatasetOpener(input_paths=input_paths,
                           input_multi_file=input_multi_file,
                           input_sort_by=input_sort_by,
                           input_decode_cf=input_decode_cf,
                           input_concat_dim=input_concat_dim,
                           input_engine=input_engine)

    pre_processor = DatasetPreProcessor(input_variables=input_variables,
                                        input_concat_dim=input_concat_dim,
                                        input_datetime_format=input_datetime_format)

    processor = DatasetProcessor(process_rechunk=process_rechunk,
                                 process_rename=process_rename,
                                 output_encoding=output_encoding)

    writer = DatasetWriter(output_path=output_path,
                           output_consolidated=output_consolidated,
                           output_encoding=output_encoding,
                           output_overwrite=output_overwrite,
                           output_append=output_append,
                           output_append_dim=output_append_dim,
                           output_s3_kwargs=output_s3,
                           dry_run=dry_run,
                           reset_attrs=not input_decode_cf)

    append = None
    for input_dataset in opener.open_datasets(preprocess=pre_processor.preprocess_dataset):
        output_dataset, output_encoding = processor.process_dataset(input_dataset)
        writer.write_dataset(output_dataset, encoding=output_encoding, append=append)
        input_dataset.close()
        append = True

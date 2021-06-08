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

from typing import Sequence, Union, Any, Dict, List

from .constants import DEFAULT_OUTPUT_APPEND_DIM_NAME
from .constants import DEFAULT_OUTPUT_PATH
from .error import ConverterError
from .log import LOGGER
from .log import log_duration
from .log import use_verbosity
from .opener import DatasetOpener
from .preprocessor import DatasetPreProcessor
from .processor import DatasetProcessor
from .writer import DatasetWriter


class Converter:
    """
    TODO: describe me and my params.

    :param input_paths:
    :param input_sort_by:
    :param input_variables:
    :param input_custom_preprocessor:
    :param input_multi_file:
    :param input_concat_dim:
    :param input_engine:
    :param input_decode_cf:
    :param input_datetime_format:
    :param input_prefetch_chunks:
    :param process_rename:
    :param process_custom_processor:
    :param process_rechunk:
    :param output_path:
    :param output_encoding:
    :param output_consolidated:
    :param output_overwrite:
    :param output_append: append to existing dataset, if one is present.
           If there is no existing dataset, one will be created regardless
           of the value of this parameter.
    :param output_append_dim:
    :param output_adjust_metadata:
    :param output_metadata:
    :param output_s3:
    :param output_custom_postprocessor:
    :param finalize_only:
    :param dry_run:
    :param verbosity:
    """

    def __init__(self, *,
                 input_paths: Union[str, Sequence[str]] = None,
                 input_sort_by: str = None,
                 input_variables: List[str] = None,
                 input_custom_preprocessor: str = None,
                 input_multi_file: bool = False,
                 input_concat_dim: str = None,
                 input_engine: str = None,
                 input_decode_cf: bool = False,
                 input_datetime_format: str = None,
                 input_prefetch_chunks: bool = False,
                 process_rename: Dict[str, str] = None,
                 process_custom_processor: str = None,
                 process_rechunk: Dict[str, Dict[str, int]] = None,
                 output_path: str = None,
                 output_encoding: Dict[str, Dict[str, Any]] = None,
                 output_consolidated: bool = False,
                 output_overwrite: bool = False,
                 output_append: bool = False,
                 output_append_dim: str = None,
                 output_adjust_metadata: bool = False,
                 output_metadata: Dict[str, Any] = None,
                 output_s3: Dict[str, Any] = None,
                 output_retry: Dict[str, Any] = None,
                 output_custom_postprocessor: str = False,
                 finalize_only: bool = False,
                 dry_run: bool = False,
                 verbosity: int = None):

        input_paths = [input_paths] if isinstance(input_paths, str) else input_paths
        if not input_paths:
            raise ConverterError('At least one input must be given.')

        # Maybe raise warning here saying that "out.zarr" is used by default.
        output_path = output_path or DEFAULT_OUTPUT_PATH

        if input_multi_file and input_concat_dim is None:
            # If input_multi_file is True, we need a input_concat_dim.
            # Maybe raise warning here saying that "time" is used by default.
            input_concat_dim = output_append_dim or DEFAULT_OUTPUT_APPEND_DIM_NAME

        # output_append_dim is used independently of output_append, namely
        # whenever there is more than one input file.
        output_append_dim = input_concat_dim or DEFAULT_OUTPUT_APPEND_DIM_NAME

        if output_overwrite and output_append:
            raise ConverterError('Output overwrite and append flags '
                                 'cannot both be given.')

        if output_metadata \
                and (not isinstance(output_metadata, dict)
                     or any(not isinstance(k, str) for k in output_metadata)):
            raise ConverterError('Output metadata must be a mapping '
                                 'from attribute names to values.')

        self.input_paths = input_paths
        self.input_sort_by = input_sort_by
        self.input_variables = input_variables
        self.input_custom_preprocessor = input_custom_preprocessor
        self.input_multi_file = input_multi_file
        self.input_concat_dim = input_concat_dim
        self.input_engine = input_engine
        self.input_decode_cf = input_decode_cf
        self.input_datetime_format = input_datetime_format
        self.input_prefetch_chunks = input_prefetch_chunks
        self.process_rename = process_rename
        self.process_custom_processor = process_custom_processor
        self.process_rechunk = process_rechunk
        self.output_path = output_path
        self.output_custom_postprocessor = output_custom_postprocessor
        self.output_encoding = output_encoding
        self.output_consolidated = output_consolidated
        self.output_overwrite = output_overwrite
        self.output_append = output_append
        self.output_append_dim = output_append_dim
        self.output_adjust_metadata = output_adjust_metadata
        self.output_metadata = output_metadata
        self.output_s3 = output_s3
        self.output_retry = output_retry
        self.finalize_only = finalize_only
        self.dry_run = dry_run
        self.verbosity = verbosity

    def run(self):
        with use_verbosity(self.verbosity or 0):
            with log_duration('Converting'):
                self._run()

    def _run(self):
        if self.dry_run:
            LOGGER.warning('Dry run!')

        if self.output_adjust_metadata:
            input_paths = DatasetOpener.resolve_input_paths(self.input_paths, sort_by=self.input_sort_by)
        else:
            input_paths = self.input_paths

        opener = DatasetOpener(input_paths=input_paths,
                               input_multi_file=self.input_multi_file,
                               input_sort_by=self.input_sort_by,
                               input_decode_cf=self.input_decode_cf,
                               input_concat_dim=self.input_concat_dim,
                               input_engine=self.input_engine,
                               input_prefetch_chunks=self.input_prefetch_chunks)

        pre_processor = DatasetPreProcessor(input_variables=self.input_variables,
                                            input_custom_preprocessor=self.input_custom_preprocessor,
                                            input_concat_dim=self.input_concat_dim,
                                            input_datetime_format=self.input_datetime_format)

        processor = DatasetProcessor(process_rechunk=self.process_rechunk,
                                     process_custom_processor=self.process_custom_processor,
                                     process_rename=self.process_rename,
                                     output_encoding=self.output_encoding)

        writer = DatasetWriter(output_path=self.output_path,
                               output_custom_postprocessor=self.output_custom_postprocessor,
                               output_consolidated=self.output_consolidated,
                               output_encoding=self.output_encoding,
                               output_overwrite=self.output_overwrite,
                               output_append=self.output_append,
                               output_append_dim=self.output_append_dim,
                               output_adjust_metadata=self.output_adjust_metadata,
                               output_metadata=self.output_metadata,
                               output_s3_kwargs=self.output_s3,
                               output_retry_kwargs=self.output_retry,
                               input_decode_cf=self.input_decode_cf,
                               input_paths=input_paths,
                               finalize_only=self.finalize_only,
                               dry_run=self.dry_run)

        if not self.finalize_only:
            append = None
            for input_dataset in opener.open_datasets(preprocess=pre_processor.preprocess_dataset):
                output_dataset, output_encoding = processor.process_dataset(input_dataset)
                writer.write_dataset(output_dataset, encoding=output_encoding, append=append)
                input_dataset.close()
                append = True
        else:
            LOGGER.warning('Running finalizer tasks only, no input data is being consumed.')

        writer.finalize_dataset()

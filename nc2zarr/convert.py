from typing import Sequence, Union, Any, Dict, List

from .constants import DEFAULT_CONCAT_DIM
from .constants import DEFAULT_OUTPUT_PATH
from .error import ConverterError
from .log import LOGGER
from .opener import DatasetOpener
from .preprocessor import DatasetPreProcessor
from .processor import DatasetProcessor
from .writer import DatasetWriter


# noinspection PyUnusedLocal
def convert_netcdf_to_zarr(input_paths: Union[str, Sequence[str]] = None,
                           input_variables: List[str] = None,
                           input_multi_file: bool = False,
                           input_concat_dim: str = None,
                           input_engine: str = None,
                           input_decode_cf: bool = False,
                           input_sort_by: str = None,
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
    input_paths = [input_paths] if isinstance(input_paths, str) else input_paths
    output_path = output_path or DEFAULT_OUTPUT_PATH
    if input_concat_dim is None and output_append_dim is None:
        input_concat_dim = output_append_dim = DEFAULT_CONCAT_DIM
    elif input_concat_dim is None or output_append_dim is None:
        input_concat_dim = output_append_dim = input_concat_dim or output_append_dim

    if output_overwrite and output_append:
        raise ConverterError('Output overwrite and append flags cannot be given both')

    if dry_run:
        LOGGER.warning('Dry run!')

    opener = DatasetOpener(input_paths=input_paths,
                           input_multi_file=input_multi_file,
                           input_sort_by=input_sort_by,
                           input_decode_cf=input_decode_cf,
                           input_concat_dim=input_concat_dim,
                           input_engine=input_engine,
                           verbosity=verbosity)

    pre_processor = DatasetPreProcessor(input_variables=input_variables,
                                        input_concat_dim=input_concat_dim,
                                        verbosity=verbosity)

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

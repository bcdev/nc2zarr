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

from typing import List, Optional, Sequence

import click

from nc2zarr.constants import DEFAULT_CONCAT_DIM
from nc2zarr.constants import DEFAULT_OUTPUT_PATH


@click.command(name='nc2zarr')
@click.argument('input_paths', nargs=-1, metavar='[INPUT_FILE ...]')
@click.option('--config', '-c', 'config_paths', metavar='CONFIG_FILE', multiple=True,
              help=f'Configuration file (YAML). Multiple may be given.')
@click.option('--output', '-o', 'output_path', metavar='OUTPUT_PATH',
              help=f'Output name. Defaults to "{DEFAULT_OUTPUT_PATH}".')
@click.option('--concat-dim', '-d', 'concat_dim', metavar='DIM',
              help=f'Dimension for concatenation. Defaults to "{DEFAULT_CONCAT_DIM}".')
@click.option('--multi-file', '-m', 'multi_file', is_flag=True, default=None,
              help='Open multiple input files as one block. Works for NetCDF files only. '
                   'Use --concat-dim to specify the dimension for concatenation.')
@click.option('--overwrite', '-w', 'overwrite', is_flag=True, default=None,
              help='Overwrite existing OUTPUT_PATH. '
                   'If OUTPUT_PATH does not exist, the option has no effect. '
                   'Cannot be used with --append.')
@click.option('--append', '-a', 'append', is_flag=True, default=None,
              help=f'Append inputs to existing OUTPUT_PATH. '
                   f'If OUTPUT_PATH does not exist, the option has no effect. '
                   'Cannot be used with --overwrite.')
@click.option('--decode-cf', 'decode_cf', is_flag=True, default=None,
              help=f'Decode variables according to CF conventions. '
                   f'Caution: array data may be converted to floating point type '
                   f'if a "_FillValue" attribute is present.')
@click.option('--dry-run', '-d', 'dry_run', is_flag=True, default=None,
              help='Open and process inputs only, omit data writing.')
@click.option('--verbose', '-v', 'verbosity', is_flag=True, multiple=True,
              help='Print more output.')
@click.option('--version', is_flag=True,
              help='Show version number and exit.')
def nc2zarr(input_paths: List[str],
            output_path: str,
            config_paths: List[str],
            multi_file: bool,
            concat_dim: Optional[str],
            overwrite: bool,
            append: bool,
            decode_cf: bool,
            dry_run: bool,
            verbosity: Sequence[int],
            version: bool):
    """
    Reads one or input datasets and writes or appends them to a single Zarr output dataset.

    INPUT_FILE may refer to a NetCDF file, or Zarr dataset, or a glob that identifies multiple
    of them, e.g. "L3_SST/**/*.nc".

    OUTPUT_PATH must be directory which will contain the output Zarr dataset, e.g. "L3_SST.zarr".

    CONFIG_FILE has YAML format. If multiple are given, their "input", "process", and "output"
    entries are merged while other settings overwrite each other in the order they appear.
    Command line arguments overwrite settings in any CONFIG_FILE:

    \b
    [--dry-run] overwrites /dry_run
    [INPUT_FILE ...] overwrites /input/paths in CONFIG_FILE
    [--multi-file] overwrites /input/multi_file
    [--concat-dim] overwrites /input/concat_dim
    [--decode-cf] overwrites /input/decode_cf
    [--output OUTPUT_FILE] overwrites /output/path
    [--overwrite] overwrites /output/overwrite
    [--append] overwrites /output/append
    """

    if version:
        from nc2zarr.version import version
        print(version)
        return 0

    from nc2zarr.config import load_config
    from nc2zarr.convert import convert_netcdf_to_zarr
    from nc2zarr.log import log_duration
    from nc2zarr.error import ConverterError
    try:
        with log_duration('Converting'):
            config_kwargs = load_config(config_paths=config_paths,
                                        return_kwargs=True,
                                        input_paths=input_paths or None,
                                        input_decode_cf=decode_cf,
                                        input_multi_file=multi_file,
                                        input_concat_dim=concat_dim,
                                        output_path=output_path,
                                        output_overwrite=overwrite,
                                        output_append=append,
                                        verbosity=sum(verbosity) if verbosity else None,
                                        dry_run=dry_run)
            convert_netcdf_to_zarr(**config_kwargs)
    except ConverterError as e:
        raise click.ClickException(e) from e


if __name__ == '__main__':
    nc2zarr()

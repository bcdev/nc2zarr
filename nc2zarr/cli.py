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

from typing import Optional, Tuple

import click

from nc2zarr.constants import DEFAULT_OUTPUT_APPEND_DIM_NAME
from nc2zarr.constants import DEFAULT_OUTPUT_PATH


@click.command(name='nc2zarr')
@click.argument('input_paths', nargs=-1, metavar='[INPUT_FILE ...]')
@click.option('--config', '-c', 'config_paths', metavar='CONFIG_FILE', multiple=True,
              help=f'Configuration file (YAML). Multiple may be given.')
@click.option('--output', '-o', 'output_path', metavar='OUTPUT_PATH',
              help=f'Output name. Defaults to "{DEFAULT_OUTPUT_PATH}".')
@click.option('--concat-dim', '-d', 'concat_dim', metavar='DIM_NAME',
              help=f'Dimension for concatenation. Defaults to "{DEFAULT_OUTPUT_APPEND_DIM_NAME}".')
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
@click.option('--sort-by', '-s', 'sort_by', default=None,
              type=click.Choice(['path', 'name'], case_sensitive=True),
              help='Sort input files by specified property.')
@click.option('--verify', 'verify', type=click.Choice(["on", 'off', 'auto']),
              default='auto',
              help='Switch verification either on, or off,'
                   ' or leave it up to CONFIG_FILE (=auto, the default).')
@click.option('--dry-run', '-d', 'dry_run', is_flag=True, default=None,
              help='Open and process inputs only, omit data writing.')
@click.option('--verbose', '-v', 'verbose', is_flag=True, multiple=True,
              help='Print more output. Use twice for even more output.')
@click.option('--version', is_flag=True,
              help='Show version number and exit.')
def nc2zarr(input_paths: Tuple[str],
            output_path: str,
            config_paths: Tuple[str],
            multi_file: bool,
            concat_dim: Optional[str],
            overwrite: bool,
            append: bool,
            decode_cf: bool,
            sort_by: str,
            verify: str,
            dry_run: bool,
            verbose: Tuple[bool],
            version: bool):
    """
    Reads one or more input datasets and writes or appends them to a single
    Zarr output dataset.

    INPUT_FILE may refer to a NetCDF file, or Zarr dataset, or a glob that
    identifies multiple paths, e.g. "L3_SST/**/*.nc".

    OUTPUT_PATH must be directory which will contain the output Zarr dataset,
    e.g. "L3_SST.zarr".

    CONFIG_FILE must be in YAML format. It comprises the optional objects
    "input", "process", and "output".
    See nc2zarr/res/config-template.yml for a template file that describes the format.
    Multiple --config options may be passed as a chain to allow for
    reuse of credentials and other common parameters. Contained configuration objects
    are recursively merged, lists are appended, and other values overwrite each
    other from left to right. For example:

    \b
    nc2zarr -c s3.yml -c common.yml -c inputs-01.yml -o out-01.zarr
    nc2zarr -c s3.yml -c common.yml -c inputs-02.yml -o out-02.zarr
    nc2zarr out-01.zarr out-02.zarr -o final.zarr

    Command line arguments and options have precedence over other
    configurations and thus overwrite settings in any CONFIG_FILE:

    \b
    [--dry-run] overwrites /dry_run
    [--verbose] overwrites /verbosity
    [INPUT_FILE ...] overwrites /input/paths in CONFIG_FILE
    [--multi-file] overwrites /input/multi_file
    [--concat-dim] overwrites /input/concat_dim
    [--decode-cf] overwrites /input/decode_cf
    [--sort-by] overwrites /input/sort_by
    [--output OUTPUT_FILE] overwrites /output/path
    [--overwrite] overwrites /output/overwrite
    [--append] overwrites /output/append
    """

    if version:
        from nc2zarr.version import version
        print(version)
        return 0

    from nc2zarr.config import load_config
    from nc2zarr.converter import Converter
    from nc2zarr.error import ConverterError
    try:
        config_kwargs = load_config(config_paths=config_paths,
                                    return_kwargs=True,
                                    input_paths=input_paths or None,
                                    input_decode_cf=decode_cf,
                                    input_multi_file=multi_file,
                                    input_concat_dim=concat_dim,
                                    input_sort_by=sort_by,
                                    output_path=output_path,
                                    output_overwrite=overwrite,
                                    output_append=append,
                                    verify_enabled=True if verify == 'on' else False if verify == 'off' else None,
                                    verbosity=sum(verbose) if verbose else None,
                                    dry_run=dry_run)
        Converter(**config_kwargs).run()
    except ConverterError as e:
        raise click.ClickException(e) from e


if __name__ == '__main__':
    nc2zarr()

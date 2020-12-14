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

from typing import List, Optional

import click

from nc2zarr.constants import DEFAULT_MODE
from nc2zarr.constants import DEFAULT_OUTPUT_FILE
from nc2zarr.constants import MODE_CHOICES


@click.command(name='nc2zarr')
@click.argument('input_files', nargs=-1, metavar='[INPUT_FILE ...]')
@click.option('--output', '-o', 'output_path', metavar='OUTPUT_PATH',
              help=f'Output name. Defaults to "{DEFAULT_OUTPUT_FILE}".')
@click.option('--config', '-c', 'config_paths', metavar='CONFIG_FILE', multiple=True,
              help=f'Configuration file (YAML). Multiple may be given.')
@click.option('--batch', '-b', 'batch_size', metavar='BATCH_SIZE', type=int,
              help=f'Batch size. If greater zero, conversion will be performed in batches of the given size.')
@click.option('--mode', '-m', 'mode', metavar='MODE', type=click.Choice(MODE_CHOICES),
              help=f'Input open mode. Must be one of {MODE_CHOICES}. Defaults to "{DEFAULT_MODE}".')
@click.option('--decode-cf', 'decode_cf', is_flag=True,
              help=f'Decode variables according to CF conventions. '
                   f'Caution: array data may be converted to floating point type '
                   f'if a "_FillValue" attribute is present.')
@click.option('--dry-run', '-d', 'dry_run', is_flag=True,
              help='Open and process inputs only, omit data writing.')
@click.option('--verbose', '-v', is_flag=True,
              help='Print more output.')
@click.option('--version', is_flag=True,
              help='Show version number and exit.')
def main(input_files: List[str],
         output_path: str,
         config_paths: List[str],
         batch_size: Optional[int],
         mode: str,
         decode_cf: bool,
         dry_run: bool,
         verbose: bool,
         version: bool):
    """
    Converts multiple NetCDF files to a single Zarr dataset.

    INPUT_FILE may refer to a NetCDF file or a glob that identifies multiple them,
    e.g. "L3_SST/**/*.nc".

    OUTPUT_PATH must be directory which will contain the Zarr result, e.g. "L3_SST.zarr".

    CONFIG_FILE has YAML format. If multiple are given, their "input", "process", and "output"
    entries are merged while other settings overwrite each other in the order they appear.
    Command line arguments overwrite settings in any CONFIG_FILE:

    \b
    [INPUT_FILE ...] overwrites /input/paths in CONFIG_FILE
    [--output OUTPUT_FILE] overwrites /output/path
    [--mode MODE] overwrites /mode
    [--dry-run] overwrites /dry_run
    [--batch SIZE] overwrites /input/batch_size (not implemented yet)
    [--decode-cf] overwrites /input/decode_cf
    """

    if version:
        from nc2zarr.version import version
        print(version)
        return 0

    if batch_size is not None:
        raise click.ClickException('option --batch is not supported yet')

    from nc2zarr.convert import convert_netcdf_to_zarr
    from nc2zarr.perf import measure_time
    with measure_time('Converting'):
        convert_netcdf_to_zarr(input_paths=input_files,
                               output_path=output_path,
                               config_paths=config_paths,
                               batch_size=batch_size,
                               mode=mode,
                               decode_cf=decode_cf,
                               verbose=verbose,
                               dry_run=dry_run,
                               exception_type=click.ClickException)


if __name__ == '__main__':
    main()

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

from nc2zarr.constants import DEFAULT_CONFIG_FILE
from nc2zarr.constants import DEFAULT_MODE
from nc2zarr.constants import DEFAULT_OUTPUT_FILE
from nc2zarr.constants import MODE_CHOICES


@click.command(name='nc2zarr')
@click.argument('input_files', nargs=-1, metavar='[INPUT_FILE ...]')
@click.option('--output', '-o', 'output_file', metavar='OUTPUT_FILE',
              help=f'Output name. Defaults to "{DEFAULT_OUTPUT_FILE}".')
@click.option('--config', '-c', 'config_file', metavar='CONFIG_FILE',
              help=f'Configuration file. Defaults to "{DEFAULT_CONFIG_FILE}".')
@click.option('--batch', '-b', 'batch_size', metavar='BATCH_SIZE', type=int,
              help=f'Batch size. If greater zero, conversion will be performed in batches of the given size.')
@click.option('--mode', '-m', 'mode', metavar='MODE', type=click.Choice(MODE_CHOICES),
              help=f'Configuration file. Must be one of {MODE_CHOICES}. Defaults to "{DEFAULT_MODE}".')
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
         output_file: str,
         config_file: str,
         batch_size: Optional[int],
         mode: str,
         decode_cf: bool,
         dry_run: bool,
         verbose: bool,
         version: bool):
    """
    Converts multiple NetCDF files to a single Zarr dataset.

    INPUT_FILE may refer to a NetCDF file or a glob that identifies multiple them, e.g. "L3_SST/**/*.nc".

    If CONFIG_FILE is given, any given NetCDF file arguments overwrite setting input/path in CONFIG_FILE .
    Accordingly the following option overwrite settings in CONFIG_FILE:

    \b
    --mode overwrites CONFIG_FILE/mode
    --dry-run overwrites CONFIG_FILE/dry_run
    --batch overwrites CONFIG_FILE/input/batch_size (not implemented yet)
    --decode-cf overwrites CONFIG_FILE/input/decode_cf
    --output overwrites CONFIG_FILE/output/path
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
                               output_path=output_file,
                               config_path=config_file,
                               batch_size=batch_size,
                               mode=mode,
                               decode_cf=decode_cf,
                               verbose=verbose,
                               dry_run=dry_run,
                               exception_type=click.ClickException)


if __name__ == '__main__':
    main()

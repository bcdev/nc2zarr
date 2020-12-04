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
from typing import List

import click

DEFAULT_OUTPUT_FILE = 'out.zarr'
DEFAULT_CONFIG_FILE = 'nc2zarr-config.yml'


@click.command()
@click.argument('input_files', nargs=-1, metavar='[INPUT_FILES ...]')
@click.option('--output', '-o', 'output_file', metavar='OUTPUT_FILE', default=DEFAULT_OUTPUT_FILE,
              help=f'Output name. Defaults to "{DEFAULT_OUTPUT_FILE}".')
@click.option('--config', '-c', 'config_file', metavar='CONFIG_FILE', default=DEFAULT_CONFIG_FILE,
              help=f'Configuration file. Defaults to "{DEFAULT_CONFIG_FILE}".')
@click.option('--verbose', '-v', is_flag=True,
              help='Print more output.')
@click.option('--version', is_flag=True,
              help='Show version number and exit.')
def nc2zarr(input_files: List[str], output_file: str, config_file: str, verbose: bool, version: bool):
    """
    Convert NetCDF files to Zarr format.

    Optional INPUT_FILES may contain wildcards, e.g. "L3_SST/*.nc".
    """

    if version:
        from nc2zarr.version import version
        print(version)
        return 0

    from .convert import convert_netcdf_to_zarr
    from .perf import measure_time
    with measure_time('Converting'):
        convert_netcdf_to_zarr(input_paths=input_files,
                               output_path=output_file,
                               config_path=config_file,
                               verbose=verbose,
                               exception_type=click.ClickException)


def main():
    nc2zarr()


if __name__ == '__main__':
    main()

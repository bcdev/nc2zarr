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

from typing import Optional

import click
import yaml

DEFAULT_CONFIG_FILE = 'fs-config.yml'


@click.group(name='fs')
@click.option('--config', '-c', 'fs_config_path', metavar='FS_CONFIG',
              help=f'Configuration file. Defaults to "{DEFAULT_CONFIG_FILE}".')
@click.option('--version', is_flag=True,
              help='Show version number and exit.')
@click.pass_context
def main(ctx, fs_config_path: Optional[str], version: bool):
    """
    CLI wrapper for fsspec.FileSystem.
    """
    if version:
        from nc2zarr.version import version
        print(version)
        raise click.exceptions.Exit(0)
    ctx.ensure_object(dict)
    if fs_config_path:
        with open(fs_config_path, 'r') as stream:
            fs_config = yaml.load(stream, Loader=yaml.SafeLoader)
            ctx.obj.update(fs_config)


@main.command(name='ls')
@click.argument('path', nargs=1, metavar='PATH')
@click.pass_context
def ls(ctx, path: str):
    """
    Copy files and directories.
    """

    import fsspec
    s3_options = ctx.obj.get('s3', {})
    local_options = ctx.obj.get('file', {})
    if path.startswith('s3://'):
        fs: fsspec.AbstractFileSystem = fsspec.filesystem('s3', **s3_options)
    else:
        fs: fsspec.AbstractFileSystem = fsspec.filesystem('file', **local_options)
    for p in fs.ls(path):
        print(p)


@main.command(name='cp')
@click.argument('from_path', nargs=1, metavar='FROM')
@click.argument('to_path', nargs=1, metavar='TO')
@click.option('--recursive', '-r', is_flag=True,
              help='Copy directories recursively.')
@click.pass_context
def cp(ctx, from_path: str, to_path: str, recursive: bool):
    """
    Copy files and directories.
    """

    import fsspec
    s3_options = ctx.obj.get('s3', {})
    local_options = ctx.obj.get('file', {})
    if from_path.startswith('s3://') and to_path.startswith('s3://'):
        fs: fsspec.AbstractFileSystem = fsspec.filesystem('s3', **s3_options)
        fs.copy(from_path, to_path, recursive=recursive)
    elif from_path.startswith('s3://'):
        fs: fsspec.AbstractFileSystem = fsspec.filesystem('s3', **s3_options)
        fs.get(from_path, to_path, recursive=recursive)
    elif to_path.startswith('s3://'):
        fs: fsspec.AbstractFileSystem = fsspec.filesystem('s3', **s3_options)
        fs.put(from_path, to_path, recursive=recursive)
    else:
        fs: fsspec.AbstractFileSystem = fsspec.filesystem('file', **local_options)
        fs.copy(from_path, to_path, recursive=recursive)


if __name__ == '__main__':
    main()

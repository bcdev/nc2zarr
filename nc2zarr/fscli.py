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

import os.path
from typing import Optional

import click
import yaml

DEFAULT_CONFIG_FILE = os.path.join('.', 'fs-config.yml')


@click.group(name='fs')
@click.option('--config', '-c', 'config_path', metavar='CONFIG',
              help=f'File systems configuration file. Defaults to "{DEFAULT_CONFIG_FILE}".')
@click.pass_context
def main(ctx, config_path: Optional[str]):
    """
    CLI wrapper for fsspec.FileSystem.
    """
    ctx.ensure_object(dict)
    ctx.obj.update(config_path=config_path)


@main.command(name='ls')
@click.argument('path', nargs=1, metavar='PATH')
@click.pass_context
def ls(ctx, path: str):
    """
    List files and directories.
    """
    import fsspec
    config = _get_config(ctx)
    s3_options = config.get('s3', {})
    local_options = config.get('file', {})
    if path.startswith('s3://'):
        fs: fsspec.AbstractFileSystem = fsspec.filesystem('s3', **s3_options)
    else:
        fs: fsspec.AbstractFileSystem = fsspec.filesystem('file', **local_options)
    for p in fs.ls(path):
        print(p)


@main.command(name='rm')
@click.argument('path', nargs=1, metavar='PATH')
@click.option('--recursive', '-r', is_flag=True,
              help='Remove directories recursively.')
@click.pass_context
def rm(ctx, path: str, recursive: bool):
    """
    Remove files and directories.
    """
    import fsspec
    config = _get_config(ctx)
    s3_options = config.get('s3', {})
    local_options = config.get('file', {})
    if path.startswith('s3://'):
        fs: fsspec.AbstractFileSystem = fsspec.filesystem('s3', **s3_options)
    else:
        fs: fsspec.AbstractFileSystem = fsspec.filesystem('file', **local_options)
    fs.rm(path, recursive=recursive)


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
    config = _get_config(ctx)
    s3_options = config.get('s3', {})
    local_options = config.get('file', {})
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


def _get_config(ctx):
    config_path = ctx.get('config_path')
    config_path = config_path or DEFAULT_CONFIG_FILE if os.path.isfile(DEFAULT_CONFIG_FILE) else None
    if config_path:
        with open(config_path, 'r') as stream:
            config = yaml.load(stream, Loader=yaml.SafeLoader)
            print(f'Configuration {config_path} loaded.')
            return config
    return {}


if __name__ == '__main__':
    main()

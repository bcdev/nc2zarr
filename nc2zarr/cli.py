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

from typing import Optional, Tuple, Dict, Any, List

import click

from nc2zarr.constants import DEFAULT_OUTPUT_APPEND_DIM_NAME
from nc2zarr.constants import DEFAULT_OUTPUT_PATH


# Important note: when adding new options, make sure their default
# value is None, otherwise the default values will override any value
# given in the configuration files.


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
@click.option('--adjust-metadata', 'adjust_metadata', is_flag=True, default=None,
              help=f'Adjust metadata attributes after the last '
                   f'write/append step.')
@click.option('--finalize-only', 'finalize_only', is_flag=True, default=None,
              help=f'Whether to just run "finalize" tasks on an existing '
                   f'output dataset. Currently, this updates the metadata only, '
                   f'given that configuration output/adjust_metadata is set or '
                   f'output/metadata is not empty. '
                   f'See also option --adjust-metadata.')
@click.option('--dry-run', '-d', 'dry_run', is_flag=True, default=None,
              help='Open and process inputs only, omit data writing.')
@click.option('--verbose', '-v', 'verbose', count=True,
              help='Print more output. Use twice for even more output.')
@click.option('--version', is_flag=True,
              help='Show version number and exit.')
def nc2zarr(
        input_paths: Tuple[str],
        output_path: str,
        config_paths: Tuple[str],
        multi_file: bool,
        concat_dim: Optional[str],
        overwrite: bool,
        append: bool,
        decode_cf: bool,
        sort_by: str,
        adjust_metadata: bool,
        finalize_only: bool,
        dry_run: bool,
        verbose: int,
        version: bool
):
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
    configurations and thus override settings in any CONFIG_FILE:

    \b
    [--finalize-only] overrides /finalize_only
    [--dry-run] overrides /dry_run
    [--verbose] overrides /verbosity

    [INPUT_FILE ...] overrides /input/paths in CONFIG_FILE
    [--multi-file] overrides /input/multi_file
    [--concat-dim] overrides /input/concat_dim
    [--decode-cf] overrides /input/decode_cf
    [--sort-by] overrides /input/sort_by

    [--output OUTPUT_FILE] overrides /output/path
    [--overwrite] overrides /output/overwrite
    [--append] overrides /output/append
    [--adjust-metadata] overrides /output/adjust_metadata
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
                                    output_adjust_metadata=adjust_metadata,
                                    finalize_only=finalize_only,
                                    verbosity=verbose if verbose else None,
                                    dry_run=dry_run)
        Converter(**config_kwargs).run()
    except ConverterError as e:
        raise click.ClickException(e) from e


if __name__ == '__main__':
    nc2zarr()


@click.command(name='nc2zarr-batch')
@click.argument('config_template_path', metavar='CONFIG_TEMPLATE_PATH')
@click.argument('config_path_template', metavar='CONFIG_PATH_TEMPLATE')
@click.option('--range', '-R', 'ranges',
              metavar='KEY MIN MAX', nargs=3, multiple=True,
              help=f'Key value range assignments. MIN and MAX must be integers. Option may be repeated.')
@click.option('--value', '-V', 'values',
              metavar='KEY VALUE', nargs=2, multiple=True,
              help=f'Key value assignments. Option may be repeated.')
@click.option('--scheduler', '-s', 'scheduler_config_path',
              metavar='FILE',
              help=f'Scheduler configuration file (YAML).')
@click.option('--dry-run', '-d', 'dry_run', is_flag=True, default=None,
              help='Open and process inputs only, omit data writing.')
@click.option('--verbose', '-v', 'verbose', count=True,
              help='Print more output. Use twice for even more output.')
def nc2zarr_batch(
        config_template_path: str,
        config_path_template: str,
        ranges: Tuple[Tuple[str]],
        values: Tuple[Tuple[str]],
        scheduler_config_path: str,
        dry_run: bool,
        verbose: int
):
    """
    Run nc2zarr in batch mode.

    Example:

    \b
        $ nc2zarr-batch \\
            --range year 2002 2017 \\
            --value base_dir . \\
            --scheduler ../slurm-config.yml \\
            seaice/config-template.yml \\
            seaice/batch/config-${year}.yml

    """
    import datetime
    import time
    import os.path
    import yaml

    from nc2zarr.batch import TemplateBatch

    if not os.path.isfile(config_template_path):
        raise click.exceptions.FileError(config_template_path, 'not found')

    for k, _, _ in ranges:
        ref = "${" + k + "}"
        if ref not in config_path_template:
            raise click.exceptions.BadArgumentUsage(f'reference "{ref}" '
                                                    f'missing in CONFIG_PATH_TEMPLATE')

    config_template_variables = expand_config_template_variables(ranges, values)

    job_config = {}
    if scheduler_config_path:
        if not os.path.isfile(scheduler_config_path):
            raise click.exceptions.FileError(scheduler_config_path, 'not found')
        with open(scheduler_config_path) as fp:
            job_config = yaml.load(fp, Loader=yaml.SafeLoader)

    job_type = job_config.pop('type', 'local')
    job_env_vars = job_config.pop('env_vars', {})
    job_cwd_path = job_config.get('cwd_path', os.path.curdir)

    if job_env_vars:
        environ = dict(os.environ)
        environ.update(job_env_vars)
        job_env_vars = environ
    python_path_ex = os.path.dirname(config_template_path) or '.'
    python_path = job_env_vars.get('PYTHONPATH')
    if python_path:
        job_env_vars['PYTHONPATH'] = os.path.pathsep.join([python_path, python_path_ex])
    else:
        job_env_vars['PYTHONPATH'] = python_path_ex

    batch = TemplateBatch(config_template_variables,
                          config_template_path,
                          config_path_template,
                          dry_run=dry_run,
                          verbosity=verbose)

    jobs = batch.execute(nc2zarr_args=['-' + (max(1, verbose) * 'v')],
                         job_type=job_type,
                         job_env_vars=job_env_vars,
                         job_cwd_path=job_cwd_path,
                         **job_config)

    while True:
        print(f'\n{datetime.datetime.now()}:')
        for values, job in zip(config_template_variables, jobs):
            print(f'  {values}: {job.status}')
        if all([job.done for job in jobs]):
            break
        time.sleep(2.0)


def expand_config_template_variables(key_min_max_tuples: Tuple[Tuple[str, ...], ...],
                                     key_value_tuples: Tuple[Tuple[str, ...], ...]) -> List[Dict[str, Any]]:
    """
    Helper that computes a list of key-value dictionaries
    from all value-combinations of key-min-max and key-value
    assignments.
    """
    import itertools
    keys = [k for k, _, _ in key_min_max_tuples] + [k for k, _ in key_value_tuples]
    # noinspection PyTypeChecker
    values_iterators = [(range(int(min_value), int(max_value) + 1))
                        for _, min_value, max_value in key_min_max_tuples] \
                       + [(value,) for _, value in key_value_tuples]
    if not values_iterators:
        return []
    return [{k: v for k, v in zip(keys, values)}
            for values in itertools.product(*values_iterators)]

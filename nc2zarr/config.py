# The MIT License (MIT)
# Copyright (c) 2020 by Brockmann Consult GmbH and contributors
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

import yaml

from .error import ConverterError
from .log import LOGGER


# noinspection PyUnusedLocal
def load_config(config_paths: Union[str, Sequence[str]] = None,
                return_kwargs: bool = False,
                **kwargs) -> Dict[str, Any]:
    """
    Create a new configuration.

    :param config_paths:
    :param return_kwargs:
    :param kwargs: see nc2zarr.convert.convert_netcdf_to_zarr
    :raise ConverterError
    """
    arg_config = dict()
    arg_input = dict()
    arg_process = dict()
    arg_output = dict()
    for k, v in kwargs.items():
        if v is not None:
            if k.startswith('input_'):
                arg_input[k[len('input_'):]] = v
            elif k.startswith('process_'):
                arg_process[k[len('process_'):]] = v
            elif k.startswith('output_'):
                arg_output[k[len('output_'):]] = v
            else:
                arg_config[k] = v

    if arg_input:
        arg_config['input'] = arg_input
    if arg_process:
        arg_config['process'] = arg_process
    if arg_output:
        arg_config['output'] = arg_output

    if config_paths:
        config_paths = [config_paths] if isinstance(config_paths, str) else config_paths
        configs = [_load_config(config_path)
                   for config_path in config_paths] + [arg_config]
        config = _merge_configs(configs)
    else:
        config = arg_config

    return config_to_kwargs(config) if return_kwargs else config


def config_to_kwargs(config: Dict[str, Any]) -> Dict[str, Any]:
    config = dict(config)
    input_params = config.pop('input') if 'input' in config else {}
    process_params = config.pop('process') if 'process' in config else {}
    output_params = config.pop('output') if 'output' in config else {}
    return dict(**{'input_' + k: v for k, v in input_params.items()},
                **{'process_' + k: v for k, v in process_params.items()},
                **{'output_' + k: v for k, v in output_params.items()},
                **config)


def _load_config(path: str) -> Dict[str, Any]:
    try:
        with open(path) as fp:
            config = yaml.load(fp, Loader=yaml.SafeLoader)
            LOGGER.info(f'Configuration {path} loaded.')
        return config
    except FileNotFoundError as e:
        raise ConverterError(f'Configuration not found: {path}') from e


def _merge_configs(configs: List[Dict[str, Any]]) -> Dict[str, Any]:
    effective_config = dict()
    for config in configs:
        effective_config = _merge_2_configs(effective_config, config)
    return effective_config


def _merge_2_configs(config_1: Dict[str, Any], config_2: Dict[str, Any]) -> Dict[str, Any]:
    effective_config = dict(config_1)
    for k, v2 in config_2.items():
        if k in effective_config:
            v1 = config_1[k]
            if isinstance(v1, dict) and isinstance(v2, dict):
                effective_config[k] = _merge_2_configs(v1, v2)
            elif isinstance(v1, list) and isinstance(v2, list):
                effective_config[k] = v1 + v2
            else:
                effective_config[k] = v2
        else:
            effective_config[k] = v2
    return effective_config

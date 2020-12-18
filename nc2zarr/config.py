from typing import Sequence, Union, Any, Dict, List

import yaml

from .error import ConverterError
from .log import LOGGER


# noinspection PyUnusedLocal
def load_config(config_paths: Union[str, Sequence[str]] = None,
                input_paths: Union[str, Sequence[str]] = None,
                input_decode_cf: bool = False,
                input_multi_file: bool = False,
                input_concat_dim: str = None,
                output_path: str = None,
                output_overwrite: bool = False,
                output_append: bool = False,
                dry_run: bool = False,
                verbosity: int = None) -> Dict[str, Any]:
    """
    Create a new configuration.

    :param input_paths:
    :param output_path:
    :param config_paths:
    :param input_multi_file:
    :param input_concat_dim:
    :param output_overwrite:
    :param output_append:
    :param input_decode_cf:
    :param dry_run:
    :param verbosity:
    :raise ConverterError
    """

    arg_config = dict(input=dict(), process=dict(), output=dict())
    if dry_run:
        arg_config['dry_run'] = True
    if verbosity:
        arg_config['verbosity'] = True
    if input_paths:
        arg_config['input']['paths'] = input_paths
    if input_multi_file:
        arg_config['input']['multi_file'] = True
    if input_concat_dim:
        arg_config['input']['concat_dim'] = input_concat_dim
    if input_decode_cf:
        arg_config['input']['decode_cf'] = True
    if output_path:
        arg_config['output']['path'] = output_path
    if output_overwrite:
        arg_config['output']['overwrite'] = True
    if output_append:
        arg_config['output']['append'] = True

    if not config_paths:
        return arg_config

    config_paths = [config_paths] if isinstance(config_paths, str) else config_paths
    configs = [_load_config(config_path)
               for config_path in config_paths] + [arg_config]
    return _merge_configs(configs)


def _load_config(path: str) -> Dict[str, Any]:
    try:
        with open(path) as fp:
            config = yaml.load(fp, Loader=yaml.SafeLoader)
            LOGGER.info(f'Configuration {path} loaded.')
        return config
    except FileNotFoundError as e:
        raise ConverterError(f'{path} not found.') from e


def _merge_configs(configs: List[Dict[str, Any]]) -> Dict[str, Any]:
    effective_config = dict()
    for config in configs:
        effective_config.update(**config)
    for config in configs:
        for k in ('input', 'process', 'output'):
            if k in config:
                if k not in effective_config:
                    effective_config[k] = dict()
                effective_config[k].update(**config[k])
    return effective_config

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
import importlib
import importlib.util
from typing import Callable


def load_custom_func(func_ref: str) -> Callable:
    module_name, func_name = '', ''
    if isinstance(func_ref, str):
        func_ref_parts = func_ref.rsplit(':', maxsplit=1)
        if len(func_ref_parts) == 2:
            module_name, func_name = func_ref_parts
    if not module_name or not func_name:
        raise ValueError(f'func_ref "{func_ref}" is invalid,'
                         f' format must be <module>:<function>')
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        raise ValueError(f'module for function "{func_ref}" not found')
    obj = module
    for attr_name in func_name.split('.'):
        if not attr_name.isidentifier():
            raise ValueError(f'func_ref "{func_ref}" is invalid,'
                             f' "{attr_name}" is not a valid identifier')
        try:
            obj = getattr(obj, attr_name)
        except AttributeError:
            raise ValueError(f'function "{func_ref}" not found,'
                             f' unknown attribute "{attr_name}"')
    if not callable(obj):
        raise ValueError(f'"{func_ref}" is not callable')
    return obj

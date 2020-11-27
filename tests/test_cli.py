import unittest
from typing import List

import click
import click.testing

from nc2zarr.cli import nc2zarr


class MainTest(unittest.TestCase):
    def test_noop(self):
        self._invoke_cli([])

    def test_version(self):
        self._invoke_cli(['--version'])

    def _invoke_cli(self, args: List[str]):
        self.runner = click.testing.CliRunner()
        return self.runner.invoke(nc2zarr, args, catch_exceptions=False)

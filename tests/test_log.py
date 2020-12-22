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

import logging
import time
import unittest

from nc2zarr.log import LOGGER
from nc2zarr.log import get_verbosity
from nc2zarr.log import log_duration
from nc2zarr.log import use_verbosity


class LogTest(unittest.TestCase):

    def test_LOGGER(self):
        self.assertIsNotNone(LOGGER)
        self.assertEqual(logging.WARNING, LOGGER.level)

    def test_get_and_use_verbosity(self):
        self.assertEqual(0, get_verbosity())

        with use_verbosity(2):
            self.assertEqual(2, get_verbosity())
            self.assertEqual(logging.DEBUG, LOGGER.level)
        self.assertEqual(0, get_verbosity())

        with use_verbosity(0):
            self.assertEqual(0, get_verbosity())
            self.assertEqual(logging.WARNING, LOGGER.level)
        self.assertEqual(0, get_verbosity())

        with use_verbosity(1):
            self.assertEqual(1, get_verbosity())
            self.assertEqual(logging.INFO, LOGGER.level)
        self.assertEqual(0, get_verbosity())

    def test_log_duration(self):
        with use_verbosity(1):
            with log_duration('Waiting') as cm:
                time.sleep(0.05)
                time.sleep(0.05)
            self.assertTrue(cm.duration >= 0.05)

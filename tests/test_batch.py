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

import os.path
import unittest
import time
from nc2zarr.batch import LocalJob
from nc2zarr.batch import write_config_files
from tests.helpers import IOCollector

TEST_DIR = os.path.dirname(__file__)


class LocalJobTest(unittest.TestCase):
    OUT_PATH = os.path.join(TEST_DIR, "local.out")
    ERR_PATH = os.path.join(TEST_DIR, "local.err")

    io_collector = IOCollector()

    def setUp(self) -> None:
        self.io_collector.reset_paths()
        self.io_collector.add_path(self.OUT_PATH)
        self.io_collector.add_path(self.ERR_PATH)

    def tearDown(self) -> None:
        self.io_collector.delete_paths()

    def test_local_job_ok(self):
        job = LocalJob.submit_job(['nc2zarr', '--help'], self.OUT_PATH, self.ERR_PATH)
        while job.is_running():
            time.sleep(0.1)
        self.assertIsInstance(job, LocalJob)


class WriteConfigFilesTest(unittest.TestCase):
    CONFIG_TEMPLATE_PATH = os.path.join(TEST_DIR, 'ghg-template.yml')

    CONFIG_DIR = os.path.join(TEST_DIR, '_ghg')
    CONFIG_PATH_TEMPLATE = os.path.join(CONFIG_DIR, 'ghg-${year}.yml')

    io_collector = IOCollector()

    def setUp(self) -> None:
        self.io_collector.reset_paths()
        self.io_collector.add_path(self.CONFIG_TEMPLATE_PATH)
        self.io_collector.add_path(self.CONFIG_DIR)

    def tearDown(self) -> None:
        self.io_collector.delete_paths()

    def test_write_config_files(self):
        ghg_config_template = (
            'input:'
            '  paths:'
            '    - "/neodc/esacci/ghg/data/crdp_4/SCIAMACHY/CO2_SCI_WFMD/v4.0/${year}/ESACCI-GHG-L2-CO2-SCIAMACHY-WFMD-*-fv1.nc"'
            '  sort_by: "name"'
            'output:'
            '  path: "./data/${year}-ESACCI-GHG-L2-CO2-SCIAMACHY-WFMD-fv1.zarr/"'
        )

        with open(self.CONFIG_TEMPLATE_PATH, 'w') as fp:
            fp.write(ghg_config_template)

        self.assertFalse(os.path.exists(self.CONFIG_DIR))

        paths = write_config_files(self.CONFIG_TEMPLATE_PATH,
                                   self.CONFIG_PATH_TEMPLATE,
                                   [dict(year=year) for year in range(2008, 2012 + 1)])

        expected_files = [('ghg-2008.yml', 'ghg-2008.out', 'ghg-2008.err'),
                          ('ghg-2009.yml', 'ghg-2009.out', 'ghg-2009.err'),
                          ('ghg-2010.yml', 'ghg-2010.out', 'ghg-2010.err'),
                          ('ghg-2011.yml', 'ghg-2011.out', 'ghg-2011.err'),
                          ('ghg-2012.yml', 'ghg-2012.out', 'ghg-2012.err')]
        self.assertEqual([tuple(map(lambda f: os.path.join(self.CONFIG_DIR, f), t))
                          for t in expected_files],
                         paths)

        self.assertTrue(os.path.isdir(self.CONFIG_DIR))
        self.assertEqual([t[0] for t in expected_files],
                         sorted(os.listdir(self.CONFIG_DIR)))

        with open(os.path.join(self.CONFIG_DIR, 'ghg-2012.yml'), 'r') as fp:
            ghg_config_2012 = fp.read()

        self.assertEqual((
            'input:'
            '  paths:'
            '    - "/neodc/esacci/ghg/data/crdp_4/SCIAMACHY/CO2_SCI_WFMD/v4.0/2012/ESACCI-GHG-L2-CO2-SCIAMACHY-WFMD-*-fv1.nc"'
            '  sort_by: "name"'
            'output:'
            '  path: "./data/2012-ESACCI-GHG-L2-CO2-SCIAMACHY-WFMD-fv1.zarr/"'
        ), ghg_config_2012)

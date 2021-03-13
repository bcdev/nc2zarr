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

import os
import os.path
import stat
import sys
import time
import unittest

from nc2zarr.batch import DryRunJob
from nc2zarr.batch import LocalJob
from nc2zarr.batch import SlurmJob
from nc2zarr.batch import TemplateBatch
from tests.helpers import IOCollector

TEST_DIR = os.path.dirname(__file__)


class TemplateBatchTest(unittest.TestCase):
    CONFIG_TEMPLATE_PATH = os.path.join(TEST_DIR, 'ghg-template.yml')

    CONFIG_DIR = os.path.join(TEST_DIR, '_ghg')
    CONFIG_PATH_TEMPLATE = os.path.join(CONFIG_DIR, 'ghg-${year}.yml')

    io_collector = IOCollector()

    CONFIG_TEMPLATE_TEXT = (
        'input:\n'
        '  paths:\n'
        '    - "${input_dir}/${year}/ESACCI-GHG-L2-CO2-SCIAMACHY-WFMD-*-fv1.nc"\n'
        '  sort_by: "name"\n'
        'output:\n'
        '  path: "${output_dir}/${year}-ESACCI-GHG-L2-CO2-SCIAMACHY-WFMD-fv1.zarr/"\n'
    )

    CONFIG_VARIABLES = [
        dict(year=year,
             input_dir='/neodc/esacci/ghg/data/crdp_4/SCIAMACHY/CO2_SCI_WFMD/v4.0',
             output_dir='./data')
        for year in range(2008, 2012 + 1)
    ]

    def setUp(self) -> None:
        self.io_collector.reset_paths()
        self.io_collector.add_path(self.CONFIG_TEMPLATE_PATH)
        self.io_collector.add_path(self.CONFIG_DIR)

        with open(self.CONFIG_TEMPLATE_PATH, 'w') as fp:
            fp.write(self.CONFIG_TEMPLATE_TEXT)

    def tearDown(self) -> None:
        self.io_collector.delete_paths()

    def test_execute_dry_run(self):
        batch = TemplateBatch(self.CONFIG_VARIABLES,
                              self.CONFIG_TEMPLATE_PATH,
                              self.CONFIG_PATH_TEMPLATE, dry_run=True)
        jobs = batch.execute()
        self.assertIsInstance(jobs, list)
        self.assertEqual(5, len(jobs))
        for job in jobs:
            self.assertIsInstance(job, DryRunJob)
            self.assertFalse(job.is_running)

    def test_execute_dry_run_illegal_job_type(self):
        batch = TemplateBatch(self.CONFIG_VARIABLES,
                              self.CONFIG_TEMPLATE_PATH,
                              self.CONFIG_PATH_TEMPLATE, dry_run=True)
        with self.assertRaises(ValueError) as cm:
            batch.execute(job_type='pippo')
        self.assertEqual('illegal job_type "pippo"', f'{cm.exception}')

    def test_write_config_files(self):
        batch = TemplateBatch(self.CONFIG_VARIABLES,
                              self.CONFIG_TEMPLATE_PATH,
                              self.CONFIG_PATH_TEMPLATE)
        paths = batch.write_config_files()

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
            'input:\n'
            '  paths:\n'
            '    - "/neodc/esacci/ghg/data/crdp_4/SCIAMACHY/CO2_SCI_WFMD/v4.0/'
            '2012/ESACCI-GHG-L2-CO2-SCIAMACHY-WFMD-*-fv1.nc"\n'
            '  sort_by: "name"\n'
            'output:\n'
            '  path: "./data/2012-ESACCI-GHG-L2-CO2-SCIAMACHY-WFMD-fv1.zarr/"\n'
        ), ghg_config_2012)


JOB_OUT_PATH = "_job_test.out"
JOB_ERR_PATH = "_job_test.err"


# noinspection PyPep8Naming
class BatchJobTest(unittest.TestCase):
    io_collector = IOCollector()

    def setUp(self) -> None:
        self.io_collector.reset_paths()
        self.io_collector.add_path(JOB_OUT_PATH)
        self.io_collector.add_path(JOB_ERR_PATH)

    def tearDown(self) -> None:
        self.io_collector.delete_paths()


class DryRunJobTest(BatchJobTest):

    def test_job_ok(self):
        job = DryRunJob.submit_job(['nc2zarr', '--help'], JOB_OUT_PATH, JOB_ERR_PATH)
        self.assertIsInstance(job, DryRunJob)
        while job.is_running:
            time.sleep(0.1)


class LocalJobTest(BatchJobTest):

    def test_job_ok(self):
        job = LocalJob.submit_job(['nc2zarr', '--help'],
                                  JOB_OUT_PATH,
                                  JOB_ERR_PATH,
                                  env_vars=dict(TEST='42'),
                                  cwd_path='.')
        self.assertIsInstance(job, LocalJob)
        while job.is_running:
            time.sleep(0.1)


SBATCH_MOCK_WIN32 = 'sbatch-mock.bat'
SBATCH_MOCK_UNIX = './sbatch-mock.sh'


class SlurmJobTest(BatchJobTest):

    def setUp(self) -> None:
        super().setUp()
        if sys.platform == 'win32':
            self.sbatch_program = SBATCH_MOCK_WIN32
        else:
            self.sbatch_program = SBATCH_MOCK_UNIX
        self.io_collector.add_path(self.sbatch_program)

    def _write_sbatch_exe(self, unix_content: str, win32_content: str):
        if sys.platform == 'win32':
            with open(self.sbatch_program, 'w') as fp:
                fp.write(win32_content)
        else:
            with open(self.sbatch_program, 'w') as fp:
                fp.write(unix_content)
            st = os.stat(self.sbatch_program)
            os.chmod(self.sbatch_program, st.st_mode | stat.S_IEXEC)


class SlurmJobSuccessTest(SlurmJobTest):

    def setUp(self) -> None:
        super().setUp()
        self._write_sbatch_exe('#!/bin/sh\n'
                               'echo Submitted batch job 137',
                               '@echo Submitted batch job 137')

    def test_job_ok(self):
        job = SlurmJob.submit_job(['nc2zarr', '--help'], JOB_OUT_PATH, JOB_ERR_PATH, cwd_path='.',
                                  env_vars=dict(TEST=123), partition='short-serial', duration='02:00:00',
                                  sbatch_program=self.sbatch_program)
        self.assertEquals('137', job.job_id)
        self.assertIsInstance(job, SlurmJob)
        while job.is_running:
            time.sleep(0.1)


class SlurmJobFailureTest(SlurmJobTest):

    def setUp(self) -> None:
        super().setUp()
        self._write_sbatch_exe('#!/bin/sh\n'
                               'exit 2',
                               '@exit /B 2')

    def test_job_fails(self):
        with self.assertRaises(EnvironmentError) as cm:
            SlurmJob.submit_job(['nc2zarr', '--help'], JOB_OUT_PATH, JOB_ERR_PATH, cwd_path='.',
                                env_vars=dict(TEST=123), partition='short-serial', duration='02:00:00',
                                sbatch_program=self.sbatch_program)
        print(f'{cm.exception}')
        self.assertEqual(f"Slurm job submission failed for command line:"
                         f" {self.sbatch_program}"
                         f" -o _job_test.out"
                         f" -e _job_test.err"
                         f" --partition=short-serial"
                         f" --time=02:00:00"
                         f" --chdir=."
                         f" --export=TEST=123"
                         f" nc2zarr --help",
                         f'{cm.exception}')

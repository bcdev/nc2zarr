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

import os.path
import subprocess
import threading
import time
from abc import ABC, abstractmethod
from io import StringIO
from typing import Dict, List, Any, Sequence, Tuple, Optional, TextIO, Type

from .log import LOGGER


class TemplateBatch:

    def __init__(self,
                 config_template_path: str,
                 config_path_template: str,
                 variables: Sequence[Dict[str, Any]],
                 create_parents: bool = True,
                 dry_run: bool = False):
        self._config_template_path = config_template_path
        self._config_path_template = config_path_template
        self._variables = variables
        self._create_parents = create_parents
        self._dry_run = dry_run

    def execute(self,
                nc2zarr_args: List[str] = None,
                job_type: str = 'local',
                exports: Dict[str, str] = None,
                directory: str = None,
                job_kwargs: Dict = None) -> List['BatchJob']:

        job_class = self._get_job_class(job_type)

        config_paths = self.write_config_files()

        jobs = []
        for config_path, out_path, err_path in config_paths:
            command = ['nc2zarr'] + (nc2zarr_args or []) + ['-c', config_path]
            job = job_class.submit_job(command,
                                       out_path,
                                       err_path,
                                       exports=exports,
                                       directory=directory,
                                       **(job_kwargs or {}))
            jobs.append(job)
        return jobs

    def write_config_files(self) -> List[Tuple[str, str, str]]:
        with open(self._config_template_path, 'r') as fp:
            config_template = fp.read()
        paths = []
        for mapping in self._variables:
            config = config_template
            config_path = self._config_path_template
            for k, v in mapping.items():
                k = '${' + k + '}'
                v = f'{v}'
                config = config.replace(k, v)
                config_path = config_path.replace(k, v)
            if self._create_parents:
                config_dir = os.path.dirname(config_path)
                if not os.path.exists(config_dir):
                    if not self._dry_run:
                        os.makedirs(config_dir)
                    else:
                        LOGGER.warning(f'Dry run: skipped creating parent {config_dir}')
            if not self._dry_run:
                with open(config_path, 'w') as fp:
                    fp.write(config)
            else:
                LOGGER.warning(f'Dry run: skipped writing {config_path}')
            config_path_base, _ = os.path.splitext(config_path)
            paths.append((config_path,
                          config_path_base + '.out',
                          config_path_base + '.err'))
        return paths

    def _get_job_class(self, job_type) -> Type['BatchJob']:
        job_class_registry: Dict[str, Type[BatchJob]] = {
            'local': LocalJob,
            'slurm': SlurmJob,
            'dry_run': DryRunJob,
        }
        if job_type not in job_class_registry:
            raise ValueError(f'illegal job_type "{job_type}"')
        if self._dry_run:
            job_type = 'dry_run'
        return job_class_registry[job_type]


class BatchJob(ABC):
    @classmethod
    @abstractmethod
    def submit_job(cls,
                   command: List[str],
                   out_path: str,
                   err_path: str,
                   exports: Dict[str, str] = None,
                   directory: str = None,
                   **kwargs) -> 'BatchJob':
        """Create a new batch job."""

    @property
    @abstractmethod
    def is_running(self) -> bool:
        """Check whether job is still running."""


class DryRunJob(BatchJob):

    @classmethod
    def submit_job(cls, command: List[str], *args, **kwargs) -> 'DryRunJob':
        LOGGER.warning(f'Dry run: job not submitted for'
                       f' command={command!r}, args={args!r}, kwargs={kwargs!r}')
        return DryRunJob()

    @property
    def is_running(self) -> bool:
        return False


class LocalJob(BatchJob):

    def __init__(self, process: subprocess.Popen, stdout: TextIO, stderr: TextIO):
        self._process: subprocess.Popen = process
        self._stdout: Optional[TextIO] = stdout
        self._stderr: Optional[TextIO] = stderr
        self._exit_code = None
        # TODO: use a single observer thread for all jobs
        self._observer = threading.Thread(target=self._observe)
        self._observer.start()

    @classmethod
    def submit_job(cls,
                   command: List[str],
                   out_path: str,
                   err_path: str,
                   exports: Dict[str, str] = None,
                   directory: str = None,
                   **subprocess_kwargs) -> 'LocalJob':
        if exports is not None:
            subprocess_kwargs.update(env=exports)
        if directory is not None:
            subprocess_kwargs.update(cwd=directory)
        stdout = open(out_path, 'w')
        stderr = open(err_path, 'w')
        subprocess_kwargs.update(stdout=stdout, stderr=stderr)
        # noinspection PyBroadException
        try:
            process = subprocess.Popen(command, **subprocess_kwargs)
        except BaseException:
            stdout.close()
            stderr.close()
            raise
        return LocalJob(process, stdout, stderr)

    @property
    def exit_code(self) -> Optional[int]:
        return self._exit_code

    @property
    def is_running(self) -> bool:
        return self._exit_code is None

    def _observe(self):
        while True:
            exit_code = self._process.poll()
            if exit_code is not None:
                self._observer = None
                self._stdout.close()
                self._stderr.close()
                self._exit_code = exit_code
                break
            time.sleep(0.5)


class SlurmJob(BatchJob):
    def __init__(self, job_id):
        self.job_id = job_id

    @classmethod
    def submit_job(cls,
                   command: List[str],
                   out_path: str,
                   err_path: str,
                   exports: Dict[str, str] = None,
                   directory: str = None,
                   partition: str = None,
                   duration: str = None) -> 'SlurmJob':

        sbatch_command = ['sbatch', '-o', out_path, '-e', err_path]
        if partition:
            sbatch_command += [f'--partition={partition}']
        if duration:
            sbatch_command += [f'--time={duration}']
        if directory:
            sbatch_command += [f'--chdir={directory}']
        if exports:
            sbatch_command += [f'--export={",".join(f"{k}={repr(v)}" for k, v in exports.items())}']
        sbatch_command += command

        stdout = StringIO()
        stderr = StringIO()
        exit_code = subprocess.call(sbatch_command, stdout=stdout, stderr=stderr)
        if exit_code != 0:
            with open(out_path, 'w') as out:
                out.write('')
            with open(err_path, 'w') as err:
                err.write(stderr.read())
            raise EnvironmentError(f'Slurm job submission failed for {sbatch_command}')
        output = str(stdout.read())
        print(f'output = [{output}]')
        prefix = 'Submitted batch job '
        if not output.startswith(prefix):
            raise EnvironmentError(f'Cannot obtain Slurm job ID from output "{output}"')
        job_id = output[len(prefix):]
        return SlurmJob(job_id)

    @property
    def is_running(self) -> bool:
        # TODO
        return True

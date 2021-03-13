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
from typing import Dict, List, Any, Sequence, Tuple, Optional, TextIO, Type

from .log import LOGGER


class TemplateBatch:
    """
    Create and optionally execute a series of nc2zarr jobs where each job's
    configuration is generated from the sequence *config_template_variables*
    and the given templates.

    :param config_template_variables: A sequence of dictionaries comprising template
        variables for the configuration templates. Each dictionary
        is used to creates a new nc2zarr configuration.
    :param config_template_path: Path to configuration file that serves as template
        for multiple configuration files to be generated. The configuration
        may contain placeholders of the form "${variable_name}" that will be
        interpolated by every dictionary in *config_template_variables*.
    :param config_path_template: Path template for the configuration files
        to be generated. The path
        may contain placeholders of the form "${variable_name}" that will be
        interpolated by every dictionary in *config_template_variables*.
    :param create_parents: Whether to create the parent directories given
        by *config_path_template* when they do not exist.
    :param dry_run: If true, any actions that would have been performed are
        logged. No files are generated and no actual processes are spawned.
    """

    def __init__(self,
                 config_template_variables: Sequence[Dict[str, Any]],
                 config_template_path: str,
                 config_path_template: str,
                 create_parents: bool = True,
                 dry_run: bool = False):
        self._config_template_path = config_template_path
        self._config_path_template = config_path_template
        self._variables = config_template_variables
        self._create_parents = create_parents
        self._dry_run = dry_run

    def execute(self,
                nc2zarr_args: List[str] = None,
                job_type: str = None,
                job_cwd_path: str = None,
                job_env_vars: Dict[str, str] = None,
                **job_params) -> List['BatchJob']:
        """
        Generate configurations and execute them.
        Return each execution as job of type :class:BatchJob.

        The method does not block.

        :param nc2zarr_args: nc2zarr extra arguments
        :param job_type: job type, "local" or "slurm", defaults to "local".
        :param job_env_vars: exported environment for the jobs.
        :param job_cwd_path: working directory for the jobs.
        :param job_params: special job arguments depending on *job_type*.
        :return: list of jobs created.
        """

        job_class = self._get_job_class(job_type or 'local')

        config_paths = self.write_config_files()

        jobs = []
        for config_path, out_path, err_path in config_paths:
            command = ['nc2zarr'] + (nc2zarr_args or []) + ['-c', config_path]
            job = job_class.submit_job(command,
                                       out_path,
                                       err_path,
                                       cwd_path=job_cwd_path,
                                       env_vars=job_env_vars,
                                       **job_params)
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
                   *,
                   cwd_path: str = None,
                   env_vars: Dict[str, str] = None, **kwargs) -> 'BatchJob':
        """Create a new batch job."""

    @property
    @abstractmethod
    def is_running(self) -> bool:
        """Check whether job is still running."""


class DryRunJob(BatchJob):
    """Does nothing but logging job information."""

    @classmethod
    def submit_job(cls,
                   command: List[str],
                   out_path: str,
                   err_path: str,
                   **job_params: str) -> 'DryRunJob':
        LOGGER.warning(f'Dry run: job not submitted for'
                       f' command={command!r},'
                       f' out_path={out_path!r},'
                       f' err_path={err_path!r},'
                       f' job_params={job_params!r}')
        return DryRunJob()

    @property
    def is_running(self) -> bool:
        return False


class LocalJob(BatchJob):
    """A job performed as a local OS process."""

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
                   *,
                   cwd_path: str = None,
                   env_vars: Dict[str, str] = None,
                   **subprocess_kwargs) -> 'LocalJob':
        if env_vars is not None:
            env = dict(os.environ)
            env.update(**env_vars)
            subprocess_kwargs.update(env=env)
        if cwd_path is not None:
            subprocess_kwargs.update(cwd=cwd_path)
        stdout = open(out_path, 'w')
        stderr = open(err_path, 'w')
        subprocess_kwargs.update(stdout=stdout, stderr=stderr)
        LOGGER.info(f'Executing command: {subprocess.list2cmdline(command)}')
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
    """A job performed by the SLURM client 'sbatch'."""

    def __init__(self, job_id):
        self._job_id = job_id

    @classmethod
    def submit_job(cls,
                   command: List[str],
                   out_path: str,
                   err_path: str,
                   *,
                   cwd_path: str = None,
                   env_vars: Dict[str, Any] = None,
                   partition: str = None,
                   duration: str = None,
                   sbatch_program: str = None,
                   **kwargs: str) -> 'SlurmJob':

        sbatch_command = [sbatch_program or 'sbatch', '-o', out_path, '-e', err_path]
        if partition:
            sbatch_command += [f'--partition={partition}']
        if duration:
            sbatch_command += [f'--time={duration}']
        if cwd_path:
            sbatch_command += [f'--chdir={cwd_path}']
        if env_vars:
            export = ",".join(f"{k}={repr(v)}" for k, v in env_vars.items())
            sbatch_command += [f'--export={export}']
        sbatch_command += command

        command_line = subprocess.list2cmdline(sbatch_command)

        LOGGER.warning(f'Executing command: {command_line}')
        result = subprocess.run(sbatch_command, capture_output=True)
        if result.returncode != 0:
            with open(out_path, 'wb') as out:
                out.write(result.stdout)
            with open(err_path, 'wb') as err:
                err.write(result.stderr)
            raise EnvironmentError(f'Slurm job submission failed for'
                                   f' command line: {command_line}')

        prefix = b'Submitted batch job '
        output = result.stdout
        for line in [l.strip() for l in output.split(b'\n')]:
            if line.startswith(prefix):
                job_id = line[len(prefix):].decode('utf-8')
                return SlurmJob(job_id)
        raise EnvironmentError(f'Cannot obtain Slurm job ID from command line:'
                               f' {command_line}: output was: "{output}"')

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def is_running(self) -> bool:
        # TODO
        return False

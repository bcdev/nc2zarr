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

from .log import LOGGER, log_duration, use_verbosity


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
                 dry_run: bool = False,
                 verbosity: int = 0):
        self._config_template_path = config_template_path
        self._config_path_template = config_path_template
        self._variables = config_template_variables
        self._create_parents = create_parents
        self._dry_run = dry_run
        self._verbosity = verbosity

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
        with use_verbosity(self._verbosity or 0):
            with log_duration('Executing jobs'):
                return self._execute(nc2zarr_args,
                                     job_type=job_type,
                                     job_cwd_path=job_cwd_path,
                                     job_env_vars=job_env_vars,
                                     **job_params)

    def _execute(self,
                 nc2zarr_args: List[str] = None,
                 job_type: str = None,
                 job_cwd_path: str = None,
                 job_env_vars: Dict[str, str] = None,
                 **job_params) -> List['BatchJob']:
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
        with use_verbosity(self._verbosity or 0):
            with log_duration('Writing job config files'):
                return self._write_config_files()

    def _write_config_files(self) -> List[Tuple[str, str, str]]:
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


class JobStatus:
    """
    A job's status.
    """

    # The job is waiting in a queue for allocation of resources
    PENDING = None
    # The job currently is allocated to a node and is running
    RUNNING = None
    # The job is finishing but some processes are still active
    COMPLETING = None
    # The job has completed successfully
    COMPLETED = None
    # Failed with non-zero exit value
    FAILED = None
    # Job terminated by the scheduler after reaching its runtime limit
    TERMINATED = None
    # A running job has been stopped with its resources released to other jobs
    SUSPENDED = None
    # A running job has been stopped with its resources retained
    STOPPED = None
    # The job status can not be determined
    UNKNOWN = None

    def __new__(cls, status_id: str):
        if not isinstance(status_id, str):
            raise TypeError(f'invalid status_id: {status_id!r}')
        try:
            status = getattr(cls, status_id.upper())
            if status is not None:
                return status
        except AttributeError:
            raise ValueError(f'invalid status_id: {status_id!r}')
        return super(JobStatus, cls).__new__(cls)

    def __init__(self, status_id: str):
        self._status_id = status_id

    def __str__(self):
        return self._status_id

    def __repr__(self):
        return f'JobStatus({self._status_id!r})'

    def __hash__(self):
        return hash(self._status_id)

    def __eq__(self, other):
        return self is other or self._status_id == other


JobStatus.PENDING = JobStatus("Pending")
JobStatus.RUNNING = JobStatus("Running")
JobStatus.COMPLETING = JobStatus("Completing")
JobStatus.COMPLETED = JobStatus("Completed")
JobStatus.FAILED = JobStatus("Failed")
JobStatus.TERMINATED = JobStatus("Terminated")
JobStatus.SUSPENDED = JobStatus("Suspended")
JobStatus.STOPPED = JobStatus("Stopped")
JobStatus.UNKNOWN = JobStatus("Unknown")


class BatchJob(ABC):
    """Abstract base class for batch jobs."""

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
    def status(self) -> JobStatus:
        """Return the job's current status."""

    @property
    def done(self) -> Optional[bool]:
        if self.status is JobStatus.UNKNOWN:
            return None
        return self.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.TERMINATED)


class DryRunJob(BatchJob):
    """Does nothing but logging job information."""

    @classmethod
    def submit_job(cls,
                   command: List[str],
                   out_path: str,
                   err_path: str,
                   poll_period: float = None,
                   **job_params: str) -> 'DryRunJob':
        LOGGER.warning(f'Dry run: job not submitted for'
                       f' command={command!r},'
                       f' out_path={out_path!r},'
                       f' err_path={err_path!r},'
                       f' job_params={job_params!r}')
        return DryRunJob()

    @property
    def status(self) -> JobStatus:
        return JobStatus.COMPLETED


class ObservedBatchJob(BatchJob, ABC):
    """An abstract base class for jobs that are observed by polling."""

    def __init__(self, command_line: str, poll_period: float = None):
        self._command_line = command_line
        self._poll_period: float = poll_period or 1.0
        self._state: Optional[Dict[str, Any]] = None
        self._observing: bool = False
        # TODO: use a single observer thread for all ObservedBatchJob instances
        self._observer = threading.Thread(target=self._observe)

    def start_observation(self):
        self._observing = True
        self._observer.start()
        LOGGER.debug(f'Started observation for command: {self.command_line}')

    def end_observation(self):
        LOGGER.debug(f'Ending observation for command: {self.command_line}')
        self._observing = False

    @property
    def command_line(self) -> str:
        return self._command_line

    @property
    def poll_period(self) -> float:
        return self._poll_period

    @property
    def observing(self) -> bool:
        return self._observing

    @property
    def state(self) -> Optional[Dict[str, Any]]:
        return self._state

    @abstractmethod
    def _should_observation_end(self) -> bool:
        """Determine whether job observation should end."""

    @abstractmethod
    def _poll(self) -> Optional[Dict[str, Any]]:
        """
        Poll job state and return it as a dictionary.
        If the state cannot be determined return None.
        """

    def _observe(self):
        num_null_polls = 0
        while self._observing:
            state = self._poll()
            if state is None:
                if num_null_polls == 3:
                    self.end_observation()
                    break
                num_null_polls += 1
            else:
                self._state = state
                if self._should_observation_end():
                    self.end_observation()
                    break
                num_null_polls = 0
            time.sleep(self._poll_period)


class LocalJob(ObservedBatchJob):
    """A job performed as a local OS process."""

    def __init__(self,
                 process: subprocess.Popen,
                 stdout: TextIO,
                 stderr: TextIO,
                 command_line: str,
                 poll_period: float = None):
        super().__init__(command_line, poll_period=poll_period)
        self._process: subprocess.Popen = process
        self._stdout: Optional[TextIO] = stdout
        self._stderr: Optional[TextIO] = stderr
        self.start_observation()

    @classmethod
    def submit_job(cls,
                   command: List[str],
                   out_path: str,
                   err_path: str,
                   *,
                   poll_period: float = None,
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

        command_line = subprocess.list2cmdline(command)

        with log_duration(f'Spawning process for command: {command_line}'):
            # noinspection PyBroadException
            try:
                process = subprocess.Popen(command, **subprocess_kwargs)
                return LocalJob(process, stdout, stderr, command_line, poll_period=poll_period)
            except BaseException:
                stdout.close()
                stderr.close()
                raise

    @property
    def status(self) -> JobStatus:
        exit_code = self.exit_code
        if exit_code is None:
            return JobStatus.RUNNING
        elif exit_code == 0:
            return JobStatus.COMPLETED
        else:
            return JobStatus.FAILED

    @property
    def exit_code(self) -> Optional[int]:
        return self.state.get('exit_code')

    @property
    def pid(self) -> int:
        return self.state['pid']

    def _should_observation_end(self) -> bool:
        return self.status is not JobStatus.RUNNING

    def _poll(self) -> Dict[str, Any]:
        exit_code = self._process.poll()
        state = dict(pid=self._process.pid)
        if exit_code is not None:
            state.update(exit_code=exit_code)
        return state

    def end_observation(self):
        super().end_observation()
        self._stdout.close()
        self._stderr.close()


class SlurmJob(ObservedBatchJob):
    """A job performed by the SLURM client 'sbatch'."""

    _STATUS_MAPPING = {
        "PD": JobStatus.PENDING,
        "R": JobStatus.RUNNING,
        "CG": JobStatus.COMPLETING,
        "CD": JobStatus.COMPLETED,
        "F": JobStatus.FAILED,
        "TO": JobStatus.TERMINATED,
        "S": JobStatus.SUSPENDED,
        "ST": JobStatus.STOPPED,
        None: JobStatus.UNKNOWN,
    }

    def __init__(self, job_id: str, command_line: str, poll_period: float = None, squeue_program: str = None):
        super().__init__(command_line, poll_period=poll_period)
        self._job_id: str = job_id
        self._state_base: Dict[str, Any] = dict(job_id=job_id)
        self._squeue_program = squeue_program
        self.start_observation()

    @classmethod
    def submit_job(cls,
                   command: List[str],
                   out_path: str,
                   err_path: str,
                   *,
                   poll_period: float = None,
                   cwd_path: str = None,
                   env_vars: Dict[str, Any] = None,
                   partition: str = None,
                   duration: str = None,
                   sbatch_program: str = None,
                   squeue_program: str = None,
                   **kwargs: str) -> 'SlurmJob':

        sbatch_command = [sbatch_program or 'sbatch', '-o', out_path, '-e', err_path]
        if partition:
            sbatch_command += [f'--partition={partition}']
        if duration:
            sbatch_command += [f'--time={duration}']
        if cwd_path:
            sbatch_command += [f'--chdir={cwd_path}']
        if env_vars:
            export = ",".join(f"{k}={v}" for k, v in env_vars.items())
            sbatch_command += [f'--export=ALL,{export}']
        sbatch_command += command

        command_line = subprocess.list2cmdline(sbatch_command)

        with log_duration(f'Running command: {command_line}'):
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
                return SlurmJob(job_id,
                                command_line,
                                poll_period=poll_period,
                                squeue_program=squeue_program)
        raise EnvironmentError(f'Cannot obtain Slurm job ID from command line:'
                               f' {command_line}: output was: "{output}"')

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def status(self) -> JobStatus:
        state = self.state
        if state is None:
            return JobStatus.UNKNOWN
        status_id = state.get('ST')
        return self._STATUS_MAPPING.get(status_id, JobStatus.UNKNOWN)

    def _should_observation_end(self) -> bool:
        return self.status in {
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.TERMINATED,
            JobStatus.STOPPED,
        }

    def _poll(self) -> Optional[Dict[str, Any]]:
        squeue_program = self._squeue_program or 'squeue --job=${job_id}'
        squeue_command = squeue_program.replace('${job_id}', self._job_id)
        result = subprocess.run(squeue_command,
                                capture_output=True,
                                timeout=0.9 * self.poll_period)
        if result.returncode == 0:
            lines = result.stdout.split(b'\n')
            if len(lines) >= 2:
                keys = lines[0].split()
                values = lines[1].split()
                if len(keys) == len(values):
                    return {k.decode(): v.decode()
                            for k, v in zip(keys, values)}
        return None

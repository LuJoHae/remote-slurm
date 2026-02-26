from typing import Literal, Optional
from returns.result import Result, Success, Failure
from remote_slurm.slurmify import SlurmScript, SlurmOptions
from remote_slurm.ssh import SSHConnection
import re
import logging

ExecutionMode = Literal["srun", "sbatch"]
logger = logging.getLogger(__name__)


class SubmittedSlurmJob:
    """Class that models a submitted SLURM job."""

    def __init__(self, job_id: str, slurm_options: 'SlurmOptions', ssh_connection: SSHConnection,
                 slurm_script: SlurmScript) -> None:
        """
        Initialize a SubmittedSlurmJob.

        Args:
            job_id: Job ID of the submitted SLURM job
            slurm_options: SlurmOptions object containing the job parameters
            ssh_connection: SSHConnection object for remote communication
            slurm_script: SlurmScript object containing the script used to execute the job
        """
        self.job_id = job_id
        self.slurm_options = slurm_options
        self.ssh_connection = ssh_connection
        self.slurm_script = slurm_script

    def is_running(self) -> Result[bool, str]:
        """
        Check if the SLURM job is still running.

        Returns:
            Result containing either a boolean (Success) or an error message (Failure)
        """
        command = f"squeue -j {self.job_id} --format='%T' --noheader"
        exec_result = self.ssh_connection.execute_command(command)

        if isinstance(exec_result, Failure):
            return Failure(f"Failed to check job status: {exec_result.failure()}")

        output = exec_result.unwrap()[0].strip()
        return Success(output in ["RUNNING", "PENDING"])

    def get_job_info(self) -> Result[dict, str]:
        """
        Get information about the SLURM job from squeue.

        Returns:
            Result containing either a dictionary with job info (Success) or an error message (Failure)
        """
        command = f"squeue -j {self.job_id} --format='%T %M %l %L' --noheader"
        exec_result = self.ssh_connection.execute_command(command)

        if isinstance(exec_result, Failure):
            return Failure(f"Failed to get job info: {exec_result.failure()}")

        output = exec_result.unwrap()[0].strip().split()
        try:
            status, elapsed_time, time_limit, time_left = output
        except ValueError as e:
            return Failure(f"Unexpected squeue output format: {e}")

        return Success({
            "status": status,
            "elapsed_time": elapsed_time,
            "time_limit": time_limit,
            "time_left": time_left
        })

    def read_log_files(self) -> Result[tuple[str, str], str]:
        """
        Read the log files from the paths given in the output and error parameters of the slurm options.

        Returns:
            Result containing either a tuple with stdout and stderr (Success) or an error message (Failure)
        """

        output_path = self.slurm_options.output\
            .replace("%j", self.job_id)\
            .replace("%u", self.ssh_connection.username)\
            .replace("%x", self.slurm_options.job_name)
        error_path = self.slurm_options.error\
            .replace("%j", self.job_id)\
            .replace("%u", self.ssh_connection.username)\
            .replace("%x", self.slurm_options.job_name)
        logger.debug(f"Setting log files to {output_path} and {error_path}")

        read_output_command = f"cat {output_path}"
        read_error_command = f"cat {error_path}"

        output_result = self.ssh_connection.execute_command(read_output_command)
        if isinstance(output_result, Failure):
            return Failure(f"Failed to read output log: {output_result.failure()}")

        error_result = self.ssh_connection.execute_command(read_error_command)
        if isinstance(error_result, Failure):
            return Failure(f"Failed to read error log: {error_result.failure()}")

        stdout = output_result.unwrap()[0]
        stderr = error_result.unwrap()[0]

        return Success((stdout, stderr))


def extract_job_number(response):
    matches = re.match(r"Submitted batch job (\d{7})\n", response)
    if matches is None:
        return Failure("No job id found in {}".format(response))
    if len(matches.groups()) >= 2:
        return Failure("Multiple job ids found in {}".format(response))
    return matches.groups()[0]


class SlurmExecutor:
    """Class that executes SLURM scripts on remote servers via SSH."""

    def __init__(
            self, ssh_connection: SSHConnection,
            slurm_script: SlurmScript,
            args_string: Optional[str] = None
    ) -> None:
        """
        Initialize a SlurmExecutor.

        Args:
            ssh_connection: SSHConnection object for remote communication
            slurm_script: SlurmScript object containing the script to execute
        """
        self.ssh_connection = ssh_connection
        self.slurm_script = slurm_script
        self.args_string = args_string

    def execute(
            self,
            mode: ExecutionMode = "sbatch",
            remote_path: Optional[str] = None,
            args_string: Optional[str] = None
    ) -> Result[SubmittedSlurmJob, str]:
        """
        Execute the SLURM script on the remote server and return a SubmittedSlurmJob object.

        Args:
            mode: Execution mode - either 'srun' for interactive or 'sbatch' for batch
            remote_path: Optional remote path where script will be uploaded. 
                        If None, uses /tmp/slurm_script_<hash>.sh
            args_string: Optional additional arguments string passed directly to sbatch/srun command.
                        If None, uses self.args_string.

        Returns:
            Result containing either the command output (Success) or an error message (Failure)
        """
        # Generate SLURM script content
        slurm_content_result = self.slurm_script.to_slurm_script()
        if isinstance(slurm_content_result, Failure):
            return slurm_content_result

        slurm_content = slurm_content_result.unwrap()

        # Determine remote script path
        if remote_path is None:
            script_hash = hash(slurm_content) & 0xFFFFFFFF
            remote_path = f"/tmp/slurm_script_{script_hash}.sh"

        # Upload script to remote server
        upload_result = self._upload_script(slurm_content, remote_path)
        if isinstance(upload_result, Failure):
            return upload_result

        # Execute with appropriate command
        command = f"{mode} {remote_path} {args_string}"
        logger.debug(f"Executing '{command}'")
        execution_result = self._run_command(command)

        # Cleanup: remove the script after execution
        cleanup_command = f"rm -f {remote_path}"
        self._run_command(cleanup_command)

        job_id = execution_result.map(extract_job_number)

        submitted_job = job_id.map(
            lambda x: SubmittedSlurmJob(x, self.slurm_script.slurm_options, self.ssh_connection, self.slurm_script)
        )
        return submitted_job

    def _upload_script(self, content: str, remote_path: str) -> Result[None, str]:
        """
        Upload script content to remote server.

        Args:
            content: Script content to upload
            remote_path: Path on remote server where script will be saved

        Returns:
            Result containing either None (Success) or an error message (Failure)
        """
        # Create script with proper permissions on remote
        escaped_content = content.replace("'", "'\\''")
        commands = [
            f"cat > {remote_path} << 'EOF'\n{content}\nEOF",
            f"chmod +x {remote_path}"
        ]

        for command in commands:
            result = self._run_command(command)
            if isinstance(result, Failure):
                return Failure(f"Failed to upload script: {result.failure()}")

        return Success(None)

    def _run_command(self, command: str) -> Result[str, str]:
        """
        Execute a command on the remote server via SSH.

        Args:
            command: Command to execute

        Returns:
            Result containing either the command output (Success) or an error message (Failure)
        """
        exec_result = self.ssh_connection.execute_command(command)

        if isinstance(exec_result, Failure):
            return exec_result

        return Success(exec_result.unwrap()[0])

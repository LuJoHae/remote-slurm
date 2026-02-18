from typing import Literal, Optional
from pathlib import Path
from returns.result import Result, Success, Failure
from remote_slurm.slurmify import SlurmScript
from remote_slurm.ssh import SSHConnection

ExecutionMode = Literal["srun", "sbatch"]


class SlurmExecutor:
    """Class that executes SLURM scripts on remote servers via SSH."""

    def __init__(self, ssh_connection: SSHConnection, slurm_script: SlurmScript) -> None:
        """
        Initialize a SlurmExecutor.

        Args:
            ssh_connection: SSHConnection object for remote communication
            slurm_script: SlurmScript object containing the script to execute
        """
        self.ssh_connection = ssh_connection
        self.slurm_script = slurm_script

    def execute(
            self,
            mode: ExecutionMode = "sbatch",
            remote_path: Optional[str] = None
    ) -> Result[str, str]:
        """
        Execute the SLURM script on the remote server.

        Args:
            mode: Execution mode - either 'srun' for interactive or 'sbatch' for batch
            remote_path: Optional remote path where script will be uploaded. 
                        If None, uses /tmp/slurm_script_<hash>.sh

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
        command = f"{mode} {remote_path}"
        execution_result = self._run_command(command)

        # Cleanup: remove the script after execution
        cleanup_command = f"rm -f {remote_path}"
        self._run_command(cleanup_command)

        return execution_result

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

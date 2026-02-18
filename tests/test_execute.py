import unittest
from unittest.mock import Mock
from returns.result import Success, Failure
from remote_slurm.execute import SlurmExecutor
from remote_slurm.slurmify import SlurmScript


class TestSlurmExecutor(unittest.TestCase):
    """Test suite for SlurmExecutor class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_ssh = Mock()
        self.mock_slurm_script = Mock(spec=SlurmScript)

    def test_execute_sbatch_success(self) -> None:
        """Test successful sbatch execution."""
        self.mock_slurm_script.to_slurm_script.return_value = Success("#!/bin/bash\necho test")
        self.mock_ssh.execute_command.side_effect = [
            Success(("", "", 0)),
            Success(("", "", 0)),
            Success(("Submitted batch job 12345\n", "", 0)),
            Success(("", "", 0))
        ]

        executor = SlurmExecutor(self.mock_ssh, self.mock_slurm_script)
        result = executor.execute(mode="sbatch")

        self.assertIsInstance(result, Success)
        self.assertEqual(self.mock_ssh.execute_command.call_count, 4)

    def test_execute_srun_success(self) -> None:
        """Test successful srun execution."""
        self.mock_slurm_script.to_slurm_script.return_value = Success("#!/bin/bash\necho test")
        self.mock_ssh.execute_command.side_effect = [
            Success(("", "", 0)),
            Success(("", "", 0)),
            Success(("test output\n", "", 0)),
            Success(("", "", 0))
        ]

        executor = SlurmExecutor(self.mock_ssh, self.mock_slurm_script)
        result = executor.execute(mode="srun")

        self.assertIsInstance(result, Success)

    def test_execute_with_custom_path(self) -> None:
        """Test execution with custom remote path."""
        custom_path = "/custom/path/script.sh"
        self.mock_slurm_script.to_slurm_script.return_value = Success("#!/bin/bash\necho test")
        self.mock_ssh.execute_command.side_effect = [
            Success(("", "", 0)),
            Success(("", "", 0)),
            Success(("output", "", 0)),
            Success(("", "", 0))
        ]

        executor = SlurmExecutor(self.mock_ssh, self.mock_slurm_script)
        result = executor.execute(remote_path=custom_path)

        self.assertIsInstance(result, Success)
        calls = self.mock_ssh.execute_command.call_args_list
        self.assertIn(custom_path, calls[2][0][0])

    def test_execute_script_generation_failure(self) -> None:
        """Test handling of script generation failure."""
        self.mock_slurm_script.to_slurm_script.return_value = Failure("Script generation failed")

        executor = SlurmExecutor(self.mock_ssh, self.mock_slurm_script)
        result = executor.execute()

        self.assertIsInstance(result, Failure)
        self.assertEqual(result.failure(), "Script generation failed")

    def test_execute_upload_failure(self) -> None:
        """Test handling of script upload failure."""
        self.mock_slurm_script.to_slurm_script.return_value = Success("#!/bin/bash\necho test")
        self.mock_ssh.execute_command.return_value = Failure(Exception("Upload failed"))

        executor = SlurmExecutor(self.mock_ssh, self.mock_slurm_script)
        result = executor.execute()

        self.assertIsInstance(result, Failure)

    def test_execute_command_failure(self) -> None:
        """Test handling of command execution failure."""
        self.mock_slurm_script.to_slurm_script.return_value = Success("#!/bin/bash\necho test")
        self.mock_ssh.execute_command.side_effect = [
            Success(("", "", 0)),
            Success(("", "", 0)),
            Failure(Exception("Execution failed")),
            Success(("", "", 0))  # cleanup still runs
        ]

        executor = SlurmExecutor(self.mock_ssh, self.mock_slurm_script)
        result = executor.execute()

        self.assertIsInstance(result, Failure)

    def test_upload_script(self) -> None:
        """Test script upload."""
        content = "echo 'test'"
        remote_path = "/tmp/test.sh"
        self.mock_ssh.execute_command.side_effect = [
            Success(("", "", 0)),
            Success(("", "", 0))
        ]

        executor = SlurmExecutor(self.mock_ssh, self.mock_slurm_script)
        result = executor._upload_script(content, remote_path)

        self.assertIsInstance(result, Success)
        self.assertEqual(self.mock_ssh.execute_command.call_count, 2)

    def test_run_command_success(self) -> None:
        """Test successful command execution."""
        self.mock_ssh.execute_command.return_value = Success(("output", "", 0))

        executor = SlurmExecutor(self.mock_ssh, self.mock_slurm_script)
        result = executor._run_command("echo test")

        self.assertIsInstance(result, Success)
        self.assertEqual(result.unwrap(), "output")

    def test_run_command_failure(self) -> None:
        """Test failed command execution."""
        self.mock_ssh.execute_command.return_value = Failure(Exception("Command failed"))

        executor = SlurmExecutor(self.mock_ssh, self.mock_slurm_script)
        result = executor._run_command("invalid")

        self.assertIsInstance(result, Failure)


if __name__ == '__main__':
    unittest.main()

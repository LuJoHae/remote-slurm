import unittest
from remote_slurm.slurmify import SlurmOptions, SlurmScriptConverter, SlurmScript
from returns.result import Success, Failure
from pathlib import Path
import tempfile
import os


class TestSlurmOptions(unittest.TestCase):
    """Test suite for SlurmOptions class."""

    def test_to_dict_empty(self) -> None:
        """Test to_dict with no options set."""
        options = SlurmOptions()
        result = options.to_dict()
        self.assertEqual(result, {})

    def test_to_dict_basic_options(self) -> None:
        """Test to_dict with basic options."""
        options = SlurmOptions(partition="gpu", time="01:00:00", nodes=2, job_name="test_job")
        result = options.to_dict()
        self.assertEqual(result['partition'], 'gpu')
        self.assertEqual(result['time'], '01:00:00')
        self.assertEqual(result['nodes'], '2')
        self.assertEqual(result['job-name'], 'test_job')

    def test_to_dict_underscore_to_hyphen(self) -> None:
        """Test that underscores are converted to hyphens."""
        options = SlurmOptions(cpus_per_task=4, ntasks_per_node=2, mem_per_cpu="2G")
        result = options.to_dict()
        self.assertIn('cpus-per-task', result)
        self.assertIn('ntasks-per-node', result)
        self.assertIn('mem-per-cpu', result)

    def test_to_dict_boolean_conversion(self) -> None:
        """Test that booleans are converted to lowercase strings."""
        options = SlurmOptions(exclusive=True, requeue=False)
        result = options.to_dict()
        self.assertEqual(result['exclusive'], 'true')
        self.assertEqual(result['requeue'], 'false')


class TestSlurmScriptConverter(unittest.TestCase):
    """Test suite for SlurmScriptConverter class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.converter = SlurmScriptConverter()

    def test_bash_to_slurm_simple(self) -> None:
        """Test converting simple bash script to SLURM."""
        bash_script = "echo 'Hello World'"
        result = self.converter.bash_to_slurm(bash_script)
        self.assertIn('#!/bin/bash', result)
        self.assertIn("echo 'Hello World'", result)

    def test_bash_to_slurm_with_shebang(self) -> None:
        """Test converting bash script with existing shebang."""
        bash_script = "#!/bin/bash\necho test"
        result = self.converter.bash_to_slurm(bash_script)
        self.assertTrue(result.startswith('#!/bin/bash'))
        self.assertIn('echo test', result)

    def test_bash_to_slurm_with_options(self) -> None:
        """Test converting bash script with SLURM options."""
        bash_script = "echo test"
        options = SlurmOptions(partition="gpu", time="01:00:00", nodes=2)
        result = self.converter.bash_to_slurm(bash_script, options)
        self.assertIn('#SBATCH --partition=gpu', result)
        self.assertIn('#SBATCH --time=01:00:00', result)
        self.assertIn('#SBATCH --nodes=2', result)
        self.assertIn('echo test', result)

    def test_slurm_to_bash_remove_directives(self) -> None:
        """Test converting SLURM script to bash with directives removed."""
        slurm_script = "#!/bin/bash\n#SBATCH --partition=gpu\n#SBATCH --time=01:00:00\necho test"
        result = self.converter.slurm_to_bash(slurm_script, remove_directives=True)
        self.assertIn('#!/bin/bash', result)
        self.assertNotIn('#SBATCH', result)
        self.assertIn('echo test', result)

    def test_slurm_to_bash_keep_directives(self) -> None:
        """Test converting SLURM script to bash with directives kept."""
        slurm_script = "#!/bin/bash\n#SBATCH --partition=gpu\necho test"
        result = self.converter.slurm_to_bash(slurm_script, remove_directives=False)
        self.assertIn('#SBATCH --partition=gpu', result)
        self.assertIn('echo test', result)

    def test_extract_slurm_options_empty(self) -> None:
        """Test extracting options from script with no SBATCH directives."""
        slurm_script = "#!/bin/bash\necho test"
        result = self.converter.extract_slurm_options(slurm_script)
        self.assertEqual(result, {})

    def test_extract_slurm_options_multiple(self) -> None:
        """Test extracting multiple SLURM options."""
        slurm_script = "#!/bin/bash\n#SBATCH --partition=gpu\n#SBATCH --time=01:00:00\n#SBATCH --nodes=2\necho test"
        result = self.converter.extract_slurm_options(slurm_script)
        self.assertEqual(result['partition'], 'gpu')
        self.assertEqual(result['time'], '01:00:00')
        self.assertEqual(result['nodes'], '2')


class TestSlurmScript(unittest.TestCase):
    """Test suite for SlurmScript class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sh')
        self.temp_file.write("#!/bin/bash\necho 'test script'")
        self.temp_file.close()

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        if os.path.exists(self.temp_file.name):
            os.unlink(self.temp_file.name)

    def test_init(self) -> None:
        """Test SlurmScript initialization."""
        options = SlurmOptions(partition="gpu")
        script = SlurmScript(options, self.temp_file.name)
        self.assertEqual(script.slurm_options, options)
        self.assertEqual(script.bash_script_path, Path(self.temp_file.name))

    def test_to_slurm_script_success(self) -> None:
        """Test successful conversion to SLURM script."""
        options = SlurmOptions(partition="gpu", time="01:00:00")
        script = SlurmScript(options, self.temp_file.name)
        result = script.to_slurm_script()
        self.assertIsInstance(result, Success)
        content = result.unwrap()
        self.assertIn('#!/bin/bash', content)
        self.assertIn('#SBATCH --partition=gpu', content)
        self.assertIn('#SBATCH --time=01:00:00', content)
        self.assertIn("echo 'test script'", content)

    def test_to_slurm_script_file_not_found(self) -> None:
        """Test handling of missing bash script file."""
        options = SlurmOptions(partition="gpu")
        script = SlurmScript(options, "/nonexistent/file.sh")
        result = script.to_slurm_script()
        self.assertIsInstance(result, Failure)
        self.assertIn("not found", result.failure())

    def test_to_slurm_script_no_options(self) -> None:
        """Test conversion with no SLURM options."""
        options = SlurmOptions()
        script = SlurmScript(options, self.temp_file.name)
        result = script.to_slurm_script()
        self.assertIsInstance(result, Success)
        content = result.unwrap()
        self.assertIn('#!/bin/bash', content)
        self.assertIn("echo 'test script'", content)


if __name__ == '__main__':
    unittest.main()

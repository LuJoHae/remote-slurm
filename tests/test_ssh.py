import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
import paramiko
from returns.result import Success, Failure
from remote_slurm.ssh import SSHConnection
from pathlib import Path
from datetime import datetime, timedelta
import tempfile
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from unittest.mock import Mock


class TestSSHConnection(unittest.TestCase):
    """Test suite for SSHConnection class."""

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        self.hostname = "test.example.com"
        self.username = "testuser"
        self.port = 22
        # Create a temporary key file
        self.temp_key = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_test_key')
        self.temp_key.write("fake_key_content")
        self.temp_key.close()
        self.key_path = self.temp_key.name

    def tearDown(self) -> None:
        """Clean up test fixtures after each test method."""
        if os.path.exists(self.key_path):
            os.unlink(self.key_path)

    @patch('remote_slurm.ssh.Path.exists')
    @patch('remote_slurm.ssh.SSHConnection._parse_ssh_config')
    def test_init_with_explicit_params(self, mock_parse_config: Mock, mock_exists: Mock) -> None:
        """Test SSHConnection initialization with all parameters provided."""
        mock_parse_config.return_value = Success({})
        mock_exists.return_value = True

        conn = SSHConnection(
            hostname=self.hostname,
            username=self.username,
            key_path=self.key_path,
            port=self.port,
            key_expiration_hours=48
        )

        self.assertEqual(conn.hostname, self.hostname)
        self.assertEqual(conn.username, self.username)
        self.assertEqual(conn.port, self.port)
        self.assertEqual(conn.key_expiration_hours, 48)
        self.assertEqual(str(conn.key_path), self.key_path)

    @patch('remote_slurm.ssh.Path.exists')
    @patch('remote_slurm.ssh.SSHConnection._parse_ssh_config')
    def test_init_from_ssh_config(self, mock_parse_config: Mock, mock_exists: Mock) -> None:
        """Test SSHConnection initialization reading from SSH config."""
        mock_parse_config.return_value = Success({
            'hostname': 'full.example.com',
            'user': 'configuser',
            'identityfile': self.key_path,
            'port': 2222
        })
        mock_exists.return_value = True

        conn = SSHConnection(hostname=self.hostname)

        self.assertEqual(conn.hostname, 'full.example.com')
        self.assertEqual(conn.username, 'configuser')
        self.assertEqual(conn.port, 2222)
        self.assertEqual(str(conn.key_path), self.key_path)


    @patch('remote_slurm.ssh.Path.exists')
    @patch('remote_slurm.ssh.SSHConnection._parse_ssh_config')
    def test_init_missing_key_path_raises_error(self, mock_parse_config: Mock, mock_exists: Mock) -> None:
        """Test that missing key_path raises ValueError."""
        mock_parse_config.return_value = Success({})
        mock_exists.return_value = True

        with self.assertRaises(ValueError) as context:
            SSHConnection(hostname=self.hostname, username=self.username)

        self.assertIn("Key path not provided", str(context.exception))

    @patch('remote_slurm.ssh.Path.home')
    def test_parse_ssh_config_file_not_found(self, mock_home: Mock) -> None:
        """Test SSH config parsing when config file doesn't exist."""
        mock_config_path = Mock()
        mock_config_path.exists.return_value = False

        # Create a proper Path mock that supports division operations
        mock_ssh_dir = Mock()
        mock_ssh_dir.__truediv__ = Mock(return_value=mock_config_path)
        mock_home_path = Mock()
        mock_home_path.__truediv__ = Mock(return_value=mock_ssh_dir)
        mock_home.return_value = mock_home_path

        conn = SSHConnection.__new__(SSHConnection)
        conn.logger = Mock()
        result = conn._parse_ssh_config(self.hostname)

        self.assertIsInstance(result, Success)
        self.assertEqual(result.unwrap(), {})

    def test_check_key_validity_missing_key(self) -> None:
        """Test key validity check when key file doesn't exist."""
        conn = SSHConnection.__new__(SSHConnection)
        conn.key_path = Path('/nonexistent/key')

        result = conn._check_key_validity()

        self.assertIsInstance(result, Failure)
        self.assertIsInstance(result.failure(), FileNotFoundError)

    def test_check_key_validity_expired_key(self) -> None:
        """Test key validity check with expired key."""
        # Create key file with old modification time
        old_time = (datetime.now() - timedelta(hours=48)).timestamp()
        os.utime(self.key_path, (old_time, old_time))

        conn = SSHConnection.__new__(SSHConnection)
        conn.key_path = Path(self.key_path)
        conn.key_expiration_hours = 24
        conn.logger = Mock()

        result = conn._check_key_validity()

        self.assertIsInstance(result, Failure)
        self.assertIn("older than", str(result.failure()))

    def test_check_key_validity_valid_key(self) -> None:
        """Test key validity check with valid key."""
        conn = SSHConnection.__new__(SSHConnection)
        conn.key_path = Path(self.key_path)
        conn.key_expiration_hours = 24
        conn.logger = Mock()

        result = conn._check_key_validity()

        self.assertIsInstance(result, Success)

    def test_check_key_validity_warning_threshold(self) -> None:
        """Test key validity check warning when key is approaching expiration."""
        # Create key file with modification time that triggers warning
        warning_time = (datetime.now() - timedelta(hours=18)).timestamp()
        os.utime(self.key_path, (warning_time, warning_time))

        conn = SSHConnection.__new__(SSHConnection)
        conn.key_path = Path(self.key_path)
        conn.key_expiration_hours = 24
        conn.logger = Mock()

        result = conn._check_key_validity()

        self.assertIsInstance(result, Success)
        conn.logger.warning.assert_called_once()

    @patch('paramiko.SSHClient')
    def test_do_connect_direct(self, mock_ssh_client_class: Mock) -> None:
        """Test direct SSH connection without proxy."""
        mock_client = Mock()
        mock_sftp = Mock()
        mock_client.open_sftp.return_value = mock_sftp
        mock_ssh_client_class.return_value = mock_client

        conn = SSHConnection.__new__(SSHConnection)
        conn.hostname = self.hostname
        conn.username = self.username
        conn.key_path = Path(self.key_path)
        conn.port = self.port
        conn.proxy_hostname = None
        conn.logger = Mock()
        conn.client = None
        conn.sftp = None

        result = conn._do_connect()

        # _do_connect is decorated with @safe, so it returns Success(None)
        self.assertIsInstance(result, Success)
        self.assertIsNone(result.unwrap())
        mock_client.set_missing_host_key_policy.assert_called_once()
        mock_client.connect.assert_called_once_with(
            hostname=self.hostname,
            username=self.username,
            key_filename=str(self.key_path),  # Should be string, not Path object
            port=self.port
        )
        mock_client.open_sftp.assert_called_once()

    @patch('remote_slurm.ssh.paramiko.SSHClient')
    def test_do_connect_with_proxy(self, mock_ssh_client_class: Mock) -> None:
        """Test SSH connection through ProxyJump."""
        # Create three separate mock clients:
        # 1. main client (self.client created first)
        # 2. proxy_client (created for proxy connection)
        mock_main_client = Mock()
        mock_proxy_client = Mock()
        mock_transport = Mock()
        mock_channel = Mock()
        mock_sftp = Mock()

        mock_proxy_client.get_transport.return_value = mock_transport
        mock_transport.open_channel.return_value = mock_channel
        mock_main_client.open_sftp.return_value = mock_sftp

        # First call creates self.client, second call creates proxy_client
        mock_ssh_client_class.side_effect = [mock_main_client, mock_proxy_client]

        conn = SSHConnection.__new__(SSHConnection)
        conn.hostname = self.hostname
        conn.username = self.username
        conn.key_path = Path(self.key_path)
        conn.port = self.port
        conn.proxy_hostname = 'proxy.example.com'
        conn.proxy_username = 'proxyuser'
        conn.proxy_key_path = Path(self.key_path)
        conn.proxy_port = 22
        conn.logger = Mock()
        conn.client = None
        conn.sftp = None

        result = conn._do_connect()

        # _do_connect is decorated with @safe, so it returns Success(None)
        self.assertIsInstance(result, Success)
        self.assertIsNone(result.unwrap())

        # Verify proxy connection was made (proxy_client.connect)
        proxy_call_kwargs = mock_proxy_client.connect.call_args[1]
        self.assertEqual(proxy_call_kwargs['hostname'], 'proxy.example.com')
        self.assertEqual(proxy_call_kwargs['username'], 'proxyuser')

        # Verify target connection through proxy (main_client.connect)
        target_call_kwargs = mock_main_client.connect.call_args[1]
        self.assertEqual(target_call_kwargs['sock'], mock_channel)
        self.assertEqual(target_call_kwargs['hostname'], self.hostname)

    @patch('remote_slurm.ssh.SSHConnection._ensure_connection')
    def test_execute_command_success(self, mock_ensure_connection: Mock) -> None:
        """Test successful command execution."""
        mock_ensure_connection.return_value = Success(None)

        mock_channel = Mock()
        mock_channel.recv_exit_status.return_value = 0
        mock_stdout = Mock()
        mock_stdout.read.return_value = b"command output"
        mock_stdout.channel = mock_channel
        mock_stderr = Mock()
        mock_stderr.read.return_value = b""

        mock_client = Mock()
        mock_client.exec_command.return_value = (Mock(), mock_stdout, mock_stderr)

        conn = SSHConnection.__new__(SSHConnection)
        conn.client = mock_client
        conn.logger = Mock()

        result = conn.execute_command("ls -la", timeout=30)

        self.assertIsInstance(result, Success)
        stdout, stderr, exit_code = result.unwrap()
        self.assertEqual(stdout, "command output")
        self.assertEqual(stderr, "")
        self.assertEqual(exit_code, 0)
        mock_client.exec_command.assert_called_once_with("ls -la", timeout=30)

    @patch('remote_slurm.ssh.SSHConnection._ensure_connection')
    def test_execute_command_connection_failure(self, mock_ensure_connection: Mock) -> None:
        """Test command execution when connection fails."""
        mock_ensure_connection.return_value = Failure(Exception("Connection failed"))

        conn = SSHConnection.__new__(SSHConnection)
        conn.logger = Mock()

        result = conn.execute_command("ls -la")

        self.assertIsInstance(result, Failure)

    @patch('remote_slurm.ssh.SSHConnection._ensure_connection')
    def test_execute_command_exec_failure(self, mock_ensure_connection: Mock) -> None:
    #     """Test command execution failure."""
        mock_ensure_connection.return_value = Success(None)

        mock_client = Mock()
        mock_client.exec_command.side_effect = Exception("Exec failed")

        conn = SSHConnection.__new__(SSHConnection)
        conn.client = mock_client
        conn.logger = Mock()

        result = conn.execute_command("invalid_command")

        self.assertIsInstance(result, Failure)

    @patch('remote_slurm.ssh.SSHConnection._ensure_connection')
    @patch('remote_slurm.ssh.Path.exists')
    def test_copy_to_remote_success(self, mock_exists: Mock, mock_ensure_connection: Mock) -> None:
        """Test successful file copy to remote."""
        mock_ensure_connection.return_value = Success(None)
        mock_exists.return_value = True

        mock_sftp = Mock()
        conn = SSHConnection.__new__(SSHConnection)
        conn.sftp = mock_sftp
        conn.logger = Mock()
        conn.hostname = self.hostname

        result = conn.copy_to_remote(self.key_path, "/remote/path/file")

        self.assertIsInstance(result, Success)
        mock_sftp.put.assert_called_once_with(self.key_path, "/remote/path/file")

    @patch('remote_slurm.ssh.SSHConnection._ensure_connection')
    def test_copy_to_remote_local_file_not_found(self, mock_ensure_connection: Mock) -> None:
        """Test copy to remote when local file doesn't exist."""
        mock_ensure_connection.return_value = Success(None)

        conn = SSHConnection.__new__(SSHConnection)
        conn.logger = Mock()

        result = conn.copy_to_remote("/nonexistent/file", "/remote/path/file")

        self.assertIsInstance(result, Failure)
        self.assertIsInstance(result.failure(), FileNotFoundError)

    @patch('remote_slurm.ssh.SSHConnection._ensure_connection')
    def test_copy_from_remote_success(self, mock_ensure_connection: Mock) -> None:
        """Test successful file copy from remote."""
        mock_ensure_connection.return_value = Success(None)

        mock_sftp = Mock()
        conn = SSHConnection.__new__(SSHConnection)
        conn.sftp = mock_sftp
        conn.logger = Mock()
        conn.hostname = self.hostname

        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = os.path.join(tmpdir, "downloaded_file")
            result = conn.copy_from_remote("/remote/path/file", local_path)

            self.assertIsInstance(result, Success)
            mock_sftp.get.assert_called_once()

    @patch('remote_slurm.ssh.SSHConnection.execute_command')
    @patch('remote_slurm.ssh.SSHConnection.copy_to_remote')
    @patch('remote_slurm.ssh.Path.exists')
    def test_execute_script_success(self, mock_exists: Mock, mock_copy: Mock, mock_execute: Mock) -> None:
        """Test successful script execution."""
        mock_exists.return_value = True
        mock_copy.return_value = Success(None)
        mock_execute.side_effect = [
            Success(("", "", 0)),  # chmod command
            Success(("script output", "", 0))  # script execution
        ]

        conn = SSHConnection.__new__(SSHConnection)
        conn.logger = Mock()

        result = conn.execute_script(self.key_path, "/tmp", "--arg1 --arg2", timeout=60)

        self.assertIsInstance(result, Success)
        stdout, stderr, exit_code = result.unwrap()
        self.assertEqual(stdout, "script output")
        self.assertEqual(exit_code, 0)

    @patch('remote_slurm.ssh.SSHConnection._ensure_connection')
    def test_check_connection_success(self, mock_ensure_connection: Mock) -> None:
        """Test successful connection check."""
        mock_ensure_connection.return_value = Success(None)

        conn = SSHConnection.__new__(SSHConnection)
        conn.logger = Mock()

        # Mock execute_command to return success
        conn.execute_command = Mock(return_value=Success(("connection_test\n", "", 0)))  # type: ignore

        result = conn.check_connection()

        self.assertIsInstance(result, Success)
        self.assertTrue(result.unwrap())

    @patch('remote_slurm.ssh.SSHConnection._ensure_connection')
    def test_check_connection_failure(self, mock_ensure_connection: Mock) -> None:
        """Test connection check failure."""
        mock_ensure_connection.return_value = Failure(Exception("Connection failed"))

        conn = SSHConnection.__new__(SSHConnection)
        conn.logger = Mock()

        result = conn.check_connection()

        self.assertIsInstance(result, Failure)

    def test_close_connection(self) -> None:
        """Test closing SSH connection."""
        mock_client = Mock()
        mock_sftp = Mock()

        conn = SSHConnection.__new__(SSHConnection)
        conn.client = mock_client
        conn.sftp = mock_sftp
        conn.hostname = self.hostname
        conn.logger = Mock()

        conn.close()

        mock_sftp.close.assert_called_once()
        mock_client.close.assert_called_once()
        self.assertIsNone(conn.client)
        self.assertIsNone(conn.sftp)

    @patch('remote_slurm.ssh.SSHConnection._ensure_connection')
    def test_context_manager(self, mock_ensure_connection: Mock) -> None:
        """Test SSHConnection as context manager."""
        mock_ensure_connection.return_value = Success(None)

        mock_client = Mock()
        mock_sftp = Mock()

        conn = SSHConnection.__new__(SSHConnection)
        conn.client = None
        conn.sftp = None
        conn.hostname = self.hostname
        conn.logger = Mock()

        # Set mocks before entering context so they're cleaned up on exit
        conn.client = mock_client
        conn.sftp = mock_sftp

        with conn as context_conn:
            self.assertEqual(context_conn, conn)

        # After exiting context, close should have been called
        mock_sftp.close.assert_called_once()
        mock_client.close.assert_called_once()

    @patch('remote_slurm.ssh.SSHConnection._ensure_connection')
    def test_context_manager_with_exception(self, mock_ensure_connection: Mock) -> None:
        """Test context manager handles exceptions properly."""
        mock_ensure_connection.return_value = Failure(Exception("Connection failed"))

        conn = SSHConnection.__new__(SSHConnection)
        conn.hostname = self.hostname
        conn.logger = Mock()

        with self.assertRaises(Exception):
            with conn as context_conn:
                pass

    def test_ensure_connection_checks_existing_connection(self) -> None:
        """Test _ensure_connection reuses active connection."""
        mock_transport = Mock()
        mock_transport.is_active.return_value = True
        mock_client = Mock()
        mock_client.get_transport.return_value = mock_transport

        conn = SSHConnection.__new__(SSHConnection)
        conn.client = mock_client
        conn.key_path = Path(self.key_path)
        conn.key_expiration_hours = 24
        conn.logger = Mock()

        result = conn._ensure_connection()

        self.assertIsInstance(result, Success)
        # _do_connect should not be called since connection is active
        mock_client.get_transport.assert_called()


    def test_ensure_connection_reconnects_inactive_connection(self) -> None:
        """Test _ensure_connection reconnects when connection is inactive."""
        mock_transport = Mock()
        mock_transport.is_active.return_value = False
        mock_client = Mock()
        mock_client.get_transport.return_value = mock_transport

        conn = SSHConnection.__new__(SSHConnection)
        conn.client = mock_client
        conn.key_path = Path(self.key_path)
        conn.key_expiration_hours = 24
        conn.logger = Mock()
        conn._do_connect = Mock(return_value=Success(None))  # type: ignore

        result = conn._ensure_connection()

        self.assertIsInstance(result, Success)
        conn._do_connect.assert_called_once()


if __name__ == '__main__':
    unittest.main()

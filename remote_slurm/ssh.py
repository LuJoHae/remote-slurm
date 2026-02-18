import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any
import paramiko
import os
from returns.result import Result, Success, Failure, safe


class SSHConnection:
    """Class to handle SSH connections to a remote server."""
    client: Optional[paramiko.SSHClient]
    sftp: Optional[paramiko.SFTPClient]
    hostname: str
    username: str
    key_path: Optional[Path]
    port: int
    logger: logging.Logger

    def __init__(self, hostname: str, username: Optional[str] = None, key_path: Optional[str] = None,
                 port: Optional[int] = None, key_expiration_hours: float = 24) -> None:
        """
        Initialize SSH connection parameters.

        Args:
            hostname: Remote server hostname or IP address
            username: Username for SSH connection (optional, reads from SSH config if not provided)
            key_path: Path to SSH private key file (optional, reads from SSH config if not provided)
            port: SSH port (optional, reads from SSH config if not provided, defaults to 22)
            key_expiration_hours: Hours before SSH key expires (default: 24)
        """
        self.logger = logging.getLogger(__name__)

        # Parse SSH config to fill in missing parameters
        config_result = self._parse_ssh_config(hostname)
        config_params = config_result.value_or({})

        # Update hostname to full URL from SSH config if available
        self.hostname = config_params.get('hostname', hostname)

        self.username = username if username is not None else str(config_params.get('user'))

        # Safely extract and convert key_path
        key_path_str: Optional[str] = key_path if key_path is not None else config_params.get('identityfile')
        self.key_path = Path(key_path_str) if key_path_str is not None else None

        self.port = port if port is not None else config_params.get('port', 22)
        self.key_expiration_hours = key_expiration_hours

        # ProxyJump configuration
        self.proxy_hostname = config_params.get('proxy_hostname')
        self.proxy_username = config_params.get('proxy_username')

        proxy_key_path_str: Optional[str] = config_params.get('proxy_key_path') if config_params.get('proxy_key_path') else None
        self.proxy_key_path = Path(proxy_key_path_str) if proxy_key_path_str else None

        self.proxy_port = config_params.get('proxy_port', 22)

        if self.username is None:
            raise ValueError(f"Username not provided and not found in SSH config for host {hostname}")
        if self.key_path is None:
            raise ValueError(f"Key path not provided and not found in SSH config for host {hostname}")

        self.client = None
        self.sftp = None


    def _parse_ssh_config(self, hostname: str) -> Result[Dict[str, Any], Exception]:
        """
        Parse SSH config file for the given hostname.

        Args:
            hostname: Hostname to look up in SSH config

        Returns:
            Result[dict, Exception]: Configuration parameters from SSH config
        """
        config_path = Path.home() / '.ssh' / 'config'
        config_params: dict[str, str] = {}

        if not config_path.exists():
            self.logger.debug(f"SSH config file not found at {config_path}")
            return Success(config_params)

        @safe
        def _do_parse_config(hostname: str, config_path: Path, config_params: Dict[str, Any]) -> Dict[str, Any]:
            """
            Parse SSH config file implementation.

            Args:
                hostname: Hostname to look up in SSH config
                config_path: Path to SSH config file
                config_params: Dictionary to store parsed parameters

            Returns:
                dict: Configuration parameters from SSH config
            """
            ssh_config = paramiko.SSHConfig()
            with open(config_path, 'r') as f:
                ssh_config.parse(f)

            host_config = ssh_config.lookup(hostname)

            if 'hostname' in host_config:
                config_params['hostname'] = host_config['hostname']
            if 'user' in host_config:
                config_params['user'] = host_config['user']
            if 'identityfile' in host_config and host_config['identityfile']:
                # identityfile can be a list, take the first one
                identity = host_config['identityfile']
                identity_str = identity[0] if isinstance(identity, list) else identity
                config_params['identityfile'] = os.path.expanduser(identity_str)
            if 'port' in host_config:
                config_params['port'] = int(host_config['port'])

            # Parse ProxyJump configuration
            if 'proxyjump' in host_config:
                proxy_jump = host_config['proxyjump']
                # ProxyJump can be in format: user@host:port or just host
                proxy_parts = proxy_jump.split('@')
                if len(proxy_parts) == 2:
                    config_params['proxy_username'] = proxy_parts[0]
                    proxy_host_port = proxy_parts[1]
                else:
                    proxy_host_port = proxy_parts[0]

                # Parse host and port
                if ':' in proxy_host_port:
                    proxy_host, proxy_port_str = proxy_host_port.split(':')
                    config_params['proxy_hostname'] = proxy_host
                    config_params['proxy_port'] = int(proxy_port_str)
                else:
                    config_params['proxy_hostname'] = proxy_host_port

                # Look up proxy host configuration for additional details
                proxy_config = ssh_config.lookup(config_params['proxy_hostname'])
                if 'hostname' in proxy_config:
                    config_params['proxy_hostname'] = proxy_config['hostname']
                if 'user' in proxy_config and 'proxy_username' not in config_params:
                    config_params['proxy_username'] = proxy_config['user']
                if 'identityfile' in proxy_config and proxy_config['identityfile']:
                    proxy_identity = proxy_config['identityfile']
                    proxy_identity_str = proxy_identity[0] if isinstance(proxy_identity, list) else proxy_identity
                    config_params['proxy_key_path'] = os.path.expanduser(proxy_identity_str)
                if 'port' in proxy_config and 'proxy_port' not in config_params:
                    config_params['proxy_port'] = int(proxy_config['port'])

            self.logger.debug(f"Parsed SSH config for {hostname}: {config_params}")
            return config_params

        result = _do_parse_config(hostname, config_path, config_params)
        if isinstance(result, Failure):
            self.logger.warning(f"Failed to parse SSH config: {result.failure()}")
            return Success(config_params)

        return result


    def _check_key_validity(self) -> Result[None, Exception]:
        """
        Check if SSH key is valid based on modification time.

        Returns:
            Result[None, Exception]: Success if key is valid, Failure otherwise
        """
        if self.key_path is None or not self.key_path.exists():
            return Failure(FileNotFoundError(f"SSH key not found at {self.key_path}"))

        key_mtime = datetime.fromtimestamp(self.key_path.stat().st_mtime)
        key_age = datetime.now() - key_mtime
        expiration_time = timedelta(hours=self.key_expiration_hours)

        if key_age > expiration_time:
            return Failure(RuntimeError(
                f"SSH key is older than {self.key_expiration_hours} hours ({key_age}). Please regenerate the key."))

        # Warning threshold is half the expiration time or one week, whichever is smaller
        warning_threshold = min(expiration_time / 2, timedelta(weeks=1))
        if key_age > warning_threshold:
            remaining_time = expiration_time - key_age
            self.logger.warning(f"SSH key will expire in {remaining_time}. Consider regenerating it soon.")

        return Success(None)

    def _ensure_connection(self) -> Result[None, Exception]:
        """
        Ensure SSH connection is established after validating key.

        Returns:
            Result[None, Exception]: Success if connection established, Failure otherwise
        """
        validity_result = self._check_key_validity()
        if isinstance(validity_result, Failure):
            return validity_result

        if self.client is None or not self.client.get_transport():
            return self._do_connect()
        transport = self.client.get_transport()
        if transport is None or not transport.is_active():
            return self._do_connect()


        return Success(None)

    @safe
    def _do_connect(self) -> None:
        """
        Connect to remote server implementation.

        Returns:
            None
        """
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Check if ProxyJump is configured
        if self.proxy_hostname:
            self.logger.info(f"Connecting through proxy: {self.proxy_username}@{self.proxy_hostname}:{self.proxy_port}")

            # Create proxy client
            proxy_client = paramiko.SSHClient()
            proxy_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            proxy_client.connect(
                hostname=self.proxy_hostname,
                username=self.proxy_username,
                key_filename=str(self.proxy_key_path) if self.proxy_key_path else str(self.key_path),
                port=self.proxy_port
            )

            # Create transport channel through proxy
            proxy_transport = proxy_client.get_transport()
            dest_addr = (self.hostname, self.port)
            local_addr = ('127.0.0.1', 0)
            if proxy_transport is None:
                raise RuntimeError("Proxy transport is None")
            proxy_channel = proxy_transport.open_channel("direct-tcpip", dest_addr, local_addr)

            # Connect to target host through proxy channel
            self.client.connect(
                hostname=self.hostname,
                username=self.username,
                key_filename=str(self.key_path),
                port=self.port,
                sock=proxy_channel
            )
        else:
            self.client.connect(
                hostname=self.hostname,
                username=self.username,
                key_filename=str(self.key_path),
                port=self.port
            )

        self.sftp = self.client.open_sftp()
        self.logger.info(f"SSH connection established to {self.username}@{self.hostname}")
        return None

    def execute_command(self, command: str, timeout: Optional[float] = None) -> Result[Tuple[str, str, int], Exception]:
        """
        Execute a bash command on the remote server.

        Args:
            command: Command string to execute
            timeout: Command timeout in seconds (optional)

        Returns:
            Result[tuple, Exception]: (stdout, stderr, exit_code) or error
        """
        connection_result = self._ensure_connection()
        if isinstance(connection_result, Failure):
            return connection_result

        @safe
        def _do_execute(command: str, timeout: Optional[float]) -> Tuple[str, str, int]:
            """
            Execute command on remote server implementation.

            Args:
                command: Command string to execute
                timeout: Command timeout in seconds (optional)

            Returns:
                tuple: (stdout, stderr, exit_code)
            """
            assert self.client is not None
            stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
            exit_code = stdout.channel.recv_exit_status()

            stdout_str = stdout.read().decode('utf-8')
            stderr_str = stderr.read().decode('utf-8')

            self.logger.debug(f"Executed command: {command} (exit code: {exit_code})")

            return stdout_str, stderr_str, exit_code

        return _do_execute(command, timeout)

    def copy_to_remote(self, local_path: str | Path, remote_path: str) -> Result[None, Exception]:
        """
        Copy a file from local to remote server.

        Args:
            local_path: Path to local file
            remote_path: Destination path on remote server

        Returns:
            Result[None, Exception]: Success if copied, Failure otherwise
        """
        connection_result = self._ensure_connection()
        if isinstance(connection_result, Failure):
            return connection_result

        local_path = Path(local_path)
        if not local_path.exists():
            logging.error(f"Local file not found: {local_path}")
            return Failure(FileNotFoundError(f"Local file not found: {local_path}"))

        @safe
        def _do_copy_to_remote(local_path: Path, remote_path: str) -> None:
            """
            Copy file to remote server implementation.

            Args:
                local_path: Path to local file
                remote_path: Destination path on remote server

            Returns:
                None
            """
            assert self.sftp is not None
            self.sftp.put(str(local_path), remote_path)
            self.logger.info(f"Copied {local_path} to {self.hostname}:{remote_path}")
            return None

        return _do_copy_to_remote(local_path, remote_path)



    def copy_from_remote(self, remote_path: str, local_path: str) -> Result[None, Exception]:
        """
        Copy a file from remote server to local.

        Args:
            remote_path: Path to file on remote server
            local_path: Destination path on local machine

        Returns:
            Result[None, Exception]: Success if copied, Failure otherwise
        """
        connection_result = self._ensure_connection()
        if isinstance(connection_result, Failure):
            return connection_result

        return self._do_copy_from_remote(remote_path, local_path)

    @safe
    def _do_copy_from_remote(self, remote_path: str, local_path: str) -> None:
        """
        Copy file from remote server implementation.

        Args:
            remote_path: Path to file on remote server
            local_path: Destination path on local machine

        Returns:
            None
        """
        local_path_obj = Path(local_path)
        local_path_obj.parent.mkdir(parents=True, exist_ok=True)

        assert self.sftp is not None
        self.sftp.get(remote_path, str(local_path_obj))
        self.logger.info(f"Copied {self.hostname}:{remote_path} to {local_path_obj}")
        return None

    def execute_script(self, script_path: str | Path, remote_dir: str = "/tmp", script_args: str = "",
                       timeout: Optional[float] = None) -> Result[Tuple[str, str, int], Exception]:
        """
        Execute a bash script by copying it to remote and running it.

        Args:
            script_path: Path to local script file
            remote_dir: Remote directory to copy script to (default: /tmp)
            script_args: Arguments to pass to the script (optional)
            timeout: Script execution timeout in seconds (optional)

        Returns:
            Result[tuple, Exception]: (stdout, stderr, exit_code) or error
        """
        script_path = Path(script_path)
        if not script_path.exists():
            return Failure(FileNotFoundError(f"Script not found: {script_path}"))

        remote_script_path = f"{remote_dir}/{script_path.name}"

        copy_result = self.copy_to_remote(script_path, remote_script_path)
        if isinstance(copy_result, Failure):
            return copy_result

        chmod_cmd = f"chmod +x {remote_script_path}"
        chmod_result = self.execute_command(chmod_cmd)
        if isinstance(chmod_result, Failure):
            return chmod_result

        exec_cmd = f"{remote_script_path} {script_args}"
        exec_result = self.execute_command(exec_cmd, timeout=timeout)

        if isinstance(exec_result, Success):
            stdout, stderr, exit_code = exec_result.unwrap()
            self.logger.info(f"Executed script {script_path.name} on remote (exit code: {exit_code})")

        return exec_result

    def check_connection(self) -> Result[bool, Exception]:
        """
        Check if SSH connection works.

        Returns:
            Result[bool, Exception]: Success(True) if connection works, Failure otherwise
        """
        connection_result = self._ensure_connection()
        if isinstance(connection_result, Failure):
            return connection_result

        # Try to execute a simple command to verify connection
        test_result = self.execute_command("echo 'connection_test'", timeout=5.0)
        if isinstance(test_result, Failure):
            return test_result

        stdout, stderr, exit_code = test_result.unwrap()
        if exit_code == 0 and 'connection_test' in stdout:
            return Success(True)
        else:
            return Failure(RuntimeError("Connection test failed"))

    def close(self) -> None:
        """Close SSH and SFTP connections."""
        if self.sftp:
            self.sftp.close()
            self.sftp = None
        if self.client:
            self.client.close()
            self.client = None
        self.logger.info(f"SSH connection closed to {self.hostname}")

    def __enter__(self) -> 'SSHConnection':
        """Context manager entry."""
        connection_result = self._ensure_connection()
        if isinstance(connection_result, Failure):
            raise connection_result.failure()
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[Any]) -> None:
        """Context manager exit."""
        self.close()

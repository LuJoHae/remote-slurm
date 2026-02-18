import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Literal, Union
from returns.result import Result, Success, Failure


@dataclass
class SlurmOptions:
    """Dataclass for SLURM job options with full typing."""

    # Partition and QOS
    partition: Optional[str] = None
    qos: Optional[str] = None
    account: Optional[str] = None

    # Time limits
    time: Optional[str] = None  # Format: DD-HH:MM:SS or HH:MM:SS
    time_min: Optional[str] = None

    # Resource allocation
    nodes: Optional[int] = None
    ntasks: Optional[int] = None
    ntasks_per_node: Optional[int] = None
    cpus_per_task: Optional[int] = None
    ntasks_per_core: Optional[int] = None
    ntasks_per_socket: Optional[int] = None

    # Memory
    mem: Optional[str] = None  # e.g., "4G", "4096M"
    mem_per_cpu: Optional[str] = None
    mem_per_gpu: Optional[str] = None

    # GPUs and generic resources
    gres: Optional[str] = None  # e.g., "gpu:1", "gpu:v100:2"
    gpus: Optional[Union[int, str]] = None
    gpus_per_node: Optional[Union[int, str]] = None
    gpus_per_task: Optional[Union[int, str]] = None

    # Job identification
    job_name: Optional[str] = None
    output: Optional[str] = None
    error: Optional[str] = None

    # Email notifications
    mail_type: Optional[Literal["NONE", "BEGIN", "END", "FAIL", "REQUEUE", "ALL"]] = None
    mail_user: Optional[str] = None

    # Job dependencies
    dependency: Optional[str] = None  # e.g., "afterok:12345"

    # Job arrays
    array: Optional[str] = None  # e.g., "1-10", "1-10:2"

    # Constraints and features
    constraint: Optional[str] = None

    # Working directory
    chdir: Optional[str] = None

    # Distribution
    distribution: Optional[Literal["block", "cyclic", "arbitrary", "plane"]] = None

    # Priority
    priority: Optional[int] = None
    nice: Optional[int] = None

    # Restart/requeue
    requeue: Optional[bool] = None
    no_requeue: Optional[bool] = None

    # Signals
    signal: Optional[str] = None

    # Other options
    exclusive: Optional[bool] = None
    overcommit: Optional[bool] = None
    wait: Optional[bool] = None
    test_only: Optional[bool] = None

    # Advanced scheduling
    begin: Optional[str] = None  # Start time
    deadline: Optional[str] = None

    # License
    licenses: Optional[str] = None

    # Reservation
    reservation: Optional[str] = None

    # Core specification
    core_spec: Optional[int] = None
    thread_spec: Optional[int] = None

    # Socket/core/thread binding
    sockets_per_node: Optional[int] = None
    cores_per_socket: Optional[int] = None
    threads_per_core: Optional[int] = None

    # I/O
    input: Optional[str] = None
    open_mode: Optional[Literal["append", "truncate"]] = None

    # Network
    network: Optional[str] = None

    # Heterogeneous jobs
    het_group: Optional[str] = None

    # MPI options
    mpi: Optional[str] = None

    # Propagate environment
    propagate: Optional[str] = None
    export: Optional[str] = None
    export_file: Optional[str] = None

    # Power management
    power: Optional[str] = None

    # Profile
    profile: Optional[Literal["none", "energy", "task", "lustre", "network", "all"]] = None

    # Other
    comment: Optional[str] = None
    wckey: Optional[str] = None
    cluster: Optional[str] = None
    clusters: Optional[str] = None

    def to_dict(self) -> Dict[str, str]:
        """Convert the dataclass to a dictionary, excluding None values."""
        result = {}
        for key, value in self.__dict__.items():
            if value is not None:
                # Convert underscores to hyphens for SLURM
                slurm_key = key.replace('_', '-')
                result[slurm_key] = str(value).lower() if isinstance(value, bool) else str(value)
        return result


class SlurmScriptConverter:
    """Converts between bash scripts and SLURM scripts."""

    def __init__(self) -> None:
        self.slurm_directives = [
            'partition', 'time', 'nodes', 'ntasks', 'ntasks-per-node',
            'cpus-per-task', 'mem', 'mem-per-cpu', 'job-name', 'output',
            'error', 'mail-type', 'mail-user', 'account', 'qos', 'gres'
        ]

    def bash_to_slurm(self, bash_script: str,
                      slurm_options: Optional[SlurmOptions] = None) -> str:
        """
        Convert a bash script to a SLURM script.

        Args:
            bash_script: The bash script content
            slurm_options: SlurmOptions instance with SLURM job options

        Returns:
            SLURM script content
        """
        lines = bash_script.strip().split('\n')
        slurm_lines = []

        # Handle shebang
        if lines and lines[0].startswith('#!'):
            slurm_lines.append(lines[0])
            lines = lines[1:]
        else:
            slurm_lines.append('#!/bin/bash')

        # Add SLURM directives
        if slurm_options:
            slurm_lines.append('')
            options_dict = slurm_options.to_dict()
            for key, value in options_dict.items():
                slurm_lines.append(f'#SBATCH --{key}={value}')

        # Add remaining bash script content
        slurm_lines.append('')
        slurm_lines.extend(lines)

        return '\n'.join(slurm_lines)

    def slurm_to_bash(self, slurm_script: str, remove_directives: bool = True) -> str:
        """
        Convert a SLURM script to a bash script.

        Args:
            slurm_script: The SLURM script content
            remove_directives: Whether to remove SLURM directives (default: True)

        Returns:
            Bash script content
        """
        lines = slurm_script.strip().split('\n')
        bash_lines = []

        for line in lines:
            # Keep shebang
            if line.startswith('#!'):
                bash_lines.append(line)
            # Remove or keep SLURM directives based on flag
            elif line.startswith('#SBATCH'):
                if not remove_directives:
                    bash_lines.append(line)
            # Keep all other lines
            else:
                bash_lines.append(line)

        return '\n'.join(bash_lines)

    def extract_slurm_options(self, slurm_script: str) -> Dict[str, str]:
        """
        Extract SLURM options from a SLURM script.

        Args:
            slurm_script: The SLURM script content

        Returns:
            Dictionary of SLURM options
        """
        options = {}
        pattern = r'#SBATCH\s+--([a-zA-Z-]+)=(.+)'

        for line in slurm_script.split('\n'):
            match = re.match(pattern, line.strip())
            if match:
                key, value = match.groups()
                options[key] = value.strip()

        return options


class SlurmScript:
    """Class that combines SLURM options with a bash script file."""

    def __init__(self, slurm_options: SlurmOptions, bash_script_path: Union[str, Path]):
        """
        Initialize a SlurmJob.

        Args:
            slurm_options: SlurmOptions instance with SLURM job configuration
            bash_script_path: Path to the bash script file
        """
        self.slurm_options = slurm_options
        self.bash_script_path = Path(bash_script_path)
        self.converter = SlurmScriptConverter()

    def to_slurm_script(self) -> Result[str, str]:
        """
        Read the bash script file and convert it to a SLURM script.

        Returns:
            Result containing either the SLURM script content (Success) or an error message (Failure)
        """
        if not self.bash_script_path.exists():
            return Failure(f"Bash script not found: {self.bash_script_path}")

        try:
            bash_content = self.bash_script_path.read_text()
            slurm_script = self.converter.bash_to_slurm(bash_content, self.slurm_options)
            return Success(slurm_script)
        except IOError as e:
            return Failure(f"Error reading file: {str(e)}")

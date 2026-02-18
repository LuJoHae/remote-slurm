# remote-slurm

A Python library for executing SLURM scripts on remote servers via SSH connections.

## Overview

remote-slurm provides a programmatic interface for managing and executing SLURM workload manager scripts on remote
high-performance computing clusters. The library handles SSH connectivity, script generation, remote execution, and
error handling through a type-safe Result-based API.

## Features

- SSH-based remote execution of SLURM scripts
- Support for both interactive (srun) and batch (sbatch) execution modes
- Automatic script upload and cleanup
- Type-safe error handling using the returns library
- Configurable SLURM parameters through a script builder interface

## Requirements

- Python 3.10 or higher
- paramiko 4.0.0
- returns 0.26.0

## Installation

Install the package using uv or pip:

```bash
pip install remote-slurm
```

Or with uv:

```bash
uv add remote-slurm
```

## Quick Start

```python
from remote_slurm.ssh import SSHConnection
from remote_slurm.slurmify import SlurmOptions, SlurmScript
from remote_slurm.execute import SlurmExecutor

# Configure SSH connection
ssh_config = {
    "hostname": "your-hpc-cluster.edu",
    "username": "your-username",
    "key_filename": "/path/to/ssh/key"
}

# Create SSH connection
ssh_conn = SSHConnection(**ssh_config)
connect_result = ssh_conn.connect()

if connect_result.is_success:
    # Configure SLURM options
    slurm_opts = SlurmOptions(
        partition="gpu",
        time="01:00:00",
        nodes=1,
        ntasks_per_node=4,
        cpus_per_task=2,
        mem="16G",
        gres="gpu:1",
        job_name="my_job",
        output="job_%j.out",
        error="job_%j.err"
    )

    # Create SLURM script from bash script
    slurm_script = SlurmScript(slurm_opts, "path/to/your/script.sh")

    # Execute on remote cluster
    executor = SlurmExecutor(ssh_conn, slurm_script)
    result = executor.execute(mode="sbatch")

    if result.is_success:
        print(f"Job submitted: {result.unwrap()}")
    else:
        print(f"Error: {result.failure()}")

    ssh_conn.close()
```

## Usage Examples

### Basic SLURM Job Submission

```python
from remote_slurm.slurmify import SlurmOptions, SlurmScript

# Define SLURM parameters
options = SlurmOptions(
    partition="compute",
    time="02:00:00",
    ntasks=8,
    mem="32G",
    job_name="data_processing"
)

# Create script from bash file
script = SlurmScript(options, "process_data.sh")

# Get SLURM script content
slurm_content = script.to_slurm_script()
if slurm_content.is_success:
    print(slurm_content.unwrap())
```

### Interactive Execution with srun

```python
# Execute interactively instead of batch mode
result = executor.execute(mode="srun")
```

### Custom Remote Path

```python
# Specify custom remote path for script
result = executor.execute(
    mode="sbatch",
    remote_path="/home/username/jobs/my_job.sh"
)
```

### Converting Between Bash and SLURM Scripts

```python
from remote_slurm.slurmify import SlurmScriptConverter

converter = SlurmScriptConverter()

# Convert bash to SLURM
bash_script = """#!/bin/bash
echo "Hello World"
python train_model.py
"""

options = SlurmOptions(partition="gpu", time="01:00:00")
slurm_script = converter.bash_to_slurm(bash_script, options)

# Extract options from SLURM script
extracted_options = converter.extract_slurm_options(slurm_script)
```

## API Reference

### SlurmOptions

Configuration class for SLURM job parameters. Key attributes:

- `partition`: SLURM partition name
- `time`: Maximum run time (format: `DD-HH:MM:SS`)
- `nodes`: Number of nodes
- `ntasks`: Number of tasks
- `cpus_per_task`: CPUs per task
- `mem`: Memory allocation (e.g., "16G")
- `gres`: Generic resources (e.g., "gpu:2")
- `job_name`: Job name
- `output`: Standard output file path
- `error`: Standard error file path

See the `SlurmOptions` class for all available parameters.

### SlurmScript

Combines SLURM options with a bash script file.

**Methods:**
- `to_slurm_script() -> Result[str, str]`: Converts bash script to SLURM script

### SSHConnection

Manages SSH connections to remote servers.

**Methods:**
- `connect() -> Result[None, str]`: Establish SSH connection
- `execute_command(command: str) -> Result[tuple[str, str], str]`: Execute remote command
- `close() -> None`: Close SSH connection

### SlurmExecutor

Executes SLURM scripts on remote servers.

**Methods:**
- `execute(mode: ExecutionMode = "sbatch", remote_path: Optional[str] = None) -> Result[str, str]`: Execute SLURM script remotely

## Error Handling

The library uses the `returns` library for type-safe error handling. All operations return `Result` objects:

```python
result = executor.execute(mode="sbatch")

if result.is_success:
    output = result.unwrap()
    print(f"Success: {output}")
else:
    error = result.failure()
    print(f"Error: {error}")
```

from functools import cached_property
from typing import List

import boto3
from pathlib import Path
import subprocess


class S3Syncer:
    def __init__(
        self, bucket_name="serial-scammer-analyser-bucket", data_dir="resources/data"
    ):

        self.bucket_name = bucket_name
        self.data_dir = Path(data_dir)
        self.s3_client = boto3.client("s3")

    @cached_property
    def current_file_path(self):
        return Path(__file__)

    @cached_property
    def project_root(self):
        for parent in self.current_file_path.parents:
            if (parent / "requirements.txt").exists():
                return parent
        # If no .git is found, raise an exception
        raise FileNotFoundError("No project root directory containing 'requirements.txt' found.")

    @cached_property
    def data_directory(self):
        return (self.project_root / self.data_dir).resolve()

    def sync(self):
        """
        Call this function to synchronise the s3 bucket with the items in data_directory
        """
        local_path = str(self.data_directory)
        command_to_sync_local_to_bucket = ["aws", "s3", "sync", local_path, f"s3://{self.bucket_name}"]
        command_to_sync_bucket_to_local = ["aws", "s3", "sync", f"s3://{self.bucket_name}", local_path]

        sync_commands = [command_to_sync_local_to_bucket, command_to_sync_bucket_to_local]
        for sync_command in sync_commands:
            self.run_command(sync_command)

    def run_command(self, command: List[str]) -> None:
        """
        Runs a system command using subprocess and handles output/error.
        """
        try:
            with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
                for line in process.stdout:
                    print(line, end="")  # Print each line as it comes in

                stderr_output = process.stderr.read()  # Capture any error output
                if stderr_output:
                    print(f"Error output: {stderr_output}")

                process.wait()  # Wait for the process to finish
                if process.returncode != 0:
                    print(f"Error syncing files: {process.returncode}")
        except Exception as e:
            print(f"Failed to execute command {' '.join(command)}: {e}")


if __name__ == "__main__":
    """
    1. Install aws-cli (https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) 
    
    2. Run the aws configure command in your terminal. This will prompt you to enter your AWS credentials. If you 
    don't have these credentials, reach out to your project manager to obtain them.  
    
    3. Instantiate the S3FileManager class by specifying the directory to sync between the S3 bucket and local storage.
       
       For example: s3_file_manager = S3FileManager(data_dir="resources/data/uniswap")

       Note that `data_dir` is the path to the local directory that will be synchronized with the corresponding 
       directory in your S3 bucket.

    4. To synchronize files between the S3 bucket and the local directory, simply call the `sync()` method wherever
       necessary in your program:

       s3_file_manager.sync()

       Note that this will ensure that any new or modified files in either the local directory or the S3 bucket are 
       synchronized.
    """
    # Example usage:

    s3_file_manager = S3Syncer(data_dir="resources/data")
    s3_file_manager.sync()

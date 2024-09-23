from functools import cached_property
import boto3
from pathlib import Path
import subprocess


class S3FileManager:
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
            if (parent / ".git").exists():
                return parent
        # If no .git is found, raise an exception
        raise FileNotFoundError("No project root directory containing '.git' found.")

    @cached_property
    def data_directory(self):
        return (self.project_root / self.data_dir).resolve()

    def sync(self):
        """
        Call this function to synchronise the s3 bucket with the items in data_directory
        """
        local_path = str(self.data_directory)
        command = ["aws", "s3", "sync", local_path, f"s3://{self.bucket_name}"]

        with subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        ) as process:
            for line in process.stdout:
                print(line, end="")  # Print each line as it comes in

            stderr_output = process.stderr.read()  # Capture any error output
            if stderr_output:
                print(stderr_output)

        try:
            process.wait()  # Wait for the process to finish
            if process.returncode != 0:
                print(f"Error syncing files: {process.returncode}")
        except Exception as e:
            print(f"Error syncing files: {e}")


if __name__ == "__main__":
    """
    1. Instantiate the S3FileManager class by specifying the directory to sync between the S3 bucket and local storage.
       For example:

       s3_file_manager = S3FileManager(data_dir="resources/data/uniswap")

       - `data_dir` is the path to the local directory that will be synchronized with the corresponding directory in your S3 bucket.

    2. To synchronize files between the S3 bucket and the local directory, simply call the `sync()` method wherever necessary in your program:

       s3_file_manager.sync()

       - This will ensure that any new or modified files in either the local directory or the S3 bucket are synchronized.
    """
    # Example usage:
    s3_file_manager = S3FileManager(data_dir="resources/data/uniswap")
    s3_file_manager.sync()

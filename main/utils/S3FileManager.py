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
    S3FileManager().sync()

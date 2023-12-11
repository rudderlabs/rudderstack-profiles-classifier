"""
We run the module2 (heavy lifting work of loading data from warehouse and training) in ec2 using this as entry point.

There are three main scripts related to this workflow:
1. Current script, which is run on module1/local/rudder-sources that triggers the job on ec2
2. A shell script that is run on ec2. This prepares the environment and runs the module2 script, finally cleans up the files in remote
3. preprocess_and_train.py, a python script where the actual work is done. 
    Currently it's just a poc where it is connecting to a warehouse, downloading some data locally and writing to a remote folder in s3

We are doing following steps here:
1. Load credentials from site config and create a local credentials.json file
2. Zip the current directory excluding some unnecessary folders and files
3. Transfer the zip file and the shell script to the remote machine
4. Run the shell script on the remote machine
5. Delete the locally created credentials file

Assumptions:
1. The ec2 machine is on before this script starts
2. AWS Credentials are already set up on the local machine and on ec2 - and required permissions are available. Key ones being:
    * Current env has access to ec2
    * s3 bucket is already available
    * ec2 instance has access to s3 bucket
    * ec2 instance can make warehouse connection (no ip restrictions)
3. python and pip are already available on ec2

"""

import zipfile
import os
from fnmatch import fnmatch
import tempfile
import json
import subprocess
import yaml
import pathlib

TEMP_DIR = tempfile.gettempdir()

exclude_folders = ["__pycache__",  ".*", "logs", "output", "python_packages", "sample_profiles_project", 
                   "tests"]
exclude_files = [".*", "*.md", "*.log", "*.ipynb", "launch_ec2_job.sh"]


def can_exclude(name: str, exclude_list: list) -> bool:
    """Check if the name can be excluded

    Args:
        name: Name to exclude
        exlude_list: List of names to be excluded can be regular expressions

    Returns:
        bool: True if the name can be excluded
    """
    for exclude_name in exclude_list:
        if fnmatch(name, exclude_name):
            return True
    return False

def zip_directory(
    dir_path: str, exclude_folders: list = [], exclude_files: list = []
) -> str:
    """Zip the directory

    Args:
        dir_path (str):  Path to the directory to zip
        exclude_folders (list): List of directories to be excluded from the zip
        exclude_files (list): List of files to be excluded from the zip

    Returns:
        str: Path to the zip file

    Raises:
        ValueError: If the directory does not exist
    """
    if dir_path == '.':
        dir_path = os.getcwd()
    if not os.path.exists(dir_path):
        raise ValueError(f"Directory {dir_path} does not exist")

    dir_name = os.path.basename(dir_path)
    zip_path = os.path.join(TEMP_DIR, dir_name + ".zip")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(dir_path):

            # Exclude folders
            dirs_to_remove = []
            for dirname in dirs:
                if can_exclude(dirname, exclude_folders):
                    dirs_to_remove.append(dirname)

            for dirname in dirs_to_remove:
                dirs.remove(dirname)

            for file in files:
                # Exclude files
                if can_exclude(file, exclude_files):
                    continue

                abs_file_path = os.path.join(root, file)
                zip_file.write(
                    abs_file_path, abs_file_path.replace(dir_path, "")
                )

    return zip_path

def main(pem_file, ec2_user, ec2_ip, remote_dir, remote_shell_script):
    homedir = os.path.expanduser("~") 
    with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
        creds = yaml.safe_load(f)["connections"]["shopify_wh"]["outputs"]["dev"]
        
    with open("credentials.json", "w") as f:
        json.dump(creds, f)
        
    zip_path = zip_directory(os.getcwd(), exclude_folders, exclude_files)
    print("Transfering the zip file with all scripts to the remote machine")
    response = subprocess.run(["scp", "-i", pem_file, zip_path, f"{ec2_user}@{ec2_ip}:{remote_dir}"],  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print("Transfering the shell script to the remote machine")
    response = subprocess.run(["scp", "-i", pem_file, remote_shell_script, f"{ec2_user}@{ec2_ip}:{remote_dir}"],  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    remote_zip_path = os.path.join(remote_dir, os.path.basename(zip_path))
    remote_unzippped = remote_zip_path.replace(".zip", "")

    cmds = ["ssh", "-i", pem_file, f"{ec2_user}@{ec2_ip}",
            "sh", f"{remote_dir}/{remote_shell_script}", 
            remote_zip_path, remote_unzippped, 
            "preprocess_and_train.py", 
            "--output_path", "output",
            "--s3_bucket", "ml-usecases-poc",
            "--s3_path", "test_export"]
    print("Running the shell script on the remote machine")
    response = subprocess.run(cmds,  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    # Delete the locally created credentials file
    pathlib.Path("credentials.json").unlink()

if __name__ == "__main__":
    pem_file = "<path_to_pem_file>.pem"
    ec2_ip= '<public-ipv4-dns>.compute-1.amazonaws.com'
    ec2_user = 'ec2-user'
    remote_dir = '/home/ec2-user'
    remote_shell_script = "launch_ec2_job.sh"
    import time
    tic = time.time()
    main(pem_file, ec2_user, ec2_ip, remote_dir, remote_shell_script)
    toc = time.time()
    print(f"Time taken: {toc-tic} seconds")
    
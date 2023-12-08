"""
Test creation of a sagemaker processing job. Should do following
1. Read a local file (config.yaml, credentials.yaml)
2. Use a local module (utils.py)
3. Connect to an external warehouse and read/write to it
4. Write an output file to an s3 location

/opt/ml/processing would be the working directory. Any files should be written to this, and read through this.

Two scripts:
A scrpit to call sagemaker processing job
    Sets up required session objects
    Should zip all required files and prepare ProcessingInput and ProcessingOutput
    Should call sagemaker processing job with these inputs
    Extract output from s3 location
    
An entry point inside sagemaker processing job
    Should unzip all files from input
    Should install requirements
    Call the actual script
    Write output to output folder
"""
import os
from pathlib import Path
import zipfile

def unzip_directory(zip_path: str, out_dir_path: str) -> None:
    if not os.path.exists(zip_path):
        raise ValueError(f"Zip file {zip_path} does not exist")
    
    Path(out_dir_path).mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zip_file:
        zip_file.extractall(out_dir_path)


if __name__ == "__main__":
    import sys 
    import subprocess
    import argparse 
    import json
    print(sys.version)
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str)
    parser.add_argument('--source_code_zip', type=str)
    parser.add_argument('--credentials', type=str)
    parser.add_argument('--output_path', type=str)
    args = parser.parse_args()
    print(args.credentials)
    input_path= args.input_path
    print(input_path)
    source_code_path = os.path.dirname(args.source_code_zip)
    #home_dir = "/opt/ml/processing/"    
    #sys.path.append(os.path.join(home_dir, "code"))
    sys.path.append(input_path)
    unzip_directory(args.source_code_zip, input_path)
    subprocess.run([sys.executable, "-m", "pip","install", "-r", os.path.join(input_path, "requirements.txt"),  "--quiet"])
    
    creds = json.loads(args.credentials)
    from SnowflakeConnector import SnowflakeConnector
    connector = SnowflakeConnector()
    sess = connector.build_session(creds)
    t = sess.table("material_registry_4").toPandas()
    t.to_csv(os.path.join(args.output_path, 'data.csv'), index=False)
    

    #config_file = load_yaml(os.path.join(home_dir, "input", "config.yaml"))

    # with open("/opt/ml/processing/train/foo.txt", "w") as f:
    #     f.write("Hello World")

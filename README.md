### 1. Building a virtual environment

You can create a virtual environment through conda. The approach is outlined below. 

#### 1.1 Creating the conda environment

```bash
conda create -n pynative --override-channels -c https://repo.anaconda.com/pkgs/snowflake python=3.8
```

NOTE - There is a known issue with running Snowpark Python on Apple silicon chips due to memory handling in pyOpenSSL. The error message displayed is, “Cannot allocate write+execute memory for ffi.callback()”.

As a workaround, set up a virtual environment that uses x86 Python using these commands:
```bash
CONDA_SUBDIR=osx-64 conda create -n pynative python=3.8 --override-channels -c https://repo.anaconda.com/pkgs/snowflake
conda activate pynative
conda config --env --set subdir osx-64
```
After creating the environment, you need to install the requirements inside the environment.

#### 1.2 Installing profiles_rudderstack modules inside conda env

first we need to install the profiles_rudderstack module inside the env. For that, first pull the latest changes from pywht repo and run following commands inside it -
```bash
cd profiles_rudderstack
WHT_CI=true pip install .
```

#### 1.3 Installing python packages from classifier repo inside conda env

Python packages which we want to run are needed to be installed inside the env as well. For example, installing the profiles_classifier module present inside the classifier repo, follow below steps inside classifier repo -
```bash
cd python_packages/profiles_classifier_pkg
pip install .
```

#### 1.4 Making necessary corrections in conda env
NOTE- If you are running the code on Mac M1/M2, you need to install xgboost seperately using below lines -
```bash
pip uninstall grpcio
conda install grpcio
brew install libomp
conda install -c conda-forge py-xgboost==1.5.0
```

#### 1.5 Letting wht know about conda env python path
find python path of the created env -
```bash
conda activate pynative
which python
```

Now copy this python path and paste it to file pb_pypath.txt (this file should be in the same folder as that of pb. If this file doesn't exist, create it.). run following lines from home directory -
```bash
which pb
cd <pb_folder_path>
touch pb_pypath.txt
vim pb_pypath.txt
```
After opening this file just paste the python path of the environment in it.

### 2. Running the model

To run the models as per your requirements, change the profiles.yaml file accordingly and run following lines inside classifier repo -
```bash
cd sample_profiles_project
pb run --migrate_on_load=True
```
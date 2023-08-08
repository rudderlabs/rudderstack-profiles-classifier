Churn classification model built on top of profiles feature tables. 



## Building the conda environment

First you need to create the proper conda environment. Follow below steps to build the environment-
```bash
conda create -n pysnowpark --override-channels -c https://repo.anaconda.com/pkgs/snowflake python=3.8
```

NOTE - There is a known issue with running Snowpark Python on Apple silicon chips due to memory handling in pyOpenSSL. The error message displayed is, “Cannot allocate write+execute memory for ffi.callback()”.

As a workaround, set up a virtual environment that uses x86 Python using these commands:
```bash
CONDA_SUBDIR=osx-64 conda create -n pysnowpark python=3.8 --override-channels -c https://repo.anaconda.com/pkgs/snowflake
conda activate pysnowpark
conda config --env --set subdir osx-64
```
After creating the environment, you need to install the requirements inside the environment using "pip install -r requirements.txt".

NOTE- If you are running the code on Mac M1/M2, you need to install xgboost seperately using below lines -
```bash
brew install libomp
conda install -c conda-forge py-xgboost==1.5.0
```


### Test cases:

This is a WIP. Currently only the skeleton is present with some sample code. You can test the code by running the following command:

```bash
python -m unittest discover -s tests
```

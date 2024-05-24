from setuptools import setup, find_packages
from version import version

setup(
    name="profiles_mlcorelib",
    version=version,
    author="rudderstack",
    packages=find_packages(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License"
    ],
    install_requires=[
        "profiles_rudderstack>=0.11.1",
        "cachetools>=5.3.2",
        "hyperopt>=0.2.7",
        "joblib>=1.3.2",
        "matplotlib>=3.7.5",
        "seaborn>=0.13.1",
        "numpy>=1.24.4",
        "pandas>=2.0.3,<2.2.0",
        "pyarrow>=14.0.2",
        "PyYAML>=6.0.1",
        "scikit_learn>=1.3.2",
        "shap>=0.44.0",
        "xgboost>=2.0.3",
        "redshift-connector>=2.0.918",
        "pandas-redshift>=2.0.5",
        "sqlalchemy-redshift>=0.8.14",
        "sqlalchemy>=1.4.51,<2.0.0",
        "snowflake_connector_python>=3.6.0",
        "snowflake-snowpark-python[pandas]>=1.11.1",
        "google-cloud-bigquery>=3.17.2",
        "sqlalchemy-bigquery>=1.9.0",
        "db-dtypes>=1.2.0",
    ],
    include_package_data=True,
)

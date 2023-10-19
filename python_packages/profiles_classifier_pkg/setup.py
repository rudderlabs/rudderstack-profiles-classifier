from setuptools import setup

version = "1.0.0"

requirements = [
        "cachetools==4.2.2",
        "hyperopt==0.2.7",
        "joblib==1.2.0",
        "matplotlib==3.7.1",
        "seaborn==0.12.0",
        "numpy==1.23.1",
        "pandas==1.5.3",
        "PyYAML==6.0.1",
        "snowflake_connector_python==3.1.0",
        "snowflake-snowpark-python[pandas]==0.10.0",
        "scikit_learn==1.1.1",
        "scikit_plot==0.3.7",
        "shap==0.41.0",
        "platformdirs==3.8.1",
        "xgboost==1.5.0"
        ]

setup(
    name='profiles_classifier',
    version=version,
    author='Ambuj Mishra',
    author_email='ambujm@ruddersatck.com',
    description='A py_native model package that implements the classification/regression model',
    packages=['profiles_classifier'],
    include_package_data=True,
    install_requires=requirements,
)
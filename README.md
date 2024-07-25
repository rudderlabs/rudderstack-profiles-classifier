# profiles_mlcorelib
profiles_mlcorelib is a Python package that provides PyNative models which can be used in Profiles projects.

### Installation

You can install the package from [PyPi](https://pypi.org/project/profiles-mlcorelib/) using pip
```bash
pip3 install profiles_mlcorelib
```

### Contribution

#### Updating an Existing Model

1. Make Code Changes:
- Update the desired model with your changes.
- Ensure any new or updated dependencies are reflected in [setup.py](https://github.com/rudderlabs/rudderstack-profiles-classifier/blob/main/src/predictions/setup.py).
2. Install the Updated Package:
- Navigate to the package directory and install it locally to reflect your changes:
    ```bash
    cd src/predictions
    pip3 install .
    ```
3. Compile/run the project
- Execute the project that uses the model being updated to verify your changes.

#### Creating a New Model

1. Implement the New Model:
- Implement the PyNative model interface in [py_native](https://github.com/rudderlabs/rudderstack-profiles-classifier/tree/main/src/predictions/profiles_mlcorelib/py_native)/<new_model>.py.
- Ensure any new or updated dependencies are reflected in [setup.py](https://github.com/rudderlabs/rudderstack-profiles-classifier/blob/main/src/predictions/setup.py).
- Register this model type by adding it in [register_extensions](https://github.com/rudderlabs/rudderstack-profiles-classifier/blob/main/src/predictions/profiles_mlcorelib/__init__.py#L3) method. This is required for pb to be aware of this new model.
2. Install the Updated Package:
- Navigate to the package directory and install it locally to reflect your changes:
    ```bash
    cd src/predictions
    pip3 install .
    ```
3. Create a Sample Project:
- Add a new project in the [samples](https://github.com/rudderlabs/rudderstack-profiles-classifier/tree/main/samples) directory that utilizes the new model type.
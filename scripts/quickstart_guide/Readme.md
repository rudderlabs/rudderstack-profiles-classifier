
We have published a [quickstart guide](https://www.rudderstack.com/docs/profiles/get-started/sample-data/) to help get started with the Profiles. The script `simulate_dataset.py` is used to generate this sample data. This should then be published to Snowflake 

Running the project
1. In `simulate_dataset.py`, fill the config params with the appropriate values
```USER_COUNT = 10000
PRODUCT_COUNT = 100
START_TIME = datetime.datetime(2023, 10, 1, 0, 0, 0) # The simulated journeys start from this time till DAYS_TO_SIMULATE days. 
DAYS_TO_SIMULATE = 180 ```
2. Run `python simulate_dataset.py` (you may need to setup the environment first with requirements.txt)

This creates a few csv files in the same directory. These files need to be uploaded to Snowflake in the appropriate place. 
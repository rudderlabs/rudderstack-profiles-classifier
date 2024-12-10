SAMPLE_DATA_DIR = "sample_data"
TABLE_SUFFIX = "PB_TUTORIAL"  # ToDo: Make it human readable, or let user modify it
CONFIG_FILE_PATH = "profiles/pb_project.yaml"
INPUTS_FILE_PATH = "profiles/models/inputs.yaml"
PROFILES_FILE_PATH = "profiles/models/profiles.yaml"
PREDEFINED_ID_TYPES = [
    "anon_id",
    "email",
    "user_id",
    "device_id",
    "shopify_store_id",
    "shopify_customer_id",
]
ID_GRAPH_MODEL_SUFFIX = "id_graph"
PRE_DEFINED_FEATURES = [
    {
        "name": "number_of_devices_purchased",
        "select": "count(distinct device_id)",
        "from": "inputs/rsTracks_pb_tutorial",
        "description": "total number of devices bought by each customer",
    },
    {
        "name": "last_order_date",
        "select": "max(timestamp)",
        "from": "inputs/rsTracks_pb_tutorial",
        "description": "timestamp of most recent order per customer",
    },
    {
        "name": "days_since_last_order",
        "select": "datediff(day, {{user.last_order_date}}, current_date)",
        "description": "timestamp of most recent order per customer",
    },
]
USER_DEFINED_FEATURES = [
    {
        "name": "account_creation_date",
        "select": "min(TIMESTAMP)",
        "from": "inputs/rsIdentifies_pb_tutorial",
        "description": "account creation date for each customer",
        "user_prompt": "From our source data, we know that the account creation is coming from inputs/rsIdentifies_pb_tutorial. We want to select the first timestamp per user from this source table. We will use a min() function in order to do this. ",
    },
    {
        "name": "last_seen_timestamp",
        "select": "last_value(timestamp)",
        "from": "inputs/rsPages_pb_tutorial",
        "window": {
            "order_by": ["timestamp asc"],
        },
        "user_prompt": "We will now create a feature that uses a window function to show you the structure. Let's now create the last_seen_date.",
        "order_by_prompt": "Given that profiles will automatically partion the user by the user_main_id, let's now order this partition in the correct order so that we can ensure that the timestamp we are selecting within this partition is indeed the last event record with the last timestamp, per user.",
    },
    {
        "name": "total_revenue",
        "select": "sum(INVOICE_COST)",
        "from": "inputs/rsTracks_pb_tutorial",
        "description": "total revenue per user",
        "user_prompt": "Now, we want to build a feature that outputs the total revenue for each user. Here, we will use a sum() function",
    },
]

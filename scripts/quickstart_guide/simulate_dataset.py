from faker import Faker
import pandas as pd

import hashlib

import random
import string
import json
import random
import datetime
from dataclasses import dataclass, asdict

# tracks = pd.read_csv("/Users/dileep/Downloads/autotrack_tracks.csv")
# pages = pd.read_csv("/Users/dileep/Downloads/autotrack_pages.csv")
# identifies = pd.read_csv("autotrack_identifies.csv")

# Config - modify no:of users we need in the output data, no:of products, the start date, and days to simulate.
USER_COUNT = 10000
PRODUCT_COUNT = 100
START_TIME = datetime.datetime(
    2024, 5, 1, 0, 0, 0
)  # The simulated journeys start from this time till DAYS_TO_SIMULATE days.
DAYS_TO_SIMULATE = 600
# printing the config details
print(f"USER_COUNT: {USER_COUNT}")
print(f"PRODUCT_COUNT: {PRODUCT_COUNT}")
print(f"START_TIME: {START_TIME}")
print(f"DAYS_TO_SIMULATE: {DAYS_TO_SIMULATE}")

# def get_columns(df):
#     cols = {"context": [], "others": []}
#     for col in list(df):
#         if col.startswith("CONTEXT"):
#             cols["context"].append(col)
#         else:
#             cols["others"].append(col)
#     return cols

# ID columns in tracks, pages, identifies, and checkout tables
sl_id_cols = """uuid_ts
user_id
timestamp
sent_at
received_at
original_timestamp
last_name
id
first_name
email
state
address_country
currency
context_session_start
context_session_id
context_screen_height
context_screen_width
context_screen_density
context_screen_inner_height
context_screen_inner_width
context_page_url
context_library_version
context_ip
context_campaign_term
context_campaign_source
context_campaign_name
context_campaign_medium
context_campaign_content
anonymous_id""".split(
    "\n"
)

sl_tracks_cols = """ANONYMOUS_ID
CHANNEL
EVENT
EVENT_TEXT
ID
ORIGINAL_TIMESTAMP
RECEIVED_AT
SENT_AT
TIMESTAMP
USER_ID
UUID_TS
CONTEXT_SESSION_ID
CONTEXT_DESTINATION_ID
CONTEXT_DESTINATION_TYPE
CONTEXT_CAMPAIGN_MEDIUM
CONTEXT_CAMPAIGN_CONTENT
CONTEXT_CAMPAIGN_NAME
CONTEXT_APP_NAMESPACE
CONTEXT_CAMPAIGN_SOURCE""".lower().split(
    "\n"
)

sl_pages_cols = """id
anonymous_id
user_id
received_at
sent_at
original_timestamp
timestamp
context_ip
name
category
path
title
url
uuid_ts
CONTEXT_SESSION_ID
CONTEXT_DESTINATION_ID
CONTEXT_DESTINATION_TYPE
CONTEXT_CAMPAIGN_MEDIUM
CONTEXT_CAMPAIGN_CONTENT
CONTEXT_APP_NAMESPACE
CONTEXT_CAMPAIGN_SOURCE
CONTEXT_CAMPAIGN_NAME
CONTEXT_PAGE_PATH
CONTEXT_PAGE_REFERRER
CONTEXT_PAGE_REFERRING_DOMAIN""".lower().split(
    "\n"
)

sl_checkout = [
    "anonymous_id",
    "tax",
    "currency",
    "channel",
    "received_at",
    "event_text",
    "uuid_ts",
    "country_code",
    "user_id",
    "locale",
    "timestamp",
    "shipping",
    "revenue",
    "event",
    "products",
    "payment_method",
    "id",
    "order_id",
    "original_timestamp",
    "sent_at",
    "checkout_id",
    "cart_id",
    "context_page_url",
    "context_session_id",
    "context_screen_height",
    "context_screen_width",
    "context_screen_density",
    "context_screen_inner_height",
    "context_screen_inner_width",
    "context_ip",
    "context_session_start",
    "total",
]


# Utils for generating user data, product data etc.
@dataclass
class User:
    first_name: str
    last_name: str
    email: str
    anonymous_id: str
    last_seen_time: datetime.datetime


fake = Faker()


def generate_random_string(length):
    # Define the characters that can be used in the string
    characters = string.ascii_letters + string.digits
    # Generate a random string
    random_string = "".join(random.choice(characters) for i in range(length))
    return random_string


# Example usage
random_str = generate_random_string(10)  # Generate a random string of length 10


webpage = "www.choixcart.com"


def generate_email(fn, ln):
    email_rn = random.randint(0, 2)
    if email_rn == 0:
        email = f"{fn.lower()}.{ln.lower()}{fake.numerify()}@example.{fake.random_element(elements=('com','org', 'net'))}"
    elif email_rn == 1:
        email = f"{ln.lower()}.{fn.lower()}{fake.numerify()}@example.org"
    else:
        email = f"{fn.lower()}{ln.lower()}@example.com"
    return email


def get_timestamp(year, month, date, hour, minute, second):
    return datetime.datetime(year, month, date, hour, minute, second)


def get_timestamp_string(timestamp):
    return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def generate_product_data():
    product_ids = random.sample(
        range(10 * PRODUCT_COUNT, 100 * PRODUCT_COUNT), PRODUCT_COUNT
    )
    products = []
    for product_id in product_ids:
        # name = f"{fake.catch_phrase()} {fake.color_name()}"
        name = None
        cat = fake.random_element(
            elements=(
                "Watches",
                "Trousers",
                "Shoes",
                "Formal Shoes",
                "Mens clothing",
                "Shirts",
                "Jeans",
            )
        )
        path = f"/p/{product_id}"
        url = f"{webpage}{path}"
        product = {"title": name, "category": cat, "path": path, "url": url}
        products.append(product)
    return products


sample_user = User("John", "Doe", "jhondoe@email.com", "adfas", None)


def generate_random_ts():
    ts = START_TIME + (
        datetime.timedelta(days=random.randint(0, DAYS_TO_SIMULATE))
        + datetime.timedelta(hours=random.randint(0, 23))
        + datetime.timedelta(minutes=random.randint(0, 59))
        + datetime.timedelta(seconds=random.randint(0, 59))
    )
    return ts
    # return get_timestamp(2024, random.randint(4,12), random.randint(1, 30), random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))


## Sample Data Store

# device_cols = [col for col in list(identifies) if col.startswith("CONTEXT_SCREEN")]
# device_specs = identifies[device_cols].drop_duplicates().sample(10).to_dict(orient='records')
device_cols = [
    "CONTEXT_SCREEN_DENSITY",
    "CONTEXT_SCREEN_WIDTH",
    "CONTEXT_SCREEN_HEIGHT",
    "CONTEXT_SCREEN_INNER_HEIGHT",
    "CONTEXT_SCREEN_INNER_WIDTH",
]
device_specs = [
    {
        "CONTEXT_SCREEN_DENSITY": 1,
        "CONTEXT_SCREEN_WIDTH": 800,
        "CONTEXT_SCREEN_HEIGHT": 600,
        "CONTEXT_SCREEN_INNER_HEIGHT": 1024,
        "CONTEXT_SCREEN_INNER_WIDTH": 1280,
    },
    {
        "CONTEXT_SCREEN_DENSITY": 2,
        "CONTEXT_SCREEN_WIDTH": 2560,
        "CONTEXT_SCREEN_HEIGHT": 1440,
        "CONTEXT_SCREEN_INNER_HEIGHT": 1319,
        "CONTEXT_SCREEN_INNER_WIDTH": 2560,
    },
    {
        "CONTEXT_SCREEN_DENSITY": 1,
        "CONTEXT_SCREEN_WIDTH": 385,
        "CONTEXT_SCREEN_HEIGHT": 854,
        "CONTEXT_SCREEN_INNER_HEIGHT": 726,
        "CONTEXT_SCREEN_INNER_WIDTH": 384,
    },
    {
        "CONTEXT_SCREEN_DENSITY": 1,
        "CONTEXT_SCREEN_WIDTH": 1280,
        "CONTEXT_SCREEN_HEIGHT": 720,
        "CONTEXT_SCREEN_INNER_HEIGHT": 551,
        "CONTEXT_SCREEN_INNER_WIDTH": 1280,
    },
    {
        "CONTEXT_SCREEN_DENSITY": 1,
        "CONTEXT_SCREEN_WIDTH": 1920,
        "CONTEXT_SCREEN_HEIGHT": 1080,
        "CONTEXT_SCREEN_INNER_HEIGHT": 969,
        "CONTEXT_SCREEN_INNER_WIDTH": 1920,
    },
    {
        "CONTEXT_SCREEN_DENSITY": 1,
        "CONTEXT_SCREEN_WIDTH": 1920,
        "CONTEXT_SCREEN_HEIGHT": 1080,
        "CONTEXT_SCREEN_INNER_HEIGHT": 993,
        "CONTEXT_SCREEN_INNER_WIDTH": 1920,
    },
    {
        "CONTEXT_SCREEN_DENSITY": 1,
        "CONTEXT_SCREEN_WIDTH": 1600,
        "CONTEXT_SCREEN_HEIGHT": 900,
        "CONTEXT_SCREEN_INNER_HEIGHT": 739,
        "CONTEXT_SCREEN_INNER_WIDTH": 1600,
    },
    {
        "CONTEXT_SCREEN_DENSITY": 2,
        "CONTEXT_SCREEN_WIDTH": 1800,
        "CONTEXT_SCREEN_HEIGHT": 1169,
        "CONTEXT_SCREEN_INNER_HEIGHT": 858,
        "CONTEXT_SCREEN_INNER_WIDTH": 1340,
    },
    {
        "CONTEXT_SCREEN_DENSITY": 1,
        "CONTEXT_SCREEN_WIDTH": 1536,
        "CONTEXT_SCREEN_HEIGHT": 864,
        "CONTEXT_SCREEN_INNER_HEIGHT": 738,
        "CONTEXT_SCREEN_INNER_WIDTH": 1522,
    },
    {
        "CONTEXT_SCREEN_DENSITY": 3,
        "CONTEXT_SCREEN_WIDTH": 360,
        "CONTEXT_SCREEN_HEIGHT": 780,
        "CONTEXT_SCREEN_INNER_HEIGHT": 649,
        "CONTEXT_SCREEN_INNER_WIDTH": 360,
    },
]


campaign_samples = [
    {
        "context_campaign_term": "christmas sale",
        "context_campaign_source": "google",
        "context_campaign_name": "cmpn_232",
        "context_campaign_medium": "ppc",
        "context_campaign_content": "53454fasdf",
    },
    {
        "context_campaign_term": "brand launch",
        "context_campaign_source": "email",
        "context_campaign_name": "cmpn_421",
        "context_campaign_medium": "email",
        "context_campaign_content": "53dsvdsa",
    },
    {
        "context_campaign_term": "abandon cart",
        "context_campaign_source": "twitter",
        "context_campaign_name": "cmpn_125",
        "context_campaign_medium": "paidsocial",
        "context_campaign_content": "gts23ds",
    },
    {
        "context_campaign_term": "new users",
        "context_campaign_source": "google",
        "context_campaign_name": "cmpn_533",
        "context_campaign_medium": "ppc",
        "context_campaign_content": "gfaa4rvdsa",
    },
    {
        "context_campaign_term": "abandon cart",
        "context_campaign_source": "email",
        "context_campaign_name": "cmpn_125",
        "context_campaign_medium": "email",
        "context_campaign_content": "gsf4dsgs",
    },
    {
        "context_campaign_term": "paid users",
        "context_campaign_source": "bing",
        "context_campaign_name": "cmpn_830",
        "context_campaign_medium": "ppc",
        "context_campaign_content": "cdasareravf",
    },
    {
        "context_campaign_term": "christmas sale",
        "context_campaign_source": "bing",
        "context_campaign_name": "cmpn_232",
        "context_campaign_medium": "ppc",
        "context_campaign_content": "53454fasdf",
    },
    {
        "context_campaign_term": "paid users",
        "context_campaign_source": "google",
        "context_campaign_name": "cmpn_830",
        "context_campaign_medium": "ppc",
        "context_campaign_content": "cdasareravf",
    },
    {
        "context_campaign_term": "topoffers",
        "context_campaign_source": "email",
        "context_campaign_name": "cmpn_90",
        "context_campaign_medium": "email",
        "context_campaign_content": "4sdf4sda",
    },
    {
        "context_campaign_term": "similar items",
        "context_campaign_source": "twitter",
        "context_campaign_name": "cmpn_80",
        "context_campaign_medium": "paidsocial",
        "context_campaign_content": "13vfs5asdfa",
    },
]

states_list = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
]


products = generate_product_data()
product_prices = [random.randint(10, 200) for _ in range(PRODUCT_COUNT)]

lib_versions = ["1.1.4", "0.10.4", "1.0.2"]
urls = {
    "home": webpage,
    "cart": f"{webpage}/cart/view.html?ref_=nav_cart",
    "order_complete": f"{webpage}/order/success",
}
# domains = {"$direct": None, "www.google.com": "google", "www.duckduckgo.com": "duckduckgo.com", "https://www.bing.com": "bing.com"}

# referrers = {"campaign_source": ["referrer", "referrer_domain"]}
referrers = {
    "email": ["$direct", None],
    "bing": ["https://www.bing.com", "bing.com"],
    "google": ["www.google.com", "google"],
    "twitter": ["www.twitter.com", "twitter.com"],
}

destination_id = generate_random_string(14)
destination_type = "snowflake"


# Functions to generate events
def identify(
    user, event_time, campaign_id, device_id, ip, st_time, lib_version, url
) -> dict:
    event = {}
    if not user.email:
        user.email = generate_email(user.first_name, user.last_name)
    user_data = asdict(user)
    for key in sl_id_cols:
        if key in user_data:
            event[key] = user_data[key]
        elif "timestamp" in key or key in ("uuid_ts", "sent_at", "received_at"):
            event[key] = get_timestamp_string(event_time)
        elif "campaign" in key:
            if campaign_id is None:
                event[key] = None
            else:
                event[key] = campaign_samples[campaign_id][key]
        elif "screen" in key:
            event[key] = device_specs[device_id][key.upper()]
    event["user_id"] = user.email
    event["context_ip"] = ip
    event["id"] = generate_random_string(16)
    event["context_session_start"] = get_timestamp_string(st_time)
    event["context_session_id"] = int(datetime.datetime.timestamp(st_time))
    event["context_library_version"] = lib_version
    event["context_page_url"] = url
    event["state"] = fake.random_element(elements=states_list)
    event["address_country"] = "US"
    event["currency"] = "usd"
    return event


def track(user, event_time, st_time, campaign_id, event_name) -> dict:
    event = {}
    user_data = asdict(user)
    for key in sl_tracks_cols:
        if key in user_data:
            event[key] = user_data[key]
        elif "timestamp" in key or key in ("uuid_ts", "sent_at", "received_at"):
            event[key] = get_timestamp_string(event_time)
        elif "campaign" in key:
            if campaign_id is None:
                event[key] = None
            else:
                event[key] = campaign_samples[campaign_id][key]
    event["context_app_namespace"] = "com.rudderstack.javascript"
    event["context_destination_id"] = destination_id
    event["context_destination_type"] = destination_type
    event["context_session_id"] = int(datetime.datetime.timestamp(st_time))
    event["user_id"] = user.email
    event["id"] = generate_random_string(16)
    event["channel"] = "web"
    event["event_text"] = event_name.upper().replace("_", " ")
    event["event"] = event_name
    return event


def page(
    user: User,
    event_time: str,
    st_time: str,
    campaign_id: int,
    page_name: int,
    path: str,
    referrer_path: str,
    ip: str,
    product_id=None,
):
    event = {}
    event["id"] = generate_random_string(16)
    event["context_ip"] = ip
    event["name"] = page_name
    event["path"] = path
    event["context_session_id"] = int(datetime.datetime.timestamp(st_time))
    if path:
        event["url"] = f"{webpage}{path}"
    else:
        event["url"] = webpage
    event["context_destination_id"] = destination_id
    event["context_destination_type"] = destination_type
    event["context_app_namespace"] = "com.rudderstack.javascript"
    if campaign_id:
        campaign_source = campaign_samples[campaign_id]["context_campaign_source"]
        event["context_page_referrer"] = referrers[campaign_source][0]
        event["context_page_referring_domain"] = referrers[campaign_source][1]
    else:
        event["context_page_referrer"] = None
        event["context_page_referring_domain"] = None
    event["context_page_path"] = referrer_path
    user_data = asdict(user)
    for key in sl_pages_cols:
        if key in user_data:
            event[key] = user_data[key]
        if key == "user_id":
            event[key] = user.email
        elif "timestamp" in key or key in ("uuid_ts", "sent_at", "received_at"):
            event[key] = get_timestamp_string(event_time)
        elif "campaign" in key:
            if campaign_id is None:
                event[key] = None
            else:
                event[key] = campaign_samples[campaign_id][key]
    if not product_id:
        event["category"] = None
        event["title"] = None
    else:
        for k, v in products[product_id].items():
            event[k] = v
    return event


def order_complete_event(
    user, product_info, event_time, device_id, ip, st_time, page_url
):
    event = {}
    user_data = asdict(user)
    event["context_ip"] = ip
    event["context_session_start"] = get_timestamp_string(st_time)
    event["context_session_id"] = int(datetime.datetime.timestamp(st_time))
    event["context_page_url"] = page_url
    event["cart_id"] = generate_random_string(20)
    event["checkout_id"] = generate_random_string(10)
    event["order_id"] = generate_random_string(12)
    event["id"] = generate_random_string(14)
    event["payment_method"] = fake.random_element(
        elements=("paypal", "amex", "visa", "mastercard")
    )
    event["event"] = "order_complete"
    event["locale"] = fake.random_element(elements=("us", "gb", "it", "ca"))
    event["currency"] = "usd"
    event["channel"] = "web"
    event["event_text"] = "ORDER COMPLETED"
    event["country_code"] = event["locale"]
    for key in sl_checkout:
        if key in user_data:
            event[key] = user_data[key]
        elif "timestamp" in key or key in ("uuid_ts", "sent_at", "received_at"):
            event[key] = get_timestamp_string(event_time)
        elif "screen" in key:
            event[key] = device_specs[device_id][key.upper()]
    event["user_id"] = user.email
    event["products"] = json.dumps([product_info])
    price = product_info["price"]
    revenue, tax, shipping = price * 0.8, price * 0.1, price * 0.1
    event["revenue"] = round(revenue, 2)
    event["tax"] = round(tax, 2)
    event["shipping"] = round(shipping, 2)
    event["total"] = product_info["price"] * 0.9
    return event


st_time_ = generate_random_ts()
# Sample page call
samp_page_event = page(
    user=sample_user,
    event_time=st_time_,
    st_time=st_time_,
    campaign_id=0,
    page_name="home-page",
    path="/c/shoes",
    referrer_path="/c/categories",
    ip=fake.ipv4(),
    product_id=4,
)

try:
    assert [col for col in samp_page_event if col not in sl_pages_cols] == []
    assert [col for col in sl_pages_cols if col not in samp_page_event] == []
except AssertionError:
    print([col for col in sl_pages_cols if col not in samp_page_event])
    print([col for col in samp_page_event if col not in sl_pages_cols])
    raise AssertionError

campaign_id_ = 0
device_id_ = 2
ip_ = fake.ipv4()
# Sample track call
track_event = track(sample_user, st_time_, st_time_, campaign_id_, "order_completed")
try:
    assert [col for col in track_event if col not in sl_tracks_cols] == []
    assert [col for col in sl_tracks_cols if col not in track_event] == []
except AssertionError:
    print([col for col in sl_tracks_cols if col not in track_event])
    print([col for col in track_event if col not in sl_tracks_cols])
    raise AssertionError

id_event = identify(
    sample_user, st_time_, 0, 2, ip_, st_time_, lib_versions[0], webpage
)

try:
    assert [col for col in id_event if col not in sl_id_cols] == []
    assert [col for col in sl_id_cols if col not in id_event] == []
except AssertionError:
    print([col for col in sl_id_cols if col not in id_event])
    print([col for col in id_event if col not in sl_id_cols])
    raise AssertionError

p_ = {"product_id": 4, "name": "Nike Air Max", "price": 100, "quantity": 1}

checkout_event = order_complete_event(
    sample_user, p_, st_time_, 0, fake.ipv4(), st_time_, "asdf"
)

try:
    assert [col for col in checkout_event if col not in sl_checkout] == []
    assert [col for col in sl_checkout if col not in checkout_event] == []
except AssertionError:
    print([col for col in sl_checkout if col not in checkout_event])
    print([col for col in checkout_event if col not in sl_checkout])
    raise AssertionError

del st_time_, campaign_id_, device_id_, ip_, p_

cart_add_path = "/cart/view.html?ref=nav_cart"
signup_path = "/signup"
login_path = "/login"
order_complete_path = "/order/success"
paths = {
    "signup": signup_path,
    "login": login_path,
    "order_complete": order_complete_path,
    "cart_add": cart_add_path,
}


def product_page_views(
    user, max_events, event_time, st_time, campaign_id, referrer_path, ip
):
    events = []
    for _ in range(random.randint(1, max_events)):
        product_id = fake.random_int(min=0, max=(len(products) - 1))
        product_event = page(
            user,
            event_time,
            st_time,
            campaign_id,
            "product-view",
            None,
            referrer_path,
            ip,
            product_id,
        )
        referrer_path = products[product_id]["path"]
        events.append({"page": product_event})
        event_time = (
            event_time
            + datetime.timedelta(minutes=random.randint(6, 60))
            + datetime.timedelta(seconds=random.randint(10, 600))
        )
    return events, event_time, referrer_path, product_id


def signin(
    user,
    event,
    event_time,
    session_start_time,
    campaign_id,
    device_id,
    referrer_path,
    ip,
):
    assert event in ("signup", "login")
    # Add signup event/login event
    event_time = event_time + datetime.timedelta(seconds=random.randint(0, 120))
    lib_version = fake.random_element(elements=lib_versions)
    signin_track = track(user, event_time, session_start_time, campaign_id, event)
    identify_event = identify(
        user,
        event_time,
        campaign_id,
        device_id,
        ip,
        session_start_time,
        lib_version,
        f"{webpage}{paths[event]}",
    )
    page_event = page(
        user,
        event_time,
        session_start_time,
        campaign_id,
        event,
        paths[event],
        referrer_path,
        ip,
    )
    signin_events = [
        {"page": page_event},
        {"track": signin_track},
        {"identify": identify_event},
    ]
    referrer_path = signin_events
    return event_time, referrer_path, signin_events


def cart_add(user, event_time, st_time, campaign_id, referrer_path, ip, product_id):
    event_time = event_time + datetime.timedelta(minutes=random.randint(10, 120))
    cart_add_page_view = page(
        user,
        event_time,
        st_time,
        campaign_id,
        "cart-add",
        cart_add_path,
        referrer_path,
        ip,
        product_id,
    )
    cart_add_track = track(user, event_time, st_time, campaign_id, "cart_add")
    referrer_path = cart_add_path
    return (
        event_time,
        referrer_path,
        [{"page": cart_add_page_view}, {"track": cart_add_track}],
    )


def order_completed_event(
    user, event_time, campaign_id, referrer_path, ip, device_id, st_time, product_id
):
    event_time = (
        event_time
        + datetime.timedelta(minutes=random.randint(10, 600))
        + datetime.timedelta(seconds=random.randint(60, 600))
    )
    order_completed_track = track(
        user, event_time, st_time, campaign_id, "order_completed"
    )
    order_completed_page = page(
        user,
        event_time,
        st_time,
        campaign_id,
        "order_completed",
        order_complete_path,
        referrer_path,
        ip,
    )
    product_info = {**products[product_id], "price": product_prices[product_id]}
    order_completed_event = order_complete_event(
        user,
        product_info,
        event_time,
        device_id,
        ip,
        st_time,
        f"{webpage}{referrer_path}",
    )
    referrer_path = order_complete_path
    return (
        event_time,
        referrer_path,
        [
            {"page": order_completed_page},
            {"track": order_completed_track},
            {"order_completed": order_completed_event},
        ],
    )


def simulate_journey(user, journey_type):
    """
    **Journeys:**
    1. home page, product page * n
    2. home page, product page * n,  cart add
    3. home page, product page * n, cart add, sign up, purchase
    4. home page, product page * n, cart add, login, purchase
    5. home page, sign up, product page * n
    6. home page, sign up, product page * n, cart add
    7. home page, sign up, product page * n, cart add, purchase
    8. login, product page, cart add, purchase
    9. product page, product page, cart add
    10. product page, product page, cart add, login, purchase
    """
    # assert journey_type in ("onboard", "browse", "cart_add", "purchase")
    if not user.last_seen_time:
        st_time = generate_random_ts()
        # user.last_seen_time = st_time
    else:
        st_time = (
            user.last_seen_time
            + datetime.timedelta(days=random.randint(14, 60))
            + datetime.timedelta(minutes=random.randint(0, 600))
        )
    ip = fake.ipv4()
    if random.random() <= 0.2:
        campaign_id = fake.random_int(min=0, max=(len(campaign_samples) - 1))
    else:
        campaign_id = None
    device_id = fake.random_int(min=0, max=(len(device_specs) - 1))
    if journey_type <= 7:
        events = [
            {
                "page": page(
                    user, st_time, st_time, campaign_id, "home-page", None, None, ip
                )
            }
        ]
        event_time = st_time + datetime.timedelta(days=random.randint(3, 21))
        referrer_path = None
        if journey_type <= 4:
            n_products_viewed_max = 5
            product_events, event_time, referrer_path, product_id = product_page_views(
                user,
                n_products_viewed_max,
                event_time,
                st_time,
                campaign_id,
                referrer_path,
                ip,
            )
            events.extend(product_events)
            if journey_type > 1:
                event_time, referrer_path, cart_add_events = cart_add(
                    user,
                    event_time,
                    st_time,
                    campaign_id,
                    referrer_path,
                    ip,
                    product_id,
                )
                events.extend(cart_add_events)
                if journey_type > 2:
                    # signup if jounrey type is 3, login if journey type is 4
                    event_time, referrer_path, login_events = signin(
                        user,
                        "login" if journey_type == 4 else "signup",
                        event_time,
                        st_time,
                        campaign_id,
                        device_id,
                        referrer_path,
                        ip,
                    )
                    events.extend(login_events)
                    # Add purchase event
                    event_time, referrer_path, order_complete_events = (
                        order_completed_event(
                            user,
                            event_time,
                            campaign_id,
                            referrer_path,
                            ip,
                            device_id,
                            st_time,
                            product_id,
                        )
                    )
                    events.extend(order_complete_events)
        else:
            # Add signup event followed by product page views
            event_time, referrer_path, signup_events = signin(
                user,
                "signup",
                event_time,
                st_time,
                campaign_id,
                device_id,
                referrer_path,
                ip,
            )
            events.extend(signup_events)
            n_products_viewed_max = 3
            product_events, event_time, referrer_path, product_id = product_page_views(
                user,
                n_products_viewed_max,
                event_time,
                st_time,
                campaign_id,
                referrer_path,
                ip,
            )
            if journey_type > 5:
                # If journey type is 6 or 7, add cart add event
                event_time, referrer_path, cart_add_events = cart_add(
                    user,
                    event_time,
                    st_time,
                    campaign_id,
                    referrer_path,
                    ip,
                    product_id,
                )
                events.extend(cart_add_events)
            if journey_type == 7:
                # Add purchase event
                event_time, referrer_path, order_complete_events = (
                    order_completed_event(
                        user,
                        event_time,
                        campaign_id,
                        referrer_path,
                        ip,
                        device_id,
                        st_time,
                        product_id,
                    )
                )
                events.extend(order_complete_events)
    elif journey_type == 8:
        # login, product page, cart add, purchase
        events = []
        referrer_path = None
        event_time, referrer_path, login_events = signin(
            user, "login", st_time, st_time, campaign_id, device_id, referrer_path, ip
        )
        product_events, event_time, referrer_path, product_id = product_page_views(
            user, 1, event_time, st_time, campaign_id, referrer_path, ip
        )
        event_time, referrer_path, cart_add_events = cart_add(
            user, event_time, st_time, campaign_id, referrer_path, ip, product_id
        )
        event_time, referrer_path, order_complete_events = order_completed_event(
            user,
            event_time,
            campaign_id,
            referrer_path,
            ip,
            device_id,
            st_time,
            product_id,
        )
        events.extend(
            [*login_events, *product_events, *cart_add_events, *order_complete_events]
        )
    elif journey_type >= 9:
        # product page, product page, cart add
        referrer_path = None
        events = []
        product_events, event_time, referrer_path, product_id = product_page_views(
            user, 2, st_time, st_time, campaign_id, referrer_path, ip
        )
        event_time, referrer_path, cart_add_events = cart_add(
            user, event_time, st_time, campaign_id, referrer_path, ip, product_id
        )
        events.extend([*product_events, *cart_add_events])
        if journey_type == 10:
            # Add login, purchase
            event_time, referrer_path, login_events = signin(
                user,
                "login",
                event_time,
                st_time,
                campaign_id,
                device_id,
                referrer_path,
                ip,
            )
            event_time, referrer_path, order_complete_events = order_completed_event(
                user,
                event_time,
                campaign_id,
                referrer_path,
                ip,
                device_id,
                st_time,
                product_id,
            )
            events.extend([*login_events, *order_complete_events])
    user.last_seen_time = event_time
    return events


journey_dist = {
    1: 0.35,
    2: 0.13,
    3: 0.04,
    4: 0.04,
    5: 0.05,
    6: 0.13,
    7: 0.04,
    8: 0.04,
    9: 0.14,
    10: 0.04,
}

# Extract keys and corresponding weights
keys = list(journey_dist.keys())
weights = list(journey_dist.values())


users = []
for i in range(USER_COUNT):
    fn = fake.first_name()
    ln = fake.last_name()
    # email = generate_email(fn, ln)
    anon = hashlib.md5(f"{fn}{ln}{random.randint(0,1000)}".encode("utf-8")).hexdigest()
    users.append(User(fn, ln, None, anon, None))

journeys = []
for i in range(USER_COUNT):
    journey_id = random.choices(keys, weights, k=1)[0]
    n_journeys = random.randint(1, 6)
    for _ in range(n_journeys):
        if journey_id in (8, 10):
            journeys.append(
                simulate_journey(users[i], fake.random_element(elements=(5, 6)))
            )
        journeys.append(simulate_journey(users[i], journey_id))
        if journey_id == 3:
            journeys.append(
                simulate_journey(users[i], fake.random_element(elements=(4, 8, 9)))
            )

tracks_events = []
pages_events = []
identifies_events = []
order_completed_events = []
for journey in journeys:
    for event in journey:
        if "track" in event:
            tracks_events.append(event["track"])
        elif "page" in event:
            pages_events.append(event["page"])
        elif "identify" in event:
            identifies_events.append(event["identify"])
        elif "order_completed" in event:
            order_completed_events.append(event["order_completed"])

tracks = pd.DataFrame(tracks_events)
pages = pd.DataFrame(pages_events)
identifies = pd.DataFrame(identifies_events)
order_completed = pd.DataFrame(order_completed_events)

tracks.to_csv("tracks.csv", index=False)
pages.to_csv("pages.csv", index=False)
identifies.to_csv("identifies.csv", index=False)
order_completed.to_csv("order_completed.csv", index=False)

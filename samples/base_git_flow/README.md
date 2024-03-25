# About

This is a Profiles library project to create user features from event stream tables created using Rudderstack SDK

# Inputs
## Raw Tables
| name | table | path |
| ---- | ----- | ---- |
| rsIdentifies | IDENTIFIES | RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.IDENTIFIES |
| rsTracks | TRACKS | RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.TRACKS |
| rsPages | PAGES | RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.PAGES |
## SQL Models
| name |
| ---- |
| rsTracksUnionPages |
# Identity Stitching
## user identities
| name | exclusions | sourced_from |
| ---- | ---------- | ------------ |
| user_id |  | ["rsIdentifies:user_id","rsTracks:user_id","rsPages:user_id"] |
| cart_token |  | [] |
| anonymous_id | unknown, NaN | ["rsIdentifies:anonymous_id","rsTracks:anonymous_id","rsPages:anonymous_id"] |
| email | test@company.com | ["rsIdentifies:lower(email)"] |
# Features
## user features
| Feature | Computed From | Description |
| ------- | ------------- | ----------- |
| active_days_in_past_365_days | rsTracksUnionPages | Out of 365 days, how many days have recorded an event till date including today |
| active_days_in_past_7_days | rsTracksUnionPages | Out of 7 days, how many days have recorded an event till date including today |
| avg_session_length_in_sec_365_days | rsTracksUnionPages | Average session length (in seconds) of all the user sessions that started in last 365 days |
| avg_session_length_in_sec_last_week | rsTracksUnionPages | Average session length (in seconds) of all the user sessions that started in last 7 days |
| avg_session_length_in_sec_overall | rsTracksUnionPages | Average session length (in seconds) of all the user sessions till date. |
| campaign_sources | rsIdentifies |  |
| campaigns_list | rsPages | list of all campaigns from which a user has visited the app, sorted in chronological order, from oldest to newest |
| country | rsIdentifies |  |
| currency | rsIdentifies |  |
| days_since_account_creation | rsIdentifies |  |
| days_since_last_seen |  |  |
| device_manufacturer | rsIdentifies |  |
| device_name | rsIdentifies |  |
| device_type | rsIdentifies |  |
| first_campaign_name | rsPages | First campaign from which a user has visited the app |
| first_name | rsIdentifies |  |
| first_seen_date | rsTracksUnionPages | The first date on which an event has been recorded by the user |
| first_source_name | rsPages | First source from which a user has visited the app |
| has_mobile_app | rsIdentifies |  |
| is_active_on_website | rsIdentifies |  |
| is_churned_7_days |  | Depending on the n value, it specifies if there is any activity observed in the last 7 days. |
| last_campaign_name | rsPages | Latest campaign from which a user has visited the app |
| last_name | rsIdentifies |  |
| last_seen_date | rsTracksUnionPages | The latest date on which an event has been recorded by the user |
| last_source_name | rsPages | Last source from which a user has visited the app |
| mediums_list | rsPages | list of all mediums from which a user has visited the app, sorted in chronological order, from oldest to newest |
| sources_list | rsPages | list of all sources from which a user has visited the app, sorted in chronological order, from oldest to newest |
| state | rsIdentifies |  |
| total_sessions_365_days | rsTracksUnionPages | total number of sessions over last 356 days. |
| total_sessions_90_days | rsTracksUnionPages | total number of sessions over last 90 days. |
| total_sessions_last_week | rsTracksUnionPages | total number of sessions over last 7 days. |
| total_sessions_till_date | rsTracksUnionPages | Total individual sessions created till date. |

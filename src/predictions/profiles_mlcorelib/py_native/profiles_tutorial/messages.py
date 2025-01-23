from typing import Final, Callable


WELCOME_MESSAGE: Final[
    str
] = """
    This is a guided interactive tutorial on Rudderstack Profiles. This tutorial will walk through key concepts of profiles and how it works. 
    As a part of this tutorial, we will also build a basic project with an ID Stitcher Model ultimately producing an ID Graph in your warehouse.
    This tutorial will have multiple interactive components. Most of the interaction will take place within your terminal or command line. 
    However, we will prompt you to interact with a yaml configuration directly.
    And of course, we are producing output tables and views based on a fictional business in your output schema in your warehouse that you can query directly in your DBMS console or preferred IDE.
    The goal of this tutorial is to familiarize you with the profiles product in the rudderstack platform.
    This includes details on the yaml configuration, how data unification works, what the outputs look like after a run, and how to troubleshoot and build a solid ID Graph around a user entity. 
    Our goal is that you can immediately put this knowledge to action with your own data by building your own profiles project and extending it further to unify your data around a defined entity, 
    building a c360 degree view of this entity, and putting that data into action for the benefit of your business!
"""

FICTIONAL_BUSINESS_OVERVIEW: Callable[[bool], str] = (
    lambda fast_mode: f"""
    In a moment, we will seed your warehouse with fictional business data to run the profiles project on during this tutorial. {'(Press Enter to continue)' if not fast_mode else ''}
    The business in this tutorial is `Secure Solutions, LLC`. This fictional business sells security IOT devices as well as a security management subscription service. 
    They have a number of Shopify stores and a subscription management service, and one brick and mortar store where customers can buy security equipment and checkout at a Kiosk. 
    But their pre and post sale messaging to their current and prospective customers are limited because they do not have a great view of their customers and how they interact within their business ecosystem. 
    They also struggle to allocate marketing and campaign money across their ad platforms because they do not have a great view on the user journey and what marketing initiatives are truly successful, and what aren’t. 
    In order to improve their business messaging they have to have a solid 360 degree view of their customers so that they can send the right messages, at the right time, to the right people. 
    Additionally, in order to allocate marketing spend, they have to have solid campaign reporting that builds on this 360 view of the customer. 
    In order to do both of these, Secure Solutions has to build a solid ID Graph around their customer data.
    This tutorial will walk through how to setup a profiles project, bring in source data, and build a solid ID Graph around the customer. 
    Secure Solutions, LLC knows that they have around 319 customers. 171 of which represent known users and the remaining 148 are unknown.
    Meaning, they have not performed any sort of conversion yet.
"""
)


ABOUT_PROFILES_FILES: Callable[[str], str] = (
    lambda connection_name: f"""
    Now let's create a profiles project. 
    A profiles project contains a few yaml files, of following structure:
    ```
    .
    └── <project_directory>
        ├── pb_project.yaml
        └── models
            └── inputs.yaml
            └── profiles.yaml
    ```
    Here's a brief description of what each file is:

    - `pb_project.yaml`: This file contains the project declaration. The name of the project, entity, and the entity's defined id types etc.
        It also includes the warehouse connection name - `{connection_name}`, which calls the connection config we created in the previous step (the same name as in siteconfig.yaml). 
    - `models/inputs.yaml`: This file will contain the input data sources - the tables and columns that map to the entity and their id types. We will explain this in more detail in the subsequent steps.
    - `models/profiles.yaml`: This is where we define the model configurations for the id stitcher and any features/traits you want to build for your defined entity. For the tutorial, we will only build an ID Graph using the ID Stitcher Model Type. 

    These files will be created in this tutorial, with the details you will provide in the next steps. 
    Also, for this tutorial, we will use a directory called `profiles` to store all the files. We will create it here in the current directory.
"""
)

ABOUT_ENTITY: Final[
    str
] = """
    We are now going to define an entity in the pb_project.yaml file around which we want to model the input data. In every company there are business artifacts that are tracked across systems for the purpose of creating a complete data picture of the artifact.
    In Profiles, this artifact is called an entity. 
    `Entities` can be as common as users, accounts, and households. But can expand to be anything that you track across systems and want to gain a complete picture of, such as campaigns, devices or accounts.
    Entities are the central concept that a profiles project is built around. The identity graphs and C360 tables will be built around an entity.
"""

ABOUT_ID_TYPES: Final[
    str
] = """
    You will be collecting different identifiers for this entity, such as email, phone number, user_id, anonymous_id etc.
    For your entity, some of these identifiers may represent anonymous activity while others represent known activity.

    These identifiers form digital footprints across the event tracking systems. An entity is identified by these identifiers. 
    An account may have a domain, account_id, organization_id etc. 
    A product may have sku, product_id, product_name etc.

    For our example, a user may have a few anonymous_ids, one email, and one user_id etc. Part of the profiles building process is qualifying these id values as `id_types`.
    And in a subsequent step, we will do a mapping exercise where we map specific columns from your input tables to these id_types connected to the entity defined above.

    Best practices for defining id_types for an entity is that the values need to be unique to a single instance of your entity. 
    When picking `id_types` consider the granularity of the `entity`. At the user grain, you will want to pick a unique `id_type` of the same grain.
    For higher level grains such as organization or account, you can include user level grain `id_type` as well as org level `id_type`.
    
    Sometimes, these ids may also be very transient, like a session id or an order_id. As long as they uniquely map to one entity (ex: user), you can use them.
    For example, an order_id is likely won't be shared by two users, so it can uniquely identify a user, even if we typically associate order_id with an order.
    This gets very useful when we join data from different systems such as payment systems, marketing campaign systems, order management systems etc.

    In this tutorial, we are going to define a pre-set list of id_types that belong to the customers (user entity) of Secure Solutions, LLC.
    
    We'll go through some common id types one by one. As this is a demo, please type the exact name as suggested.
"""

ABOUT_ID_TYPES_CONCLUSSION: Final[
    str
] = """
    We have now defined an entity called "user" along with the associated id_types that exist across our different source systems. 
    Now, let's move onto bringing in our data sources in order to run the ID Stitcher model and output an ID Graph.
"""


ABOUT_INPUTS: Final[
    str
] = """
    Now, we need to bring in the warehouse tables that have the relevant data for us to build an id graph off of. For this part of the tutorial, we will be updating the inputs.yaml file we mentioned above.
    The beauty of profiles is that it can ingest any table in your warehouse, whether it's collected by Rudderstack or not. For the tutorial, we will keep it simple and define 3 event sources coming from Secure Solutions, LLC's Shopify stores.
    1. PAGES: this is an event table that tracks all behavior (including anonymous activity)  across their stores. 
    2. TRACKS: This event table tracks a specific event along with user information about that event. The event tracked in this table is an ORDER_COMPLETED event. 
    3. IDENTIFIES: Users can sign up for a newsletter from the business. This event captures that event along with the users anonymous id and email. Thereby linking their anonymous activity to a specific email address.
    Each table will have its own input definition and we will define those here. This is also where we do the mapping exercise mentioned above. 
    Within each definition, we will map specific columns to the user entity's ID Types defined in the pb_project.yaml 
    We want to tell profiles exactly what columns to select in each table and map those columns to the id_types that we defined on the entity level in the pb_project.yaml file. 
    Note, the column names of the input tables are irrelevant. 
    What is relevant is what the data in each column represents and that you map the specific columns to the associated id_types. 
    For example, you will notice that the anonymous_id in the input tables is called anonymous_id. 
    But the id type we defined in the pb project yaml is anon_id. So we need to connect those two for each identifier column in each input.
"""

ABOUT_ID_STITCHER_1: Final[
    str
] = """
    Now, let's define an ID Stitcher Model. This model type will generate SQL which will run in your data warehouse and ultimately output an ID Graph. 
    Let's first define a name for the model. This will also be the name of the output view within your warehouse after the run is completed."""


ABOUT_ID_STITCHER_2: Final[
    str
] = """
    The id stitcher model has a concept known as an “edge”. An Edge is simply a direct connection between two nodes. 
    In profiles, a single node is a combination of your id value, and id type. So a full edge record would have 2 nodes.
    One side will have 1 id value and its associated id type. The other side will have another id value and its associated id type. 
    The record itself is considered an edge. Which is the connection between the 2 nodes. Within the ID Stitcher Model, we want to define the edge sources, which are the data tables we will access in order to extract the edges to build your ID Graph. 
    The edge sources come from your defined input sources, which we define in inputs.yaml file. These point to a table in the warehouse, but they can be referred with a different name within profiles project. 
    The "input model name" is different from the warehouse table name. In our demo, we give a `rs` prefix to the model names. You can see that in the inputs.yaml file too.       
    Since we have already defined our input sources, we can refer to them directly in the model spec here. Let's add our edge sources:
"""


ABOUT_PB_COMPILE: Final[
    str
] = """
    We have now defined a user entity, id types that belong to this entity, data sources from which to extract this data, and finally, a simple id stitcher model. 
    The next step is to run the project. 
    This is done by passing the `pb run` command within your CLI. Profiles will then compile the yaml code - creating a bunch of SQL files, and build a DAG based off of your configuration
    The DAG contains the sequence of SQL files that will then be executed in your warehouse.
    
    Before we perform a pb run, it is worth mentioning there is another command to produce the sql files only, without it actually running in the warehouse. This gives two advantages:
    1. If there are any errors in the yaml declaration, these can be caught here. 
    2. You can observe the generated SQL before it is run in your warehouse. 

    This optional command is `pb compile` Let's run this first to see what happens.
"""

ABOUT_PB_RUN: Final[
    str
] = """
    The command to run profiles project is `pb run`. 
    You would normally do that in your terminal as a cli command.
    But for this guided demo, you can just enter the command and the tutorial will execute it for you.
"""


EXPLAIN_PB_COMPILE_RESULTS: Callable[[str, int, str], str] = (
    lambda target, seq_no, profiles_dir: f"""
    The profiles project is compiled successfully. This would have created a new folder called `outputs` within the `{profiles_dir}` folder. In that you should see following folder structure:
    ```
    .
    └── profiles
        ├── output
        │   └── {target}
        │       └── seq_no
        │           └── {seq_no}
        │              └── compile
        │              
    ```
    You can check out the output directory and sql files produced. 
    This `pb compile` command actually did not run the profiles project. It only 'compiled' the project, i.e., created these sql files. Now let's move on to actually running the project.
"""
)

EXPLAIN_FIRST_RUN_INTRO: Callable[[str], str] = (
    lambda entity_name: f"""
    Now, let's perform some QA on the ID graph and verify the output. 
    As mentioned above, there is a column called `{entity_name}_main_id` which is supposed to represent a unique key for a unique user with multiple IDs.
    Let's get a count of the other ids per main id to see what we have. Remember, Secure Solutions, LLC is expecting roughly 319 distinct users. 
    171 of those being identified (or known) users who have either signed up for the newsletter or have completed an order.
    Those user_main_ids should have a unique email, user_id, and shopify_customer_id associated with it. Any {entity_name}_main_id that solely has anonymous_ids would represent unknown users.
    Here's the query we will run: 
"""
)

EXPLAIN_FIRST_RUN_PROMPT: Callable[[str], str] = (
    lambda entity_name: f"""
    Notice the count of user_ids and emails within each user_main_id where you can see that there are many users. That means, these users have been merged in error. 
    But interestingly, there's a single shopify_store_id per main_id
    This is an important clue. A shopify_store_id is not something that we associate at a {entity_name} level, but instead at a shop/store level.
    You may want to bring this into a user 360 view as a feature, but it should not be involved in the stitching process in any way since the values are not meant to be unique for a user.
    Adding this in the id stitcher has resulted in over-stitching of the {entity_name}s. 
    Now let's fix this. We need to make two changes to the project files. First in pb_project.yaml, and second in inputs.yaml.
    In `pb_project.yaml`, let's remove `shopify_store_id` as an id_type. This is in two places - 
        - under entities -> id_types
        - under id_types
    In our inputs.yaml we need to remove the id mappings we previously created for the shopify_store_id. You can remove the select, type, entity key/values 
    from the PAGES and IDENTIFIES input definitions underneath the `ids:` key in each definition. 
"""
)

EXPLAIN_SECOND_RUN_PROMPT: Callable[[str, str], str] = (
    lambda id_stitcher_table_name, updated_id_stitcher_table_name: f"""
    Now that the second full run is complete lets QA the outputs. 
    Optionally, you can inspect and see that profiles appended an additional seq_no directory in the outputs folder on this run. 
    This directory has the newly generated sql based on your edited configuration. Because we did not build any new models, the sql is very similar. 
    You will notice though, the hash number for the models are now different ({updated_id_stitcher_table_name} vs {id_stitcher_table_name}). 
    That is because we edited the source data for the id_stitcher model. If you add, delete, or update any of the configuraton for a model, pb will generate a new hash for that model and run it fresh.
    Representing the fact that, though it is the same model type running again, the actual SQL will be slightly different due to the updated configuration. 
    Since we are already familiar with the contents of the ID Graph, let's go ahead and aggregate this table and see how many user_main_ids we get this time.
"""
)

EXPLAIN_SECOND_RUN_PROMPT_2: Callable[[int, int], str] = (
    lambda distinct_ids, distinct_ids_upd: f"""
    Interesting, the number you should see is: {distinct_ids_upd}. This is much better than the earlier number {distinct_ids}!
    It's important to note how adding a bad id type resulted in, collapsing all users into just a handful of main ids
    But bad id types aren't the only reason for over stitching. Sometimes, a few bad data points can also cause this. 
    Let's see if we have any overly large clusters within this current output. 
    Run the following query:
"""
)

EXPLAIN_EDGES_TABLE: Callable[[str], str] = (
    lambda edge_table: f"""
    This is a much more healthy id graph. But it does appear that we have 5 user_main_ids that have an unusually high ID count. All of the user_main_ids below the top 5 seem to be fine. 
    Lets see how we can troubleshoot and fix this newly discovered issue. 
    We already reviewed the id types we brought in. And with the removal of the shopify store id as an id type that was not unique to a user, we know the rest are fine. 
    This means that, within one or more of the id types that remain, there are specific values that have caused additional overstitching to happen in the last run. 
    But how do we find these bad values?
    Since the ID Graph is the finished output, we will have to back up from that and see how it was constructed in order to observe what could have possibly caused additional overstitching. 
    We can do that by accessing a pre stitched table. That table is the Edges Table. The table name is `{edge_table}`.
    This table is constructed directly off of your edge sources specified within your ID Stitcher Spec. 
    The records in this table are the edges from your input tables. 
    A single record will have 2 nodes, one on each side, where the record itself represents the edge (the connection between the two nodes) 
    The table name will always be your main id graph output table with `_internal_edges` appended to it.
    Run the query below in order to see the contents of the edge table
    `select * from {edge_table} where id1 != id2 limit 100;`
    Now, let's run a more advanced query in order to analyze what id values have possibly stitched multiple users together. 
    Here, we are analyzing the count of edges for each side of the nodes using the edge table as the source. 
"""
)

EXPLAIN_BAD_ANNOYMOUS_IDS: Final[
    str
] = """
    Secure Solutions, LLC would expect an accurate user_main_id for a known active user who has purchased a number of IoT devices to have between 15 to 25 other ids.
    A healthy user_main_id would encompass a number of device ids, a number of anonymous_ids, but only one email, shopify_customer_id, and user_id. 
    This means, the count of edges between the different id values associated with a single user should be in the same ballpark.  
    However, after analyzing the output of the above query, we can see that 5 anonymous_ids have a count of edges well over 30. 
    This is a clue to the bad values. The true test is to see if these anonymous_ids are connected to more than one email, user_id, or shopify_customer_id. 
    We will extend this query to see.
"""

EXPLAIN_BAD_ANNOYMOUS_IDS_2: Final[
    str
] = """
    We sorted the query on an id type (email) where we know there should only be one per user_main_id. 
    And we can see that each of the 5 anonymous_ids seen in the previous query have 12 emails, user_ids, and shopify_customer_is linked to them. 
    You can see for the first anonymous_id for example, that there are many users merged together. 
    These 5 anonymous_ids are the clear culprit as to what has caused some of these users to merge. 
    Recalling the beginning of this tutorial, Secure Solutions, LLC has 1 brick and mortar store where customers can purchase IoT security devices from where they can checkout from an in store Kiosk. 
    In a realistic scenario, we could go back into our source event tables (the same ones we defined as input sources within this project) and see that the IP Address between these 5 anonymous_ids are all the same. 
    Meaning, there is a single machine that is causing these users to merge. 
    So how do we fix this? We don't want to remove anonymous_id as an id type since Secure Solutions wants to track and build features off of anonymous activity as well as known. 
    Instead, we need to filter out these specific values from being involved in the stitching process to begin with. 
    We have public docs available on the different methods for filtering, as there are multiple ways. (ref: https://www.rudderstack.com/docs/profiles/cli-user-guide/structure/#filters)
    But to wrap up this tutorial, we will go ahead and add a regex filter on the anonymous_id type. 
    This is done back in the pb_project.yaml.
"""

EXPLAIN_SEQ_NO: Callable[[int, str], str] = (
    lambda seq_no, id_stitcher_table: f"""
    Now that the anon ids are excluded in the project definition, let's re-run the profiles project. 
    But we will run with a parameter, `--seq_no`. Each pb run or pb compile creates a new seq_no as you saw.
    We can do a pb run with a seq_no param, so that profiles can reuse tables that already exist in the warehouse. If some model definition has changed, only those and the downstream queries are executed on the warehouse.
    This results in a faster pb run, along with some cost savings on the warehouse. The previous run had a seq number of {seq_no}, apparent from the id graph table name {id_stitcher_table}.
    We will use that seq no in the pb run.
"""
)

EXPLAIN_THIRD_RUN_1: Callable[[str, str, str, str], str] = (
    lambda id_stitcher_table_1, id_stitcher_table_2, id_stitcher_table_name_3, entity_name: f"""
    You can see that the id stitcher table name has changed again. It is now {id_stitcher_table_name_3} (from earlier {id_stitcher_table_2} and {id_stitcher_table_1}).
    The filter on anonymous_id resulted in the changing of the hash.
    Let's check the number of distinct {entity_name}_main_ids now.
"""
)

EXPLAIN_THIRD_RUN_2: Callable[[int, str], str] = (
    lambda distinct_ids_3, entity_name: f"""
    The number of distinct {entity_name}_main_ids is now {distinct_ids_3}. Great! Remember at the beginning we expected to have 319 {entity_name}_main_ids.
    Let's quickly look at the count of other ids per {entity_name}_main_id
"""
)


EXPLAIN_PB_RUN_RESULTS: Callable[[str, str, str, str, str], str] = (
    lambda id_stitcher_table_name, entity_name, schema, database, profiles_dir: f"""
    Congrats! You have completed your first profiles run.
    This should have created multiple tables in your warehouse account, and a new seq_no folder in the {profiles_dir}/output folder - just like the prev output of `pb compile`. 
    These sql files were actually executed on your warehouse now. That's the difference from the compile command. Every new `pb run` or `pb compile` creates a new seq_no folder.

    Lets observe the output ID Graph produced. The table that was created, which we will query now is {database}.{schema}.{id_stitcher_table_name}.        
    There are three key columns in this table, along with some other timestamp meta-data columns we can ignore for now:
        - `{entity_name}_main_id` - This is an id that Profiles creates. It uniquely identifies an entity (ex: user) across systems.
        - `other_id` - These are the id value extracted from the edge sources stitched to the user_main_id
        - `other_id_type` - the type of id from the original table. These are all the id_types you had provided earlier.
    
    So in this table, we can expect multiple rows for a unique {entity_name}, all sharing the same {entity_name}_main_id. But other_id + other_id_type should be unique. 

    You can sample the data in this table by running following query in your warehouse:

    `SELECT * FROM {database}.{schema}.{id_stitcher_table_name} LIMIT 10;`
"""
)

THIRD_RUN_CONCLUSION_MESSAGE: Final[
    str
] = """
    Great! We can see that even the user_main_ids with the highest count are within the threshold that we would expect. 
    Now that Secure Solutions, LLC has a solid id graph, they can unlock future major value add projects for the business!! Like building an accurate 360 degree view of their customers, predicting accurate churn scores, model marketing attribution, etc.
    Let's move on to the final section to build a handful of useful features for our user entity.
"""


FEATURE_CREATION_INTRO: Callable[[str], str] = (
    lambda entity: f"""In this final section, we will walk through feature creation:
Now that we have a solid id graph, we can now build accurate features(or attributes) on your users. Generally, when building features for users you are either aggregating data or performing window functions across your tables. In either case, you would use an identifier to either `group by` or `partition by` the user. 
Given that you may have many different data tables across many different sources, each one having multiple different identifiers for a single instance of a customer, a single customer identifier column that you would use to group or partition by will not suffice. That is why we build the ID Graph first. Building an ID Graph was really a means to an end. The end being to generate a unifying key that connects all identifiers for each customer instance across all sources. Now that we have generated a key (`{entity}_main_id`) that unifies the users across disparate sources, we will now use that user_main_id to `group by` or `partition by`. 

Within profiles, each customer feature has it's own definition called an entity_var. These definitions are structured similar to select SQL queries so you have a lot of flexibility on the structure and the output. You can derive simple values like timestamps, integers, strings, as well as more complex features using array, json , or variant SQL functions. The exact SQL syntax will differ depending on what warehouse platform you are using. 

Within the profiles project configuration, each entity_var definition will have the following structure:

1. NAME: This will be the column alias in the output c360 feature view
2. SELECT: This is a select statement that tells profiles what column or combination of columns to select from an input source along with what aggregate or window function to use.
3. FROM: The input source for profiles to query in order to derive the values

Note: It is implicit that you will be aggregating or partitioning data from your sources, so you have to either use an aggregation or window function within the select of an entity var definition. When profiles runs the sql query on the source table, it will automatically group by or partition by the {entity}_main_id. You will be able to observe the sql generated from your configuration within our outputs directory after we build the entity vars and perform a pb run. 

entity_var definitions can be really flexible and there are many optional keys to add in order to fine tune the sql query and derive the desired output for your customers. You can visit our docs in order to learn more about how to create features. (ref: https://www.rudderstack.com/docs/profiles/core-concepts/feature-development/#define-features)"""
)


FEATURE_DETAILS: Final[
    str
] = """
To start, Secure Solutions, LLC wants to build 6 features for their customers in order to power personalized content across their email and ad marketing efforts. 

The 6 features they want to build are:

1. Account Creation Date: We want to know the date customers created their account
2. Last Seen Date: We want to know the timestamp of the last sight visit for each customer
3. Total Revenue: We want to know the total dollars spent per customer, if any
4. Number of Devices Purchased: We want to know how many devices each customer has purchased and enrolled in the security subscription app
5. Last Order Date: The last date each customer placed an order
6. Days Since Last Order: The number of days since a customer has placed an order.

Let's build the first few features from the list above and then we will auto fill the last 3 for reference.
"""

FEATURES_ADDED: Final[
    str
] = """
Great!! You have now build 3 features. For the sake of time, we auto filled the final 3 entity_vars within the profiles.yaml file. You can check this file out now to see the yaml structure of the ones you created as well as the last 3 we auto filled.  

Take note of the entity_vars we auto filled to learn more about the structure of entity vars and how you can perform different sql functions. 

One entity_var definition we want to point out is the days_since_last_order var. Notice how that entity_var does not have a FROM key within the definition. That is because the data source where we are performing the calculation is not in any of the input sources. Rather, we are performing a date calculation on the underlying c360 table which is generated during each run. You can also see that we are referencing another previously defined entity_var within the select statement in order to perform our date calculation. This is an important concept to know and familiarize your self with because it creates more flexibility on what features you can build for your entities.
"""


DEFINE_FEATURE_VIEW: Final[
    str
] = """
This is the final step! 

Now that we have defined 6 entity_vars, we want to now create a view in the warehouse that will output these features for each user. Profiles has a concept known as a feature_view and it is defined on the entity level within the pb_project.yaml. A feature view is meant to output one record per user. And each column will be a feature on that user. The features being the entity_vars we defined in the previous step.

We will create two feature views in this section. One is known as the default feature view which will be created automatically. This default feature view will then serve as a base view that you can then generate custom feature views from. 

The default feature view will have the generated user_main_id as the primary key. Because this key is strictly internal to profiles, meaning you will not use it for downstream systems, you can also create a custom feature view using any of the id types from your entity id type list as the primary key.
"""

CONCLUSSION: Final[
    str
] = """
Great!
Secure Solutions, LLC now has a solid and accurate ID Graph modeled around their user entity. We then built a feature view on top of the id graph that has accurate and helpful traits for our users that we can then user to power our personalization efforts. 
We hope that you can take this knowledge and apply it with your own customer data!!
"""

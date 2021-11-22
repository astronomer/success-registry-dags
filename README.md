# Zendesk Extract DAG
*This DAG was created to extract three specific Zendesk objects (tickets, users, and organizations) into S3. From S3, those objects are then loaded into Snowflake.* 

# How to use this Proof of Concept

## Prerequisites
  - Astro CLI (You can use the quickstart guide [here](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart) to install it)

## Step-by-step
To test out this DAG, follow these steps:
1. In your terminal, clone this repository to a local project directory using: `git clone git@github.com:astronomer/success-registry-dags.git`
2. After cloning, make this project your current working directory by typing `cd success-registry-dags`
3. Use `astro dev start` to spin up a Docker container using this project
4. Navigate to http://localhost:8080 in your web browser to view the project's Airflow UI
5. For the default username and password enter **admin**
6. To allow Airflow to connect to Zendesk, you will have to add a Zendesk Admin account in the Airflow UI Connections (Admin >> Connections) menu. Use the following parameters for the connection:
    - Conn Id: **zendesk_api**
    - Conn Type: **HTTP**
    - Login: *<your_zendesk_email>*
    - Password: *<your_zendesk_password>*
7. To allow Airflow to connect and interact with s3, you will have to add an AWS account in the Airflow UI Connections (Admin >> Connections) menu. Use the following parameters for the connection:
    - Conn Id: **my_conn_s3**
    - Conn Type: **S3**
    - Extra: **{"aws_access_key_id":"*your_access_key_id*","aws_secret_access_key":"*your_secret_access_key*"}**
8. To allow Airflow to connect and interact with Snowflake, you will have to add a Snowflake account in the Airflow UI Connections (Admin >> Connections) menu. Use the following parameters for the connection:
    - Conn Id: **my_snowflake_conn**
    - Conn Type: **Snowflake**
    - Host: *your_snowflake_host*
    - Login: *your_snowflake_login*
    - Password: *your_snowflake_password*
    - Account: *your_snowflake_account*
    - Database: *your_snwoflake_database*
    - Region: *your_snowflake_region*
    - Warehouse: *your_snowflake_warehouse*
9. After creating the connections, in order to run the DAG, you'll need to update a few task parameters in the `zendesk_extract.py` DAG file:
    - For the `copy_daily_{zendesk_obj}_to_s3` & `upload_full_{zendesk_obj}_to_s3` tasks, be sure to enter a proper s3 bucket under the `s3_bucket_name` parameter
    - For the `copy_daily_{zendesk_obj}_to_snowflake` & `copy_full_{zendesk_obj}_to_snowflake` tasks, be sure to enter a proper snowflake schema under the `schema_name` parameter

*After following the above steps, you should be able to manually trigger this DAG and extract zendesk data to s3 and from there load it into Snowflake.*

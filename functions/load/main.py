# imports
import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
import json
import duckdb
import pandas as pd

# setup
project_id = 'group2-ba882'
secret_id = 'project_key'   #<---------- this is the name of the secret you created
version_id = 'latest'


# db setup
db = 'city_services_boston'
schema = "raw"
raw_db_schema = f"{db}.{schema}"
stage_db_schema = f"{db}.stage"



############################################################### main task

@functions_framework.http
def main(request):

    # Parse the request data
    request_json = request.get_json(silent=True)
    print(f"request: {json.dumps(request_json)}")

    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token through
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    # drop if exists and create the raw schema for 
    create_schema = f"DROP SCHEMA IF EXISTS {raw_db_schema} CASCADE; CREATE SCHEMA IF NOT EXISTS {raw_db_schema};"
    md.sql(create_schema)

    print(md.sql("SHOW DATABASES;").show())


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: requests

    # read in from gcs
    requests_path = request_json.get('requests')
    requests_df = pd.read_parquet(requests_path)

    # table logic
    raw_tbl_name = f"{raw_db_schema}.requests"
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name} ;
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM {stage_db_schema}.requests WHERE FALSE;
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # ingest into raw schema
    ingest_sql = f"INSERT INTO {raw_tbl_name} SELECT * FROM requests_df"
    print(f"Import statement: {ingest_sql}")
    md.sql(ingest_sql)
    del requests_df

    # upsert like operation -- will only insert new records, not update
    upsert_sql = f"""
    INSERT INTO {stage_db_schema}.requests AS stage
    SELECT *
    FROM {raw_tbl_name} AS raw
    ON CONFLICT (_id)
    DO NOTHING;
    """
    print(upsert_sql)
    md.sql(upsert_sql)
    
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: location

    # read in from gcs
    location_path = location_json.get('location')
    location_df = pd.read_parquet(location_path)

    # table logic
    raw_tbl_name = f"{raw_db_schema}.tags"
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name} ;
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM {stage_db_schema}.location WHERE FALSE;
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # ingest into raw schema
    ingest_sql = f"INSERT INTO {raw_tbl_name} SELECT * FROM location_df"
    print(f"Import statement: {ingest_sql}")
    md.sql(ingest_sql)
    del location_df

    # upsert like operation -- will only insert new records, not update
    upsert_sql = f"""
    INSERT INTO {stage_db_schema}.location AS stage
    SELECT *
    FROM {raw_tbl_name} AS raw
    ON CONFLICT (_id, location)
    DO NOTHING;
    """
    print(upsert_sql)
    md.sql(upsert_sql)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: department_assignment

    # read in from gcs
    department_assignment_path = request_json.get('department_assignment')
    department_assignment_df = pd.read_parquet(department_assignment_path)

    # table logic
    raw_tbl_name = f"{raw_db_schema}.department_assignment"
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name} ;
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM {stage_db_schema}.department_assignment WHERE FALSE;
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # ingest into raw schema
    ingest_sql = f"INSERT INTO {raw_tbl_name} SELECT * FROM department_assignment_df"
    print(f"Import statement: {ingest_sql}")
    md.sql(ingest_sql)
    del department_assignment_df

    # upsert like operation -- will only insert new records, not update
    upsert_sql = f"""
    INSERT INTO {stage_db_schema}.department_assignment AS stage
    SELECT *
    FROM {raw_tbl_name} AS raw
    ON CONFLICT (_id, case_enquiry_id, department)
    DO NOTHING;
    """
    print(upsert_sql)
    md.sql(upsert_sql)


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: response_time

    # read in from gcs
    response_time_path = request_json.get('response_time')
    response_time_df = pd.read_parquet(response_time_path)
    
    # table logic
    raw_tbl_name = f"{raw_db_schema}.response_time"
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name} ;
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM {stage_db_schema}.response_time WHERE FALSE;
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # ingest into raw schema
    ingest_sql = f"INSERT INTO {raw_tbl_name} SELECT * FROM response_time_df"
    print(f"Import statement: {ingest_sql}")
    md.sql(ingest_sql)
    del response_time_df

    # upsert like operation -- will only insert new records, not update
    upsert_sql = f"""
    INSERT INTO {stage_db_schema}.response_time AS stage
    SELECT *
    FROM {raw_tbl_name} AS raw
    ON CONFLICT (_id, case_enquiry_id)
    DO NOTHING;
    """
    print(upsert_sql)
    md.sql(upsert_sql)


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: status_history

    # read in from gcs
    status_history_path = request_json.get('status_history')
    status_history_df = pd.read_parquet(status_history_path)

    # table logic
    raw_tbl_name = f"{raw_db_schema}.status_history"
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name} ;
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM {stage_db_schema}.status_history WHERE FALSE;
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # ingest into raw schema
    ingest_sql = f"INSERT INTO {raw_tbl_name} SELECT * FROM status_history_df"
    print(f"Import statement: {ingest_sql}")
    md.sql(ingest_sql)
    del status_history_df

    # upsert like operation -- will only insert new records, not update
    upsert_sql = f"""
    INSERT INTO {stage_db_schema}.status_history AS stage
    SELECT *
    FROM {raw_tbl_name} AS raw
    ON CONFLICT (_id, case_enquiry_id, open_dt)
    DO NOTHING;
    """
    print(upsert_sql)
    md.sql(upsert_sql)


    return {}, 200
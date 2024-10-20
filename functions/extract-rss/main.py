import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
import requests
import json
import datetime
import uuid
import duckdb

# settings
project_id = 'group2-ba882'
secret_id = 'project_key'
version_id = 'latest'
bucket_name = 'group2-ba882-project'

# db setup
db = 'city_services'
schema = "stage"
db_schema = f"{db}.{schema}"

# API endpoint
api_url = "https://311.boston.gov/open311/v2/requests.json"

####################################################### helpers

def upload_to_gcs(bucket_name, job_id, data):
    """Uploads data to a Google Cloud Storage bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob_name = f"jobs/{job_id}/extracted_311_requests.json"
    blob = bucket.blob(blob_name)

    blob.upload_from_string(data)
    print(f"File {blob_name} uploaded to {bucket_name}.")

    return {'bucket_name': bucket_name, 'blob_name': blob_name}

def fetch_311_data(start_date, end_date, limit=1000):
    """Fetches 311 service request data from the Boston API."""
    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "limit": limit
    }
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

####################################################### core task

@functions_framework.http
def task(request):
    # job_id
    job_id = datetime.datetime.now().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())

    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token
    md = duckdb.connect(f'md:?motherduck_token={md_token}')

    ####################################### fetch 311 data

    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=30)  # Fetch last 30 days of data
    data = fetch_311_data(start_date, end_date)
    print(f"Fetched {len(data)} 311 service requests")

    # to a json string
    data_json = json.dumps(data)

    # write to gcs
    gcs_path = upload_to_gcs(bucket_name, job_id, data_json)

    ####################################### insert data into DuckDB tables

    # Insert data into requests table
    requests_data = [(
        int(entry.get('service_request_id', 0)),
        entry.get('service_request_id', ''),
        entry.get('service_name', ''),
        entry.get('service_code', ''),
        entry.get('description', ''),
        entry.get('service_name', ''),
        '',  # queue
        entry.get('agency_responsible', ''),
        entry.get('media_url', ''),
        ''  # closed_photo
    ) for entry in data]
    
    md.execute(f"""
    INSERT INTO {db_schema}.requests (_id, case_enquiry_id, case_title, subject, reason, type, queue, source, submitted_photo, closed_photo)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, requests_data)

    # Insert data into locations table
    locations_data = [(
        entry.get('address', ''),
        '',  # fire_district
        '',  # pwd_district
        '',  # city_council_district
        '',  # police_district
        '',  # neighborhood
        '',  # neighborhood_services_district
        '',  # ward
        '',  # precinct
        entry.get('address', ''),
        '',  # location_zipcode
        float(entry.get('lat', 0)),
        float(entry.get('long', 0)),
        None  # geom_4326
    ) for entry in data]
    
    md.execute(f"""
    INSERT INTO {db_schema}.locations (location, fire_district, pwd_district, city_council_district, police_district, neighborhood, neighborhood_services_district, ward, precinct, location_street_name, location_zipcode, latitude, longitude, geom_4326)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, locations_data)

    # Insert data into status_history table
    status_history_data = [(
        entry.get('service_request_id', ''),
        entry.get('requested_datetime', ''),
        entry.get('expected_datetime', ''),
        entry.get('updated_datetime', ''),
        entry.get('status', ''),
        ''  # closure_reason
    ) for entry in data]
    
    md.execute(f"""
    INSERT INTO {db_schema}.status_history (case_enquiry_id, open_dt, sla_target_dt, closed_dt, case_status, closure_reason)
    VALUES (?, ?, ?, ?, ?, ?)
    """, status_history_data)

    # Insert data into department_assignment table
    department_assignment_data = [(
        entry.get('service_request_id', ''),
        entry.get('agency_responsible', '')
    ) for entry in data]
    
    md.execute(f"""
    INSERT INTO {db_schema}.department_assignment (case_enquiry_id, department)
    VALUES (?, ?)
    """, department_assignment_data)

    # Insert data into response_time table
    response_time_data = [(
        entry.get('service_request_id', ''),
        True if entry.get('status') == 'closed' else False
    ) for entry in data]
    
    md.execute(f"""
    INSERT INTO {db_schema}.response_time (case_enquiry_id, on_time)
    VALUES (?, ?)
    """, response_time_data)

    return {
        "num_entries": len(data), 
        "job_id": job_id, 
        "bucket_name": gcs_path.get('bucket_name'),
        "blob_name": gcs_path.get('blob_name')
    }, 200
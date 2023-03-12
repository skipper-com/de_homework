import io, os, requests
import pandas as pd
import pyarrow
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
#BUCKET = os.environ.get("dtc-de-skipper", "dtc-data-lake-bucketname")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]


        # csv file_name 
        file_name = service + '_tripdata_' + year + '-' + month + '.csv.gz'

        # download it using requests via a pandas df
        request_url = init_url + service + '/' + file_name
        print(request_url)
        r = requests.get(request_url)
        open(file_name, "wb").write(r.content)
        #pd.DataFrame(io.StringIO(r.text)).to_csv(file_name)
        #print(f"Local: {file_name}")

        # read it back into a parquet file
        #df = pd.read_csv(file_name)
        #file_name = file_name.replace('.csv', '.parquet')
        #df.to_parquet(file_name, engine='pyarrow')
        #print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs('dtc-de-skipper', f"data/{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


# web_to_gcs('2019', 'green')
web_to_gcs('2020', 'green')
web_to_gcs('2021', 'green')
web_to_gcs('2019', 'yellow')
web_to_gcs('2020', 'yellow')
web_to_gcs('2021', 'yellow')
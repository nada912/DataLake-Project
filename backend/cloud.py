from google.cloud import storage

def test_gcs_credentials():
    client = storage.Client(project='charming-sonar-424115-n8')
    buckets = list(client.list_buckets())
    print(f'Buckets: {buckets}')

test_gcs_credentials()

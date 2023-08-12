import hvac
from decouple import config
import boto3

client = hvac.Client(url=config('VAULT_URL'))

client.auth.approle.login(
    role_id=config('VAULT_ROLE_ID'),
    secret_id=config('VAULT_SECRET_ID'),
)

assert client.is_authenticated(), 'couldn\'t authenticate with app role'

aws_credentials = client.read(
    config('VAULT_TOKEN_PATH'))
print(aws_credentials)

key_id = aws_credentials['data']['aws_key']
secret_key = aws_credentials['data']['aws_secret']
client = boto3.client('sts',
                      aws_access_key_id=key_id,
                      aws_secret_access_key=secret_key)
identity = client.get_caller_identity()
print('identity:', identity)

s3 = boto3.client('s3', aws_access_key_id=key_id,
                  aws_secret_access_key=secret_key)
response = s3.list_buckets()

# Output the bucket names
print('Existing buckets:')
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')

prefix = 'scratch'
bucket = 'cls-picasso'
response = s3.upload_file('test_file', bucket, f'{prefix}/test_file')
files = s3.list_objects(Bucket=bucket, Prefix=prefix)['Contents']
files = [f['Key'] for f in files]
print('found files: ', files)

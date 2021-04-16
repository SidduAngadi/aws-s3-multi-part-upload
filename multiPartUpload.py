import boto3
from datetime import datetime, timedelta, date

try:
    client = boto3.client('s3')
except Exception as err:
    print (str(err))


def list_buckets():
    response = {} 
    try:
        response = client.list_buckets()
    except Exception as err:
        print (str(err))
    return response['Buckets']

def list_all_multipart_upload(bucket_name):
    response = {} 
    try:
        response = client.list_multipart_uploads(Bucket=bucket_name )
    except Exception as err:
        print (str(err))
    return response

def abort_multipart_upload(bucket_name, key, uploadId):
    response = {} 
    try:
        response = client.abort_multipart_upload(  Bucket=bucket_name, Key=key, UploadId=uploadId)
    except Exception as err:
        print (str(err))
        print (uploadId, "  : failed to cleaned up")
    return response
   

if __name__ == "__main__":

    print ("listing all buckets in the account ")
    threshold_days = 7
    res = list_buckets()
    for buckets in res:
        bucket_name=buckets['Name']
        print(bucket_name, "============================")
       
        response = list_all_multipart_upload(bucket_name) 
        if response['NextKeyMarker']:
            for upload in response['Uploads']:

                print ("S3 Object Key        : ",upload['Key']) 
                print ("Upload ID of part    : ",upload['UploadId'])
                print ("Upload time          : ",upload['Initiated'])
                print ("Storage Class        : ",upload['StorageClass'])
                age_of_file = (datetime.now().date() - upload['Initiated'].date()).days
                print ("Age of the FIle      : ", age_of_file, "days")

                if age_of_file > threshold_days :
                    print(upload['UploadId'], " : will be cleaned up as it is more than ", threshold_days)
                    abort_multipart_upload(bucket_name, upload['Key'], upload['UploadId'])


import json
import boto3
import os

def list_dir(prefix, local, bucket, client):
    keys = []
    next_token = ""
    base_kwargs = {
        "Bucket": bucket,
        "Prefix": prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != "":
            kwargs.update({"ContinuationToken": next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get("Contents")
        for i in contents:
            k = i.get("Key")
            if k[-1] != "/":
                keys.append(k)
        next_token = results.get("NextContinuationToken")
    return keys

def extract_folder_paths(files):
    folders = list(set([folder.rpartition('/')[0] if folder.endswith('full_marks') else folder for folder in (file.rpartition('/')[0] for file in files)]))
    return folders

def place_trigger_files(bucket, files):
    client = boto3.client('s3')
    folders = extract_folder_paths(files)
    for trigger_folder in folders:
        client.put_object(Body="", Bucket=bucket, Key=trigger_folder)
    
def lambda_handler(event, context):
    s3_folder="case_number"
    s3_sub_folder="exhibits"
    s3_document_folder="folder1"
    main_s3_bucket="pythonninjas"
    trigger_s3_bucket="trigger-bucket-11"
    
    session = boto3.Session()
    s3_client = session.client(service_name="s3")
    
    files = list_dir(prefix=''.join([s3_folder,"/",s3_sub_folder,"/",s3_document_folder]), local="/tmp", bucket=main_s3_bucket, client=s3_client)
    place_trigger_files(bucket=trigger_s3_bucket, files=files)
    
    return {
        'statusCode': 200,
        'body': "Done"
    }

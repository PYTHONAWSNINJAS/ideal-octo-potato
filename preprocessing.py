import json
import boto3
import os
import traceback

def list_dir(prefix, bucket, client):
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

def place_metadata_file(bucket, file):
    client = boto3.client('s3')
    client.put_object(Body="", Bucket=bucket, Key=file)
    
def lambda_handler(event, context):
    try:
        s3_sub_folder=os.environ["s3_sub_folder"]
        main_s3_bucket=os.environ["main_s3_bucket"]
        metadata_s3_bucket=os.environ["metadata_s3_bucket"]
        trigger_s3_bucket=os.environ["trigger_s3_bucket"]
        processing_type=json.loads(event['body'])['processing_type']
        session = boto3.Session()
        s3_client = session.client(service_name="s3")
        
        if processing_type == 'case_level':
            s3_folder=json.loads(event['body'])['s3_folder']
            prefix=''.join([s3_folder,"/",s3_sub_folder])
        elif processing_type == 'doc_level':      
            s3_folder=json.loads(event['body'])['s3_folder']        
            s3_document_folder=json.loads(event['body'])['s3_document_folder']
            prefix=''.join([s3_folder,"/",s3_sub_folder,"/",s3_document_folder])
        
        files = list_dir(prefix=prefix, bucket=main_s3_bucket, client=s3_client)
        
        doc_metadata_file_path = prefix + '/' + s3_document_folder + "_" + str(len(files))
        place_metadata_file(bucket=metadata_s3_bucket, file=doc_metadata_file_path)
        place_trigger_files(bucket=trigger_s3_bucket, files=files)
        
        return {
            'statusCode': 200,
            'body': "Triggered"
        }
    except Exception as e:
        print(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': str(traceback.format_exc())
        }
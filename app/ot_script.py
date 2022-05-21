import json
import boto3

def list_dir(prefix, bucket, client):
    """
    Parameters
    ----------
    prefix: Prefix to list from in S3
    bucket: Bucket Name
    client: S3 Client object

    Returns
    -------
    Keys: list of files

    """
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
    
def lambda_handler(event, context):
    session = boto3.Session()
    s3_client = session.client(service_name="s3")

    control_files = (list_dir("case_number/doc_pdf/control_files/","trigger-bucket-11", s3_client))
    
    for control_file in control_files:
        prefix = control_file.replace("doc_pdf","exhibits").replace("control_files/","").replace(".json","")
        print(prefix)
        trigger_files = (list_dir(prefix,"pythonninjas", s3_client))
        triggers = {'/'.join(item.split('/')[0:4]) for item in trigger_files}
        print(triggers)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

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

    control_files = list_dir(
        "case_183880001/doc_pdf/unmerged_control_files/", "trialmanager", s3_client
    )
    # control_files = control_files[0:2]
    print(control_files)

    for control_file in control_files:
        print(control_file)
        prefix = control_file.replace(
            "doc_pdf/unmerged_control_files", "exhibits"
        ).replace(".json", "/")
        print(prefix)
        files = list_dir(prefix, "trialmanager", s3_client)
        print(files)
        triggers = {"/".join(item.split("/")[0:4]) for item in files}
        print(triggers)

        # write metadata file
        metadata_file = prefix + prefix.split("/")[2] + "___" + str(len(triggers))
        print(metadata_file)

        s3_client.put_object(Body="", Bucket="trialmanager-metadata", Key=metadata_file)

        # write triggers in s3
        for trigger in triggers:
            s3_client.put_object(Body="", Bucket="trialmanager-trigger", Key=trigger)

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}

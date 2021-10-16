"""
This module is trigger based on API event and
accepts the payload to start placing the trigger files for
the main lambda to work. This also places the
meta trigger objects for merge trigger conditions.
"""

import json
import os
import traceback

import boto3

from flask import Flask, request
import concurrent.futures

# template_folder points to current directory. Flask will look for '/static/'
app = Flask(__name__, template_folder=".")
# The rest of your file here


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


def extract_folder_paths(files):
    """
    Parameters
    ----------
    files: List of File Paths

    Returns
    -------
    folders: returns set of folder
    """
    folders = list(
        {
            folder.rpartition("/")[0] if folder.endswith("full_marks") else folder
            for folder in (file.rpartition("/")[0] for file in files)
        }
    )
    return folders


def place_trigger_files(bucket, folders):
    """
    Parameters
    ----------
    bucket: bucket name
    folders: trigger folder paths
    """
    client = boto3.client("s3")
    for trigger_folder in folders:
        client.put_object(Body="", Bucket=bucket, Key=trigger_folder)


def place_metadata_file(bucket, file):
    """
    Parameters
    ----------
    bucket
    file
    """
    client = boto3.client("s3")
    client.put_object(Body="", Bucket=bucket, Key=file)


def filter_trigger_folders(trigger_folders):
    """
    This function will filter the trigger folders to only
    put document level path not anything in nested paths.
    Parameters
    ----------
    event: lambda event
    context: lambda context
    """
    filtered_folders = {
        "/".join(
            [
                item.split("/")[0],
                item.split("/")[1],
                item.split("/")[2],
                item.split("/")[3],
            ]
        )
        for item in trigger_folders
    }
    return filtered_folders

<<<<<<< HEAD
def preprocess(args):
    s3_folder, s3_sub_folder, s3_document_folder, main_s3_bucket, metadata_s3_bucket, trigger_s3_bucket, s3_client = args
=======

def preprocess(
    s3_folder,
    s3_sub_folder,
    s3_document_folder,
    main_s3_bucket,
    metadata_s3_bucket,
    trigger_s3_bucket,
    s3_client,
):
>>>>>>> c998a2da2f58ad7dc74aa7ea5685ca6afcf2db30
    prefix = "".join([s3_folder, "/", s3_sub_folder, "/", s3_document_folder, "/"])
    files = list_dir(prefix=prefix, bucket=main_s3_bucket, client=s3_client)
    trigger_folders = extract_folder_paths(files)
    filtered_trigger_folders = filter_trigger_folders(trigger_folders)
    print("\n\nfiltered_trigger_folders - ", filtered_trigger_folders)
    print("s3_document_folder", s3_document_folder)
    doc_metadata_file_path = (
        prefix + s3_document_folder + "_" + str(len(filtered_trigger_folders))
    )
    print("doc_metadata_file_path - ", doc_metadata_file_path)
    place_metadata_file(bucket=metadata_s3_bucket, file=doc_metadata_file_path)
    place_trigger_files(bucket=trigger_s3_bucket, folders=trigger_folders)


# noinspection PyShadowingNames,PyUnusedLocal
@app.route("/", methods=["POST"])
def index():
    """
    Parameters
    ----------
    event: lambda event
    context: lambda context
    """

    # noinspection PyBroadException
    try:
        body = request.json
        s3_sub_folder = os.environ["s3_sub_folder"]
        main_s3_bucket = os.environ["main_s3_bucket"]
        metadata_s3_bucket = os.environ["metadata_s3_bucket"]
        trigger_s3_bucket = "trigger-bucket-11"  # os.environ["trigger_s3_bucket"]
        processing_type = body["processing_type"]
        s3_folder = body["s3_folder"]
        session = boto3.Session()
        s3_client = session.client(service_name="s3")

        if processing_type == "case_level":
            case_prefix = "".join([s3_folder, "/", s3_sub_folder])
            case_files = list_dir(
                prefix=case_prefix, bucket=main_s3_bucket, client=s3_client
            )
            case_trigger_folders = extract_folder_paths(case_files)
            s3_document_folders = list(
                {item.split("/")[2] for item in case_trigger_folders}
            )
            args = []
            for s3_document_folder in s3_document_folders:
                stuffs = []
                stuffs.extend([s3_folder, s3_sub_folder, s3_document_folder, main_s3_bucket, metadata_s3_bucket, trigger_s3_bucket, s3_client])
                args.append(stuffs)               

            with concurrent.futures.ThreadPoolExecutor() as executer:
                results_map = executer.map(preprocess, args)
        elif processing_type == "doc_level":
            s3_document_folder = body["s3_document_folder"]
            preprocess(
                s3_folder,
                s3_sub_folder,
                s3_document_folder,
                main_s3_bucket,
                metadata_s3_bucket,
                trigger_s3_bucket,
                s3_client,
            )

        return {"statusCode": 200, "body": "Triggered with " + str(body)}
    except Exception as e:
        print(f"ERROR for - {body}, The error is {e}")
        print(traceback.format_exc())
        return {"statusCode": 500, "body": str(traceback.format_exc())}


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=5000)

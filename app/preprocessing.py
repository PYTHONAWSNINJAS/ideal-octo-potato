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

import time
from botocore.exceptions import ClientError

import logging
import sys


app = Flask(__name__, template_folder=".")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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


def place_trigger_files(bucket, folders, client):
    """
    Parameters
    ----------
    bucket: bucket name
    folders: trigger folder paths
    """
    logger.info("placing trigger files")

    for trigger_folder in folders:
        delay = 1  # initial delay
        delay_incr = 1  # additional delay in each loop
        max_delay = 30  # max delay of one loop. Total delay is (max_delay**2)/2
        while delay < max_delay:
            try:
                client.put_object(Body="", Bucket=bucket, Key=trigger_folder)
                break
            except ClientError:
                time.sleep(delay)
                delay += delay_incr
        else:
            logger.info(f"place_trigger_files ERROR for: {trigger_folder}")


def place_metadata_file(bucket, file, client):
    """
    Parameters
    ----------
    bucket
    file
    """
    logger.info("placing metadata files")

    delay = 1  # initial delay
    delay_incr = 1  # additional delay in each loop
    max_delay = 30  # max delay of one loop. Total delay is (max_delay**2)/2

    while delay < max_delay:
        try:
            client.put_object(Body="", Bucket=bucket, Key=file)
            break
        except ClientError:
            time.sleep(delay)
            delay += delay_incr
    else:
        logger.info(f"place_metadata_file ERROR for: {file}")


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


def preprocess(args):
    """
    This function will list the s3 folder
    and place a metadata file and
    trigger file
    Args:
        args (list): list of arguments to be
        processed in parallel
    """
    try:
        (
            s3_folder,
            s3_sub_folder,
            s3_document_folder,
            main_s3_bucket,
            metadata_s3_bucket,
            trigger_s3_bucket,
            s3_client,
        ) = args
        prefix = "".join([s3_folder, "/", s3_sub_folder, "/", s3_document_folder, "/"])
        files = list_dir(prefix=prefix, bucket=main_s3_bucket, client=s3_client)
        trigger_folders = extract_folder_paths(files)
        filtered_trigger_folders = filter_trigger_folders(trigger_folders)
        logger.info(f"filtered_trigger_folders: {filtered_trigger_folders}")
        logger.info(f"s3_document_folder: {s3_document_folder}")
        doc_metadata_file_path = (
            prefix + s3_document_folder + "_" + str(len(filtered_trigger_folders))
        )
        logger.info(f"doc_metadata_file_path: {doc_metadata_file_path}")
        place_metadata_file(
            bucket=metadata_s3_bucket, file=doc_metadata_file_path, client=s3_client
        )
        place_trigger_files(
            bucket=trigger_s3_bucket, folders=filtered_trigger_folders, client=s3_client
        )
    except Exception as _:
        exception_type, exception_value, exception_traceback = sys.exc_info()
        traceback_string = traceback.format_exception(
            exception_type, exception_value, exception_traceback
        )
        err_msg = json.dumps(
            {
                "errorType": exception_type.__name__,
                "errorMessage": str(exception_value),
                "stackTrace": traceback_string,
            }
        )
        logger.error(err_msg)


def folder_exists_and_not_empty(bucket, path):
    '''
    bucket: bucket name. 
    path: path to wire folder.
    '''
    s3 = boto3.client('s3')
    if not path.endswith('/'):
        path = path+'/' 
    resp = s3.list_objects(Bucket=bucket, Prefix=path, Delimiter='/',MaxKeys=1)
    return 'Contents' in resp


@app.route("/", methods=["POST"])
def index():
    """
    Parameters
    ----------
    event: lambda event
    context: lambda context
    """
    try:
        body = request.json
        s3_exhibits_folder = os.environ["s3_exhibits_folder"]
        s3_wire_folder = os.environ["s3_wire_folder"]
        main_s3_bucket = os.environ["main_s3_bucket"]
        metadata_s3_bucket = os.environ["metadata_s3_bucket"]
        trigger_s3_bucket = os.environ["trigger_s3_bucket"]
        processing_type = body["processing_type"]
        s3_folder = body["s3_folder"]
        session = boto3.Session()
        s3_client = session.client(service_name="s3")

        if processing_type == "case_level":
            for item in [s3_exhibits_folder, s3_wire_folder]:
                print(folder_exists_and_not_empty(main_s3_bucket, s3_folder+'/'+item))
                case_prefix = "".join([s3_folder, "/", item])
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
                    stuffs.extend(
                        [
                            s3_folder,
                            item,
                            s3_document_folder,
                            main_s3_bucket,
                            metadata_s3_bucket,
                            trigger_s3_bucket,
                            s3_client,
                        ]
                    )
                    args.append(stuffs)

                with concurrent.futures.ThreadPoolExecutor() as executer:
                    _ = executer.map(preprocess, args)
        elif processing_type == "doc_level":
            s3_document_folder = body["s3_document_folder"]
            preprocess(
                [
                    s3_folder,
                    s3_exhibits_folder,
                    s3_document_folder,
                    main_s3_bucket,
                    metadata_s3_bucket,
                    trigger_s3_bucket,
                    s3_client,
                ]
            )

        return {"statusCode": 200, "body": "Triggered with " + str(body)}
    except Exception as _:
        exception_type, exception_value, exception_traceback = sys.exc_info()
        traceback_string = traceback.format_exception(
            exception_type, exception_value, exception_traceback
        )
        err_msg = json.dumps(
            {
                "errorType": exception_type.__name__,
                "errorMessage": str(exception_value),
                "stackTrace": traceback_string,
            }
        )
        logger.error(err_msg)
        return {"statusCode": 500, "body": str(traceback.format_exc())}


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=5000)

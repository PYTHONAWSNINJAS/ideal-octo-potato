"""
This module will merge files based on control files placed in S3 folder under doc_pdf.
This will generate two pdf documents based on current and source keys in control file.
The control file has paths to the converted pdfs that needs to be merged.
"""

import json
import os

import boto3
from PyPDF2 import PdfFileMerger

import traceback
import tempfile

import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def init():
    """
    Initialises variables required for the program to operate
    Returns: all the initialised variables
    -------
    """
    try:
        lambda_write_path = tempfile.gettempdir() + "/"
        main_s3_bucket = os.environ["main_s3_bucket"]
        metadata_s3_bucket = os.environ["metadata_s3_bucket"]
        pdf_file_suffix = "_dv"

        session = boto3.Session()
        s3_client = session.client(service_name="s3")
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
    return [
        s3_client,
        main_s3_bucket,
        metadata_s3_bucket,
        lambda_write_path,
        pdf_file_suffix,
    ]


def merge_pdf(pdfs, filename):
    """

    Parameters
    ----------
    pdfs: pdf files to be merged
    filename: filename of the consolidated file
    """
    try:
        merger = PdfFileMerger()

        for pdf_file in pdfs:
            merger.append(pdf_file)

        merger.write(filename)
        merger.close()
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

def upload_to_s3(lambda_write_path, pdf_file_name, s3_client, bucket_name, s3_folder, exhibit_id):
    delay = 1  # initial delay
    delay_incr = 1  # additional delay in each loop
    max_delay = 30  # max delay of one loop. Total delay is (max_delay**2)/2

    while delay < max_delay:
        try:
            with open(os.path.join(lambda_write_path, pdf_file_name), "rb") as merged_data:
                s3_client.upload_fileobj(
                    merged_data,
                    bucket_name,
                    s3_folder + "/doc_pdf/" + exhibit_id + "/" + pdf_file_name,
                )
            break
        except ClientError:
            time.sleep(delay)
            delay += delay_incr
    else:
        logger.error(f"upload_to_s3 ERROR for: {s3_folder}")

    
def process(
    file_type,
    exhibit_id,
    data,
    s3_client,
    bucket_name,
    lambda_write_path,
    pdf_file_suffix,
    s3_folder,
):
    """

    Parameters
    ----------
    file_type: source / current file type
    exhibit_id: name of the folder
    data: control file content
    s3_client: s3 object
    bucket_name: bucket name
    lambda_write_path: lambda path /tmp
    pdf_file_suffix: _dv
    s3_folder: the upload location of the merged file
    """
    try:
        pdf_file_name = file_type + pdf_file_suffix + ".pdf"
        pdfs = []

        for item in data["files"]:
            file_name = item[file_type].split("/")[-1]
            logger.info(f"downloading: {file_name}")
            s3_client.download_file(
                bucket_name, item[file_type], lambda_write_path + file_name
            )
            pdfs.append(lambda_write_path + file_name)

        merge_pdf(pdfs, lambda_write_path + pdf_file_name)

        logger.info(f"Merged: {os.path.join(lambda_write_path, pdf_file_name)}")
        logger.info(
            f"Uploading to: {bucket_name}/{s3_folder}/doc_pdf/{exhibit_id}/{pdf_file_name}"
        )
        upload_to_s3(lambda_write_path, pdf_file_name, s3_client, bucket_name, s3_folder, exhibit_id)        
        
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


def delete_metadata_folder(
    control_file_path, metadata_s3_bucket_name, folder_type, s3_client
):
    """Delete meta data folder after merging.
    Args:
        control_file_path ([type]): the key file that
        came from the s3 trigger. Modify the path
        to get the meta data folder path.
        s3_client ([type]): s3 client object
        folder_type: wire or exhibits
    """
    try:
        metadata_folder_to_delete = (
            control_file_path.replace("doc_pdf", folder_type)
            .replace("control_files/", "")
            .replace(".json", "")
        )
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(metadata_s3_bucket_name)
        bucket.objects.filter(Prefix=metadata_folder_to_delete + "/").delete()
        logger.info(f"Deleted all files from: {metadata_folder_to_delete}")
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


def lambda_handler(event, context):
    """

    Parameters
    ----------
    event: lambda event
    context: lambda context
    """
    trigger_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    control_file = event["Records"][0]["s3"]["object"]["key"]
    s3_folder = control_file.split("/")[0]

    try:
        (
            s3_client,
            main_s3_bucket,
            metadata_s3_bucket,
            lambda_write_path,
            pdf_file_suffix,
        ) = init()

        s3_client_obj = s3_client.get_object(Bucket=main_s3_bucket, Key=control_file)
        data = json.loads(s3_client_obj["Body"].read().decode("utf-8"))
        exhibit_id = data["s3_sub_folder"]
        folder_type = data["type"]

        if not data["files"]:
            logger.info("Empty Control File.")
            delete_metadata_folder(control_file, metadata_s3_bucket, folder_type)
            s3_client.delete_object(Bucket=trigger_bucket_name, Key=control_file)

        # loop two times in the data for source and current
        for file_type in ["source", "current"]:
            process(
                file_type,
                exhibit_id,
                data,
                s3_client,
                main_s3_bucket,
                lambda_write_path,
                pdf_file_suffix,
                s3_folder,
            )

        delete_metadata_folder(control_file, metadata_s3_bucket, folder_type, s3_client)
        s3_client.delete_object(Bucket=trigger_bucket_name, Key=control_file)
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

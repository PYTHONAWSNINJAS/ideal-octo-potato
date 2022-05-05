"""
This module will merge files based on control files placed in S3 folder under doc_pdf.
This will generate two pdf documents based on current and source keys in control file.
The control file has paths to the converted pdfs that needs to be merged.
"""

import json
import os

from shutil import rmtree

import boto3
from PyPDF2 import PdfFileMerger, PdfFileReader

import traceback

import logging
import sys

import pymysql

import signal

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def init():
    """
    Initialises variables required for the program to operate
    Returns: all the initialised variables
    -------
    """
    lambda_write_path = os.environ["lambda_write_path"]
    main_s3_bucket = os.environ["main_s3_bucket"]
    metadata_s3_bucket = os.environ["metadata_s3_bucket"]
    pdf_file_suffix = "_dv"

    session = boto3.Session()
    s3_client = session.client(service_name="s3")
    return [
        s3_client,
        main_s3_bucket,
        metadata_s3_bucket,
        lambda_write_path,
        pdf_file_suffix,
    ]


def merge_pdf(pdfs, filename, batchsize):
    """

    Parameters
    ----------
    pdfs: pdf files to be merged
    filename: filename of the consolidated file
    """
    # pdfs.sort(reverse=False)
    logger.info(f"Number of pdfs to Merge: {str(len(pdfs))}")
    if len(pdfs) < batchsize:
        merger = PdfFileMerger()
        logger.info(f"pdf files: {pdfs}")
        for pdf_file in pdfs:
            with open(pdf_file, "rb") as file:
                merger.append(PdfFileReader(file))
        merger.write(filename)
        merger.close()
        logger.info(f"Creating: {filename}")
    else:
        batch_pdfs = []
        list_of_batches = []
        for count, pdf in enumerate(pdfs, 1):
            batch_pdfs.append(pdf)
            if count % batchsize == 0:
                list_of_batches.append(batch_pdfs)
                batch_pdfs = []

            if count > len(pdfs) + 2:
                logger.info("List count larger than number of PDFs. Exiting..")
                sys.exit(1)

        list_of_batches.append(batch_pdfs)
        list_of_batches = [x for x in list_of_batches if x]
        logger.info(list_of_batches)
        logger.info(f"No of batches: {str(len(list_of_batches))}")

        final_pdfs = []
        for i, batchlist in enumerate(list_of_batches):
            logger.info(
                f"Processing Batch: {str(i)} with length: {str(len(batchlist))}"
            )
            if len(batchlist) > 0:
                merger = PdfFileMerger()
                for pdf in batchlist:
                    with open(pdf, "rb") as file:
                        merger.append(PdfFileReader(file))

                merger.write(filename + str(i) + ".pdf")
                merger.close()
                final_pdfs.append(filename + str(i) + ".pdf")

        logger.info(f"Merging Final {str(len(list_of_batches))} pdf files.")
        merger = PdfFileMerger()
        for pdf_file in final_pdfs:
            merger.append(pdf_file)
        logger.info(f"Creating: {filename}")
        merger.write(filename)
        merger.close()


def upload_to_s3(pdf_file_name, s3_client, bucket_name):
    s3_path = pdf_file_name.replace(os.environ["lambda_write_path"], "")
    s3_client.upload_file(pdf_file_name, bucket_name, s3_path)


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
    pdf_file_name = (
        lambda_write_path
        + s3_folder
        + "/doc_pdf/"
        + exhibit_id.replace("document_", "")
        + "/"
        + file_type
        + pdf_file_suffix
        + ".pdf"
    )

    pdfs = []
    for item in data["files"]:
        file_path = lambda_write_path + item[file_type]
        logger.info(f"file_path: {file_path}")
        logger.info(f"dir_path: {os.path.dirname(file_path)}")
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(name=os.path.dirname(file_path), exist_ok=True)

        logger.info(f"Downloading: {item[file_type]}")
        if os.path.isdir(os.path.dirname(file_path)):
            s3_client.download_file(bucket_name, item[file_type], file_path)
        if os.path.isfile(file_path):
            logger.info("File Exists after Download. Appending to list")
            pdfs.append(file_path)
            logger.info(pdfs)
            logger.info(f"list of pdfs: {len(pdfs)}")

    merge_pdf(pdfs, pdf_file_name, 500)
    logger.info(f"Merged: {pdf_file_name}")
    logger.info(f"Uploading: {pdf_file_name}")
    if os.path.isfile(pdf_file_name):
        logger.info("File Exists after Merging. Uploading to S3.")
        upload_to_s3(pdf_file_name, s3_client, bucket_name)


def delete_metadata_folder(control_file_path, metadata_s3_bucket_name, folder_type):
    """Delete meta data folder after merging.
    Args:
        control_file_path ([type]): the key file that
        came from the s3 trigger. Modify the path
        to get the meta data folder path.
        s3_client ([type]): s3 client object
        folder_type: wire or exhibits
    """
    metadata_folder_to_delete = (
        control_file_path.replace("doc_pdf", folder_type)
        .replace("control_files/", "")
        .replace(".json", "")
    )
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(metadata_s3_bucket_name)
    bucket.objects.filter(Prefix=metadata_folder_to_delete + "/").delete()
    logger.info(f"Deleted all files from: {metadata_folder_to_delete}")


def update_rds_entry(s3_folder, exhibit_id):
    rds_host = os.environ["db_endpoint"]
    name = os.environ["db_username"]
    password = os.environ["db_password"]
    db_name = os.environ["db_name"]

    logger.info(f"Updating RDS entry for {exhibit_id}")

    conn = pymysql.connect(
        host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=50
    )

    with conn.cursor() as cur:
        cur.execute(
            "update docviewer.jobexecution set jobexecution.processed_control_files\
            =jobexecution.processed_control_files+1 , jobexecution.last_update_datetime\
            =CURRENT_TIMESTAMP where jobexecution.case_id= %s;",
            (s3_folder,),
        )
        conn.commit()
        for row in cur:
            logger.info(row)
    conn.close()


def update_rds_entry_on_unmerged(s3_folder, exhibit_id):
    rds_host = os.environ["db_endpoint"]
    name = os.environ["db_username"]
    password = os.environ["db_password"]
    db_name = os.environ["db_name"]

    logger.info(f"Updating RDS entry for unmerged control files{exhibit_id}")

    conn = pymysql.connect(
        host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=50
    )

    with conn.cursor() as cur:
        cur.execute(
            "update docviewer.jobexecution set jobexecution.unmerged_control_files\
            =jobexecution.unmerged_control_files+1 , jobexecution.last_update_datetime\
            =CURRENT_TIMESTAMP where jobexecution.case_id= %s;",
            (s3_folder,),
        )
        conn.commit()
        for row in cur:
            logger.info(row)
    conn.close()


def find_latest_versionid(bucket, key):
    s3 = boto3.client("s3")
    versions = s3.list_object_versions(Bucket=bucket, Prefix=key)
    s3versions = versions.get("Versions")
    logger.info(f"versions - {s3versions}")
    version_id = ""
    for item in s3versions:
        if item["IsLatest"]:
            version_id = item["VersionId"]
            break
    return version_id


def place_processed_control_files(s3_folder, exhibit_id, s3_client, bucket_name):
    processed_control_files_path = (
        s3_folder + "/doc_pdf/processed_control_files/" + exhibit_id + ".json"
    )
    s3_client.put_object(Body="", Bucket=bucket_name, Key=processed_control_files_path)


def timeout_handler(_signal, _frame):
    raise ValueError("Time exceeded!")


signal.signal(signal.SIGALRM, timeout_handler)


def lambda_handler(event, context):
    """

    Parameters
    ----------
    event: lambda event
    context: lambda context
    """
    try:
        signal.alarm(int(context.get_remaining_time_in_millis() / 1000) - 60)
        logger.info(f"event: {event}")
        trigger_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        control_file = event["Records"][0]["s3"]["object"]["key"]
        s3_folder = control_file.split("/")[0]
        exhibit_id = control_file.split("/")[3].split(".")[0]

        versionId = event["Records"][0]["s3"]["object"]["versionId"]
        lastestVersionId = find_latest_versionid(
            bucket=trigger_bucket_name, key=control_file
        )

        logger.info(
            f"lastestVersionId - {lastestVersionId} eventVersionId - {versionId}"
        )
        if versionId != lastestVersionId:
            logger.info("Not the latest versionId. Exiting from here.")
            return

        if exhibit_id.startswith("document"):
            folder_type = "wire"
        else:
            folder_type = "exhibits"

        (
            s3_client,
            main_s3_bucket,
            metadata_s3_bucket,
            lambda_write_path,
            pdf_file_suffix,
        ) = init()

        s3_client_obj = s3_client.get_object(Bucket=main_s3_bucket, Key=control_file)
        data = json.loads(s3_client_obj["Body"].read().decode("utf-8"))

        if not data["files"]:
            logger.info("Empty Control File.")
        else:
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

                if os.path.exists(lambda_write_path):
                    rmtree(lambda_write_path, ignore_errors=True)
        update_rds_entry(s3_folder, exhibit_id)
        place_processed_control_files(s3_folder, exhibit_id, s3_client, main_s3_bucket)
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

        logger.info("Creating unmerged control File.")
        session = boto3.Session()
        s3_client = session.client(service_name="s3")
        s3_client.put_object(
            Body="",
            Bucket=main_s3_bucket,
            Key=control_file.replace("control_files", "unmerged_control_files"),
        )

        update_rds_entry_on_unmerged(s3_folder, exhibit_id)

    delete_metadata_folder(control_file, metadata_s3_bucket, folder_type)

    s3_client.delete_object(Bucket=trigger_bucket_name, Key=control_file)

    if os.path.exists(lambda_write_path):
        rmtree(lambda_write_path, ignore_errors=True)

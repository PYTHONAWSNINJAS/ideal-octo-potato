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

def init():
    """
    Initialises variables required for the program to operate
    Returns: all the initialised variables
    -------
    """
    lambda_write_path = tempfile.gettempdir()+"/"
    main_s3_bucket = os.environ["main_s3_bucket"]
    pdf_file_suffix = "_dv"

    session = boto3.Session()
    s3_client = session.client(service_name="s3")

    return [s3_client, main_s3_bucket, lambda_write_path, pdf_file_suffix]


def merge_pdf(pdfs, filename):
    """

    Parameters
    ----------
    pdfs: pdf files to be merged
    filename: filename of the consolidated file
    """
    merger = PdfFileMerger()

    for pdf_file in pdfs:
        merger.append(pdf_file)

    merger.write(filename)
    merger.close()


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
    pdf_file_name = file_type + pdf_file_suffix + ".pdf"
    pdfs = []

    for item in data["files"]:
        file_name = item[file_type].split("/")[-1]
        print(f"downloading {file_name}")
        s3_client.download_file(
            bucket_name, item[file_type], lambda_write_path + file_name
        )
        pdfs.append(lambda_write_path + file_name)

    merge_pdf(pdfs, lambda_write_path + pdf_file_name)

    print(f"Merged - {os.path.join(lambda_write_path, pdf_file_name)}")
    print(
        "Uploading to - ",
        bucket_name + "/" + s3_folder + "/doc_pdf/" + exhibit_id + "/" + pdf_file_name,
    )
    with open(os.path.join(lambda_write_path, pdf_file_name), "rb") as merged_data:
        s3_client.upload_fileobj(
            merged_data,
            bucket_name,
            s3_folder + "/doc_pdf/" + exhibit_id + "/" + pdf_file_name,
        )


# noinspection PyShadowingNames,PyUnusedLocal
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
        s3_client, main_s3_bucket, lambda_write_path, pdf_file_suffix = init()

        s3_client_obj = s3_client.get_object(Bucket=main_s3_bucket, Key=control_file)
        data = json.loads(s3_client_obj["Body"].read().decode("utf-8"))
        exhibit_id = data["exhibit"]

        if not data["files"]:
            print("Empty Control File.")
            s3_client.delete_object(Bucket=trigger_bucket_name, Key=control_file)
            return {"statusCode": 204, "body": "Empty Control File."}

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

        s3_client.delete_object(Bucket=trigger_bucket_name, Key=control_file)
        return {"statusCode": 200, "body": "Merged"}
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        return {"statusCode": 500, "body": str(traceback.format_exc())}

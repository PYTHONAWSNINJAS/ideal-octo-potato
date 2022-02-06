"""
This module is called from main when it encounters
a doc/docx file to be processed. It loads libre office
in efs from its attached layer and converts doc/docx
into pdf and uploads in s3.
"""

import os
from io import BytesIO
import tarfile
import boto3
import subprocess
import brotli
import logging
import sys
import traceback
import json

libre_office_install_dir = os.environ["lambda_write_path"] + "/instdir"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_libre_office():
    if os.path.exists(libre_office_install_dir) and os.path.isdir(
        libre_office_install_dir
    ):
        logger.info("We have a cached copy of LibreOffice, skipping extraction")
    else:
        logger.info(
            "No cached copy of LibreOffice, extracting tar stream from Brotli file."
        )
        buffer = BytesIO()
        with open("/opt/lo.tar.br", "rb") as brotli_file:
            d = brotli.Decompressor()
            while True:
                chunk = brotli_file.read(1024)
                buffer.write(d.decompress(chunk))
                if len(chunk) < 1024:
                    break
            buffer.seek(0)

        logger.info("Extracting tar stream to tmp for caching.")
        with tarfile.open(fileobj=buffer) as tar:
            tar.extractall(os.environ["lambda_write_path"])
        logger.info("Done caching LibreOffice!")

    return f"{libre_office_install_dir}/program/soffice.bin"


def download_from_s3(bucket, key, download_path):
    s3 = boto3.client("s3")
    if not os.path.exists(os.path.dirname(download_path)):
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
    s3.download_file(bucket, key, download_path)


def upload_to_s3(file_path, bucket, key):
    s3 = boto3.client("s3")
    s3.upload_file(file_path, bucket, key)


def convert_word_to_pdf(soffice_path, word_file_path, output_dir):
    conv_cmd = f"{soffice_path} --headless --norestore --invisible --nodefault \
    --nofirststartwizard --nolockcheck --nologo \
    --convert-to pdf:writer_pdf_Export --outdir {output_dir} {word_file_path}"
    response = subprocess.run(
        conv_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if response.returncode != 0:
        response = subprocess.run(
            conv_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if response.returncode != 0:
            return False
    return True


def lambda_handler(event, context):
    bucket_name = os.environ["main_s3_bucket"]
    lambda_write_path = os.environ["lambda_write_path"]
    s3_input_file = event["s3_input_file"]
    key_prefix, base_name = os.path.split(s3_input_file)
    filename, _ = os.path.splitext(base_name)
    download_path = f"{lambda_write_path}/{s3_input_file}"
    s3_output_file = event["s3_output_file"]
    output_dir = f"{lambda_write_path}/{s3_output_file}"

    logger.info(
        f"key_prefix- {key_prefix},\
        base_name - {base_name},\
        filename - {filename},\
        _ - {_},\
        download_path - {download_path},\
        s3_output_file - {s3_output_file},\
        output_dir - {output_dir}"
    )
    
    download_from_s3(bucket_name, s3_input_file, download_path)
    soffice_path = load_libre_office()
    converted = convert_word_to_pdf(soffice_path, download_path, output_dir)
    
    if converted:
        logger.info(f"Converted to: {s3_output_file}")
        try:
            session = boto3.Session()
            s3_client = session.client(service_name="s3")
            with open(f"{output_dir}/{filename}.pdf", "rb") as data:
                s3_client.upload_fileobj(
                    data,
                    bucket_name,
                    s3_output_file,
                )
            uploaded = True
        except Exception as _:
            (
                exception_type,
                exception_value,
                exception_traceback,
            ) = sys.exc_info()
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
            logger.info("PDF not uploaded")
            uploaded = False
    else:
        logger.info("PDF not created")

    return {"response": uploaded}

"""
This module is triggered based on files placed in trigger S3 bucket.
The trigger files corresponds to each folder in
main s3 where the documents are placed.
The files in the folder are converted in a loop and stored in doc_pdf.
A Success file is created in Merge Trigger Once the process is done.
"""

import os
import sqlite3
from shutil import copyfile, rmtree

import boto3
import extract_msg
import pandas as pd
import pdfkit
import pytesseract
from PIL import Image, ImageSequence
from PyPDF2 import PdfFileMerger
from fpdf import FPDF
from reportlab.graphics import renderPM
from svglib.svglib import svg2rlg
import traceback
import time
from botocore.exceptions import ClientError
import logging
import sys
import json
import concurrent.futures
from itertools import islice

FILE_PATTERN_TO_IGNORE = "_small"
FILE_PATTERN_TO_INCLUDE = "_unredacted_original"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def download_dir(prefix, local, bucket, client):
    """

    Parameters
    ----------
    prefix: pattern to match in s3
    local: local path to folder in which to place files
    bucket: s3 bucket with target contents
    client: initialized s3 client object
    -------

    """
    keys = []
    dirs = []
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
            else:
                dirs.append(k)
        next_token = results.get("NextContinuationToken")
    for d in dirs:
        destination_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(destination_pathname)):
            os.makedirs(os.path.dirname(destination_pathname))
    for k in keys:
        destination_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(destination_pathname)):
            os.makedirs(os.path.dirname(destination_pathname))
        client.download_file(bucket, k, destination_pathname)


def create_pdf(file_path, lambda_write_path, pdf_file_name):
    """

    Parameters
    ----------
    file_path: file path of the image
    lambda_write_path: output path of the pdf file
    pdf_file_name: name of the output pdf file

    Returns True if the pdf file is created
    -------

    """
    try:
        pdf_png = pytesseract.image_to_pdf_or_hocr(file_path)
        with open(os.path.join(lambda_write_path, pdf_file_name), "w+b") as f:
            f.write(pdf_png)
        return True
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
        return False


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


def init():
    """
    Initialises variables required for the program to operate
    Returns: all the initialised variables
    -------
    """
    lambda_write_path = os.environ["lambda_write_path"]
    main_s3_bucket = os.environ["main_s3_bucket"]
    metadata_s3_bucket = os.environ["metadata_s3_bucket"]
    merge_trigger_bucket = os.environ["merge_trigger_bucket"]
    pdf_file_suffix = os.environ["pdf_file_suffix"]
    s3_output_folder = os.environ["s3_output_folder"]

    session = boto3.Session()
    s3_client = session.client(service_name="s3")

    return [
        s3_client,
        main_s3_bucket,
        lambda_write_path,
        pdf_file_suffix,
        s3_output_folder,
        metadata_s3_bucket,
        merge_trigger_bucket,
    ]


def get_pdf_object(font_size=10):
    """
    Parameters
    ----------
    font_size: Specify the font size of the texts in the pdf

    Returns
    -------
    """
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=font_size)

    return pdf


def process_document_folders(
    s3_client,
    bucket_name,
    s3_folder,
    s3_sub_folder,
    s3_document_directory,
    lambda_write_path,
    pdf_file_suffix,
    s3_output_folder,
    trigger_folder,
):
    """
    This will process all files in the Trigger folder
    in a loop and put into Main S3.

    Args:
        s3_client boto3 object: S3 Session Client Object
        bucket_name str: main s3 bucket name
        s3_folder str: s3 folder i.e case_number
        s3_sub_folder str: s3 sub folder i.e exhibits or wire
        s3_document_directory str: s3 document directory
        i.e document folder
        lambda_write_path str: lambda_write_path i.e /tmp
        pdf_file_suffix str: pdf file suffix value i.e _dv
        s3_output_folder str: S3 output folder i.e doc_pdf
        trigger_folder str: the trigger folder in main S3 for
        which the process will be executed.
    """
    for current_item in os.listdir(
        downloaded_folder_path := os.path.join(
            lambda_write_path,
            s3_folder,
            s3_sub_folder,
            s3_document_directory,
            trigger_folder,
        )
    ):
        converted = False
        pdf_file_name = ""
        s3_location = ""
        s3_object = ""
        try:
            if os.path.isdir(
                file_path := os.path.join(downloaded_folder_path, current_item)
            ) and file_path.endswith("full_marks"):
                logger.info("Inside full_marks")
                for item_in_full_marks in os.listdir(file_path):
                    logger.info(f"item_in_full_marks: {item_in_full_marks}")
                    converted = False
                    full_marks_file_path = os.path.join(file_path, item_in_full_marks)
                    logger.info(f"full_marks_file_path: {full_marks_file_path}")
                    filename, _ = os.path.splitext(full_marks_file_path)
                    pdf_file_name = "".join([filename, pdf_file_suffix, ".pdf"])
                    s3_location = os.path.join(
                        s3_folder,
                        s3_sub_folder,
                        s3_document_directory,
                        trigger_folder,
                        current_item,
                    )
                    s3_object = pdf_file_name.split(os.sep)[-1]

                    if filename.endswith(FILE_PATTERN_TO_IGNORE):
                        continue
                    if full_marks_file_path.lower().endswith(
                        (".png", ".jpg", ".gif", ".tif", ".tiff")
                    ):
                        converted = create_pdf(
                            full_marks_file_path, lambda_write_path, pdf_file_name
                        )
                    if converted:
                        logger.info(
                            f"Created: {os.path.join(lambda_write_path, pdf_file_name)}"
                        )
                        with open(
                            os.path.join(lambda_write_path, pdf_file_name), "rb"
                        ) as data:
                            s3_client.upload_fileobj(
                                data,
                                bucket_name,
                                "".join(
                                    [
                                        s3_location.replace(
                                            s3_sub_folder, s3_output_folder
                                        ),
                                        "/",
                                        s3_object,
                                    ]
                                ),
                            )
                    else:
                        logger.info(f"PDF not created for: {current_item}")
            else:
                converted = False
                filename, _ = os.path.splitext(file_path)
                pdf_file_name = "".join([filename, pdf_file_suffix, ".pdf"])
                s3_location = os.path.join(
                    s3_folder, s3_sub_folder, s3_document_directory, trigger_folder
                )
                s3_object = pdf_file_name.split(os.sep)[-1]

                logger.info(f"Processing:{file_path}")

                if filename.endswith(FILE_PATTERN_TO_IGNORE):
                    continue
                if file_path.endswith(".pdf"):
                    copyfile(file_path, pdf_file_name)
                    converted = True
                elif file_path.endswith(".mif"):
                    copyfile(file_path, temp_mif_file := "".join([filename, ".txt"]))
                    pdfkit.from_file(
                        temp_mif_file,
                        os.path.join(lambda_write_path, pdf_file_name),
                        options={"quiet": ""},
                    )
                    converted = True
                elif file_path.endswith(".txt"):
                    pdf_txt = get_pdf_object(11)
                    with open(file_path, "rb") as f:
                        lines = f.readlines()
                    for line in lines:
                        pdf_txt.write(5, str(line))
                    pdf_txt.output(os.path.join(lambda_write_path, pdf_file_name))
                    converted = True
                elif file_path.lower().endswith(
                    "".join([".png", FILE_PATTERN_TO_INCLUDE])
                ):
                    copyfile(
                        file_path,
                        temp_unredacted_file := "".join(
                            [filename, FILE_PATTERN_TO_INCLUDE, pdf_file_suffix, ".png"]
                        ),
                    )
                    pdf_file_name = "".join(
                        [filename, FILE_PATTERN_TO_INCLUDE, pdf_file_suffix, ".pdf"]
                    )
                    s3_object = pdf_file_name.split(os.sep)[-1]
                    converted = create_pdf(
                        temp_unredacted_file, lambda_write_path, pdf_file_name
                    )
                elif file_path.lower().endswith((".png", ".jpg", ".gif")):
                    converted = create_pdf(file_path, lambda_write_path, pdf_file_name)
                elif file_path.lower().endswith((".tif", ".TIF", ".tiff")):
                    converted = tiff_to_pdf(file_path, lambda_write_path, pdf_file_name)
                elif file_path.endswith((".bmp")):
                    Image.open(file_path).save(
                        os.path.join(lambda_write_path, pdf_file_name), "pdf"
                    )
                    converted = True
                elif file_path.endswith(".svg"):
                    drawing = svg2rlg(file_path, resolve_entities=True)
                    renderPM.drawToFile(
                        drawing, temp_file := "".join([filename, ".png"]), fmt="PNG"
                    )
                    converted = create_pdf(temp_file, lambda_write_path, pdf_file_name)
                elif file_path.endswith(
                    (".html", ".htm", ".xml", ".mht", ".mhtml", ".csv", ".eml")
                ):
                    if file_path.endswith("mht"):
                        copyfile(file_path, temp_file := "".join([filename, ".html"]))
                        pdfkit.from_file(
                            temp_file,
                            os.path.join(lambda_write_path, pdf_file_name),
                            options={
                                "enable-local-file-access": "",
                                "load-error-handling": "ignore",
                            },
                        )
                        converted = True
                    elif file_path.endswith(".csv"):
                        df = pd.read_csv(file_path)
                        df.to_html(temp_file := "".join([filename, ".html"]))
                        pdfkit.from_file(
                            temp_file,
                            os.path.join(lambda_write_path, pdf_file_name),
                            options={"enable-local-file-access": "", "quiet": ""},
                        )
                        converted = True
                    elif file_path.endswith((".xls", ".xlsx")):
                        temp_pdfs = []
                        xls = pd.ExcelFile(file_path)
                        for sheet_name in xls.sheet_names:
                            df = pd.read_excel(file_path, sheet_name=sheet_name)
                            df.to_html(
                                temp_file := "".join(
                                    [filename, "_", str(sheet_name), ".html"]
                                )
                            )
                            pdfkit.from_file(
                                temp_file,
                                temp_pdf := "".join(
                                    [filename, "_", str(sheet_name), ".pdf"]
                                ),
                                options={"enable-local-file-access": "", "quiet": ""},
                            )
                            temp_pdfs.append(temp_pdf)
                        merge_pdf(
                            temp_pdfs, os.path.join(lambda_write_path, pdf_file_name)
                        )
                        converted = True
                    elif file_path.endswith((".eml")):
                        copyfile(file_path, temp_file := "".join([filename, ".txt"]))

                        with open(temp_file, "rb") as myfile:
                            head = list(islice(myfile, 1000))

                        with open(temp_file, mode="wb") as f2:
                            for item in head:
                                if item.strip() == "Content-Disposition: attachment;":
                                    break
                                f2.write(item)

                        pdfkit.from_file(
                            temp_file,
                            os.path.join(lambda_write_path, pdf_file_name),
                        )
                        converted = True
                    else:
                        try:
                            pdfkit.from_file(
                                file_path,
                                os.path.join(lambda_write_path, pdf_file_name),
                                options={"enable-local-file-access": "", "quiet": ""},
                            )
                            converted = True
                        except Exception as _:
                            logger.info(f"Trying again: {filename}")
                            copyfile(
                                file_path, temp_file := "".join([filename, ".txt"])
                            )
                            pdfkit.from_file(
                                temp_file,
                                os.path.join(lambda_write_path, pdf_file_name),
                                options={"quiet": ""},
                            )
                elif file_path.endswith(".msg"):
                    msg_properties = []
                    msg = extract_msg.Message(file_path)

                    try:
                        msg_properties.extend(
                            [
                                msg.date,
                                "",
                                "".join(filter(None, ["To:", msg.to])),
                                "",
                                msg.subject,
                                "".join(filter(None, ["", msg.Body])),
                                "".join(filter(None, ["From:", msg.sender])),
                            ]
                        )
                    except Exception as _:
                        msg_properties.extend(
                            [
                                msg.date,
                                "",
                                "".join(filter(None, ["To:", msg.to])),
                                "",
                                msg.subject,
                                "".join(filter(None, ["From:", msg.sender])),
                            ]
                        )

                    pdf_email = get_pdf_object(12)
                    for i in msg_properties:
                        pdf_email.write(5, str(i))
                        pdf_email.ln()

                    pdf_email.output(os.path.join(lambda_write_path, pdf_file_name))
                    converted = True
                # elif file_path.endswith(".db"):
                #     con = sqlite3.connect(file_path)
                #     df = pd.read_sql_query("select * from <table_name>", con)
                #     df.to_html(temp_file := "".join([filename, ".html"]))
                #     pdfkit.from_file(
                #         temp_file, os.path.join(lambda_write_path, pdf_file_name)
                #     )
                #     converted = True

        except Exception as e:
            if "Done" not in str(e):
                if file_path.endswith("mht"):
                    converted = True
                else:
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
            else:
                converted = True

        if converted:
            logger.info(f"Created: {os.path.join(lambda_write_path, pdf_file_name)}")
            with open(os.path.join(lambda_write_path, pdf_file_name), "rb") as data:
                s3_client.upload_fileobj(
                    data,
                    bucket_name,
                    "".join(
                        [
                            s3_location.replace(s3_sub_folder, s3_output_folder),
                            "/",
                            s3_object,
                        ]
                    ),
                )
        else:
            logger.info(f"PDF not created for: {current_item}")


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
    delay = 1  # initial delay
    delay_incr = 1  # additional delay in each loop
    max_delay = 30  # max delay of one loop. Total delay is (max_delay**2)/2

    while delay < max_delay:
        try:
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
            break
        except ClientError:
            time.sleep(delay)
            delay += delay_incr
    else:
        logger.info(f"list dir PDF ERROR for: {prefix}")
    return keys


def fetch_metadata_file(s3_client, meta_data_object_folder, metadata_s3_bucket):
    """
    Parameters
    ----------
    s3_client: Client Object to Access S3
    meta_data_object_folder: Location of the metadata object
    metadata_s3_bucket: metadata bucket name

    Returns
    -------
    total_no_of_trigger_files: no of trigger files
    """
    try:
        pattern_to_look = meta_data_object_folder.split("/")[-2]
        objects = list_dir(
            prefix=meta_data_object_folder, bucket=metadata_s3_bucket, client=s3_client
        )
        meta_data_object = [
            item for item in objects if item.split("/")[-1].startswith(pattern_to_look)
        ][0]
        total_no_of_trigger_files = int(meta_data_object.split("/")[-1].split("___")[1])
        logger.info(f"meta_data_object: {meta_data_object}")
        logger.info(f"total_no_of_trigger_files: {total_no_of_trigger_files}")
        return total_no_of_trigger_files
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


def create_success_file(s3_client, bucket, file):
    """
    Parameters
    ----------
    s3_client: S3 Object
    bucket: Bucket Name
    file: File Name
    """
    logger.info("Creating Success Files")

    delay = 1  # initial delay
    delay_incr = 1  # additional delay in each loop
    max_delay = 30  # max delay of one loop. Total delay is (max_delay**2)/2

    while delay < max_delay:
        try:
            s3_client.put_object(Body="", Bucket=bucket, Key=file)
            break
        except ClientError:
            time.sleep(delay)
            delay += delay_incr
    else:
        logger.info(f"create_success_file ERROR for: {file}")


def count_success_files(s3_client, metadata_s3_bucket, meta_data_object_folder):
    """
    Parameters
    ----------
    s3_client S3 Object
    metadata_s3_bucket metadata bucket name
    meta_data_object_folder metadata folder path

    Returns
    -------
    size of success objects for each run as parallel lambdas are running.
    """
    objects = list_dir(
        prefix=meta_data_object_folder, bucket=metadata_s3_bucket, client=s3_client
    )
    success_objects = [
        item for item in objects if item.split("/")[-1].startswith("Success")
    ]
    return len(success_objects)


def create_merge_trigger_file(s3_client, bucket, file):
    """
    Parameters
    ----------
    s3_client
    bucket
    file
    """
    logger.info("Creating Merge Trigger File")

    delay = 1  # initial delay
    delay_incr = 1  # additional delay in each loop
    max_delay = 30  # max delay of one loop. Total delay is (max_delay**2)/2

    while delay < max_delay:
        try:
            s3_client.put_object(Body="", Bucket=bucket, Key=file)
            break
        except ClientError:
            time.sleep(delay)
            delay += delay_incr
    else:
        logger.info(f"Creating Merge Trigger File ERROR for: {file}")


def remove_files_from_metadata_bucket(
    s3_client, metadata_s3_bucket, meta_data_object_folder
):
    """
    Parameters
    ----------
    s3_client
    metadata_s3_bucket
    meta_data_object_folder
    """
    objects = list_dir(
        prefix=meta_data_object_folder, bucket=metadata_s3_bucket, client=s3_client
    )
    logger.info("Removing Objects")
    for item in objects:
        delay = 1  # initial delay
        delay_incr = 1  # additional delay in each loop
        max_delay = 30  # max delay of one loop. Total delay is (max_delay**2)/2
        while delay < max_delay:
            try:
                s3_client.delete_object(Bucket=metadata_s3_bucket, Key=item)
                break
            except ClientError:
                time.sleep(delay)
                delay += delay_incr
        else:
            logger.info(f"ERROR for: {meta_data_object_folder,item}")


def process_tiff(args):
    lambda_write_path, i, page = args
    tmp_image_path = os.path.join(lambda_write_path, "temp_image_" + str(i) + ".png")
    x, y = page.size
    page = page.resize((int(x - x * 0.25), int(y - y * 0.25)), Image.ANTIALIAS)
    page.save(tmp_image_path)

    tmp_pdf_file_name = tmp_image_path.replace(".png", ".pdf")
    _ = create_pdf(tmp_image_path, lambda_write_path, tmp_pdf_file_name)
    logger.info(f"Created: {tmp_pdf_file_name}")
    return os.path.join(lambda_write_path, tmp_pdf_file_name)


def tiff_to_pdf(file_path, lambda_write_path, pdf_file_name):
    """

    Parameters
    ----------
    file_path: file path of the image
    lambda_write_path: output path of the pdf file
    pdf_file_name: name of the output pdf file

    Returns True if the pdf file is created
    -------

    """
    try:
        image = Image.open(file_path)
        pdfs = []
        args = []
        for i, page in enumerate(ImageSequence.Iterator(image)):
            stuffs = []
            stuffs.extend([lambda_write_path, i, page.convert("L")])
            args.append(stuffs)

        with concurrent.futures.ThreadPoolExecutor() as executer:
            results = executer.map(process_tiff, args)

            for res in results:
                pdfs.append(res)

        if len(pdfs) == 1:
            _ = create_pdf(file_path, lambda_write_path, pdf_file_name)
        else:
            merge_pdf(pdfs, pdf_file_name)
        return True
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
        return False


def lambda_handler(event, context):
    """

    Parameters
    ----------
    event: lambda event
    context: lambda context
    """
    logger.info(f"event: {event}")
    trigger_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    folder_path = event["Records"][0]["s3"]["object"]["key"]
    s3_folder = folder_path.split("/")[0]
    s3_sub_folder = folder_path.split("/")[1]
    s3_document_folder = folder_path.split("/")[2]
    trigger_folder = folder_path.split("/")[3]

    (
        s3_client,
        bucket_name,
        lambda_write_path,
        pdf_file_suffix,
        s3_output_folder,
        metadata_s3_bucket,
        merge_trigger_bucket,
    ) = init()

    download_dir(
        prefix=folder_path,
        local=lambda_write_path,
        bucket=bucket_name,
        client=s3_client,
    )

    process_document_folders(
        s3_client,
        bucket_name,
        s3_folder,
        s3_sub_folder,
        s3_document_folder,
        lambda_write_path,
        pdf_file_suffix,
        s3_output_folder,
        trigger_folder,
    )
    rmtree(lambda_write_path + folder_path)
    s3_client.delete_object(Bucket=trigger_bucket_name, Key=folder_path)

    meta_data_object_folder = "".join(
        [s3_folder, "/", s3_sub_folder, "/", s3_document_folder, "/"]
    )
    logger.info(f"meta_data_object_folder: {meta_data_object_folder}")
    total_no_of_trigger_files = fetch_metadata_file(
        s3_client, meta_data_object_folder, metadata_s3_bucket
    )
    create_success_file(
        s3_client,
        metadata_s3_bucket,
        meta_data_object_folder + "Success_" + trigger_folder,
    )
    no_of_success_files = count_success_files(
        s3_client, metadata_s3_bucket, meta_data_object_folder
    )
    if no_of_success_files == total_no_of_trigger_files:
        merge_trigger_file = "".join(
            [
                s3_folder,
                "/",
                s3_output_folder,
                "/",
                "control_files",
                "/",
                s3_document_folder,
                ".json",
            ]
        )
        logger.info(f"merge_trigger_file: f{merge_trigger_file}")
        create_merge_trigger_file(s3_client, merge_trigger_bucket, merge_trigger_file)
        # remove_files_from_metadata_bucket(
        #     s3_client, metadata_s3_bucket, meta_data_object_folder
        # )

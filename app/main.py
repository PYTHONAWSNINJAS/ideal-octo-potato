"""
This module is triggered based on files placed in trigger S3 bucket.
The trigger files corresponds to each folder in
main s3 where the documents are placed.
The files in the folder are converted in a loop and stored in doc_pdf.
A Success file is created in Merge Trigger Once the process is done.
"""

import concurrent.futures
import json
import logging
import os
import sys
import time
import traceback
from itertools import islice
from shutil import copyfile, rmtree

import boto3
import extract_msg
import pandas as pd
import pdfkit
import pytesseract
from PIL import Image, ImageSequence
from PyPDF2 import PdfFileMerger
from botocore.exceptions import ClientError
from fpdf import FPDF
from reportlab.graphics import renderPM
from svglib.svglib import svg2rlg
import signal

FILE_PATTERN_TO_INCLUDE = "_unredacted_original"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

unprocess_file = None
unprocess_bucket = None


def download_file(prefix, destination_pathname, bucket, client):
    """
    Parameters
    ----------
    prefix: pattern to match in s3
    destination_pathname: local path to folder in which to place files
    bucket: s3 bucket with target contents
    client: initialized s3 client object
    -------
    """
    if not os.path.exists(os.path.dirname(destination_pathname)):
        os.makedirs(os.path.dirname(destination_pathname), exist_ok=True)

    client.download_file(bucket, prefix, destination_pathname)


def create_pdf(file_path, pdf_file_name):
    """
    Parameters
    ----------
    file_path: file path of the image
    pdf_file_name: name of the output pdf file
    Returns True if the pdf file is created
    -------
    """
    try:
        logger.info(f"Creating PDF for: {file_path}")
        pdf_png = pytesseract.image_to_pdf_or_hocr(file_path)

        if not os.path.exists(os.path.dirname(pdf_file_name)):
            os.makedirs(os.path.dirname(pdf_file_name), exist_ok=True)

        with open(pdf_file_name, "w+b") as f:
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

        for pdf_file in pdfs:
            os.remove(pdf_file)

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
    Returns: all the initialised variable
    -------
    """
    lambda_write_path = os.environ["lambda_write_path"]
    main_s3_bucket = os.environ["main_s3_bucket"]
    metadata_s3_bucket = os.environ["metadata_s3_bucket"]
    merge_trigger_bucket = os.environ["merge_trigger_bucket"]
    session = boto3.Session()
    s3_client = session.client(service_name="s3")
    return [
        s3_client,
        main_s3_bucket,
        lambda_write_path,
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


def update_image_dpi(image_file):
    logger.info(f"Changing Image dpi of:{image_file}")
    temp_image = Image.open(image_file)
    temp_image.save(image_file, dpi=(300, 300))


def process_document_folders(
    s3_client, s3_input_file, input_file, pdf_file_name, s3_output_file, bucket_name
):
    """
    This will process all files in the Trigger folder
    in a loop and put into Main S3.
    Args:
        s3_client boto3 object: S3 Session Client Object
        trigger_folder str: the trigger folder in main S3 for
        which the process will be executed.
    """
    global unprocess_file
    global unprocess_bucket

    unprocess_file = s3_input_file
    unprocess_bucket = bucket_name

    Success_Flag = True

    download_file(
        prefix=s3_input_file,
        destination_pathname=input_file,
        bucket=bucket_name,
        client=s3_client,
    )

    converted = False

    try:
        logger.info(f"Processing:{input_file}")
        filename, _ = os.path.splitext(input_file)

        if input_file.endswith(".pdf"):
            copyfile(input_file, pdf_file_name)
            converted = True

        elif input_file.endswith(".mif"):
            copyfile(input_file, temp_mif_file := "".join([filename, ".txt"]))
            pdfkit.from_file(
                temp_mif_file,
                pdf_file_name,
                options={"quiet": ""},
            )
            converted = True

        elif input_file.endswith(".txt"):
            pdf_txt = get_pdf_object(11)
            with open(input_file, "rb") as f:
                lines = f.readlines()
            for line in lines:
                pdf_txt.write(5, str(line))
            pdf_txt.output(pdf_file_name)
            converted = True

        elif input_file.lower().endswith("".join([".png", FILE_PATTERN_TO_INCLUDE])):
            copyfile(
                input_file,
                temp_unredacted_file := "".join(
                    [filename, FILE_PATTERN_TO_INCLUDE, ".png"]
                ),
            )

            update_image_dpi(temp_unredacted_file)
            converted = create_pdf(temp_unredacted_file, pdf_file_name)

        elif input_file.lower().endswith("".join([".jpeg", FILE_PATTERN_TO_INCLUDE])):
            copyfile(
                input_file,
                temp_unredacted_file := "".join(
                    [filename, FILE_PATTERN_TO_INCLUDE, ".jpeg"]
                ),
            )

            update_image_dpi(temp_unredacted_file)
            converted = create_pdf(temp_unredacted_file, pdf_file_name)

        elif input_file.lower().endswith((".png", ".jpg", ".jpeg", ".gif")):
            update_image_dpi(input_file)
            converted = create_pdf(input_file, pdf_file_name)

        elif input_file.lower().endswith((".tif", ".TIF", ".tiff")):
            converted = tiff_to_pdf(input_file, pdf_file_name)

        elif input_file.endswith((".bmp")):
            update_image_dpi(input_file)
            Image.open(input_file).save(pdf_file_name)
            converted = True

        elif input_file.endswith(".svg"):
            update_image_dpi(input_file)
            drawing = svg2rlg(input_file, resolve_entities=True)
            renderPM.drawToFile(
                drawing, temp_file := "".join([filename, ".png"]), fmt="PNG"
            )
            converted = create_pdf(temp_file, pdf_file_name)

        elif input_file.endswith(
            (".html", ".htm", ".xml", ".mht", ".mhtml", ".csv", ".eml")
        ):
            if input_file.endswith("mht"):
                copyfile(input_file, temp_file := "".join([filename, ".html"]))
                pdfkit.from_file(
                    temp_file,
                    pdf_file_name,
                    options={
                        "enable-local-file-access": "",
                        "load-error-handling": "ignore",
                    },
                )
                converted = True

            elif input_file.endswith(".csv"):
                df = pd.read_csv(input_file)
                df.to_html(temp_file := "".join([filename, ".html"]))
                pdfkit.from_file(
                    temp_file,
                    pdf_file_name,
                    options={"enable-local-file-access": "", "quiet": ""},
                )
                converted = True

            elif input_file.endswith((".xls", ".xlsx")):
                temp_pdfs = []
                xls = pd.ExcelFile(input_file)
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(input_file, sheet_name=sheet_name)
                    df.to_html(
                        temp_file := "".join([filename, "_", str(sheet_name), ".html"])
                    )
                    pdfkit.from_file(
                        temp_file,
                        temp_pdf := "".join([filename, "_", str(sheet_name), ".pdf"]),
                        options={"enable-local-file-access": "", "quiet": ""},
                    )
                    temp_pdfs.append(temp_pdf)
                merge_pdf(temp_pdfs, pdf_file_name)
                converted = True

            elif input_file.endswith((".eml")):
                copyfile(input_file, temp_file := "".join([filename, ".txt"]))
                with open(temp_file, "rb") as myfile:
                    head = list(islice(myfile, 1000))
                with open(temp_file, mode="wb") as f2:
                    for item in head:
                        if item.strip() == "Content-Disposition: attachment;":
                            break
                        f2.write(item)
                pdfkit.from_file(
                    temp_file,
                    pdf_file_name,
                )
                converted = True
            else:
                try:
                    pdfkit.from_file(
                        input_file,
                        pdf_file_name,
                        options={"enable-local-file-access": "", "quiet": ""},
                    )
                    converted = True
                except Exception as _:
                    logger.info(f"Trying again: {filename}")
                    copyfile(input_file, temp_file := "".join([filename, ".txt"]))
                    pdfkit.from_file(
                        temp_file,
                        pdf_file_name,
                        options={"quiet": ""},
                    )

        elif input_file.endswith(".msg"):
            msg_properties = []
            msg = extract_msg.Message(input_file)
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

            pdf_email.output(pdf_file_name)
            converted = True

        elif input_file.endswith((".doc", ".docx")):
            lambda_client = boto3.client("lambda")
            payload = json.dumps(
                {"s3_input_file": s3_input_file, "s3_output_file": s3_output_file}
            )
            logger.info("Invoking Doc Processing Lambda")
            response = lambda_client.invoke(
                FunctionName=os.environ["doc_to_pdf_arn"],
                InvocationType="RequestResponse",
                Payload=payload,
            )

            if json.loads(response["Payload"].read())["response"]:
                logger.info(f"{input_file} Processed")

    except Exception as e:

        if "Done" not in str(e):
            if input_file.endswith("mht"):
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
                logger.info("Creating Unprocessed File.")

                session = boto3.Session()
                s3_client = session.client(service_name="s3")
                s3_client.put_object(
                    Body="",
                    Bucket=unprocess_bucket,
                    Key=unprocess_file.replace(
                        unprocess_file.split("/")[1], "doc_pdf/unprocessed_files"
                    ),
                )
                Success_Flag = False
        else:
            converted = True
    try:
        if converted:
            logger.info(f"Created: {pdf_file_name}")

            with open(pdf_file_name, "rb") as data:
                s3_client.upload_fileobj(data, bucket_name, s3_output_file)
        else:
            logger.info(
                f"PDF not created for: {input_file}. Creating Unprocessed File."
            )

            session = boto3.Session()
            s3_client = session.client(service_name="s3")
            s3_client.put_object(
                Body="",
                Bucket=unprocess_bucket,
                Key=unprocess_file.replace(
                    unprocess_file.split("/")[1], "doc_pdf/unprocessed_files"
                ),
            )
            Success_Flag = False

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

        logger.info("Creating Unprocessed File.")

        session = boto3.Session()
        s3_client = session.client(service_name="s3")
        s3_client.put_object(
            Body="",
            Bucket=unprocess_bucket,
            Key=unprocess_file.replace(
                unprocess_file.split("/")[1], "doc_pdf/unprocessed_files"
            ),
        )
        Success_Flag = False

    return Success_Flag


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
    total_no_of_trigger_files = 0

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

    return total_no_of_trigger_files


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
            logger.info(f"ERROR for: {meta_data_object_folder, item}")


def process_tiff(args):
    file_path, i, page = args
    tmp_image_path = os.path.join(
        file_path.replace(file_path.split("/")[-1], ""), "temp_image_" + str(i) + ".png"
    )

    logger.info(f"temp image path: {tmp_image_path}")
    x, y = page.size
    logger.info(f"x, y: {x, y}")
    page = page.resize((int(x - x * 0.25), int(y - y * 0.25)), Image.ANTIALIAS)
    page.save(tmp_image_path)
    tmp_pdf_file_name = tmp_image_path.replace(".png", ".pdf")
    _ = create_pdf(tmp_image_path, tmp_pdf_file_name)
    logger.info(f"Created tmp pdf: {tmp_pdf_file_name}")
    return tmp_pdf_file_name


def tiff_to_pdf(file_path, pdf_file_name):
    """
    Parameters
    ----------
    file_path: file path of the image
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
            stuffs.extend([file_path, i, page.convert("RGB")])
            args.append(stuffs)

        with concurrent.futures.ThreadPoolExecutor() as executer:
            results = executer.map(process_tiff, args)
            for res in results:
                pdfs.append(res)

        if len(pdfs) == 1:
            copyfile(pdfs[0], pdf_file_name)
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


def read_control_file(
    control_file_path, bucket, client, folder_path, lambda_write_path
):
    result = client.get_object(Bucket=bucket, Key=control_file_path)
    text = result["Body"].read().decode()
    file_list = json.loads(text)["files"]

    control_file_data = []
    for item in file_list:
        source_temp_item = {}
        current_temp_item = {}

        if folder_path in item["source_img"]:
            source_temp_item["info"] = "Source"
            source_temp_item["s3_input"] = item["source_img"]
            source_temp_item["efs_input"] = os.path.join(
                lambda_write_path, item["source_img"]
            )
            source_temp_item["efs_output"] = os.path.join(
                lambda_write_path, item["source"]
            )
            source_temp_item["s3_output"] = item["source"]
            control_file_data.append(source_temp_item)

        if folder_path in item["current_img"]:
            current_temp_item["info"] = "Current"
            current_temp_item["s3_input"] = item["current_img"]
            current_temp_item["efs_input"] = os.path.join(
                lambda_write_path, item["current_img"]
            )
            current_temp_item["efs_output"] = os.path.join(
                lambda_write_path, item["current"]
            )
            current_temp_item["s3_output"] = item["current"]
            control_file_data.append(current_temp_item)

    return control_file_data


def timeout_handler(_signal, _frame):
    global unprocess_bucket
    global unprocess_file

    logger.info("Time exceeded! Creating Unprocessed File.")

    session = boto3.Session()
    s3_client = session.client(service_name="s3")
    s3_client.put_object(
        Body="",
        Bucket=unprocess_bucket,
        Key=unprocess_file.replace(
            unprocess_file.split("/")[1], "doc_pdf/unprocessed_files"
        ),
    )


signal.signal(signal.SIGALRM, timeout_handler)


def lambda_handler(event, context):
    try:
        signal.alarm(int(context.get_remaining_time_in_millis() / 1000) - 15)
        logger.info(f"event: {event}")
        trigger_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        folder_path = event["Records"][0]["s3"]["object"]["key"]
        s3_folder = folder_path.split("/")[0]
        s3_sub_folder = folder_path.split("/")[1]
        s3_document_folder = folder_path.split("/")[2]
        trigger_folder = folder_path.split("/")[3]
        control_file_path = "/".join(
            [s3_folder, "doc_pdf", "control_files", s3_document_folder + ".json"]
        )
        (
            s3_client,
            bucket_name,
            lambda_write_path,
            metadata_s3_bucket,
            merge_trigger_bucket,
        ) = init()

        filtered_control_file = read_control_file(
            control_file_path=control_file_path,
            bucket=bucket_name,
            client=s3_client,
            folder_path=folder_path,
            lambda_write_path=lambda_write_path,
        )

        all_flags = []
        for item in filtered_control_file:
            Success_Flag = process_document_folders(
                s3_client,
                item["s3_input"],
                item["efs_input"],
                item["efs_output"],
                item["s3_output"],
                bucket_name,
            )
            all_flags.append(Success_Flag)

        s3_client.delete_object(Bucket=trigger_bucket_name, Key=folder_path)

        meta_data_object_folder = "".join(
            [s3_folder, "/", s3_sub_folder, "/", s3_document_folder, "/"]
        )

        logger.info(f"meta_data_object_folder: {meta_data_object_folder}")

        total_no_of_trigger_files = fetch_metadata_file(
            s3_client, meta_data_object_folder, metadata_s3_bucket
        )

        if all(all_flags):
            logger.info("All flags are True. Creating success file.")
            create_success_file(
                s3_client,
                metadata_s3_bucket,
                meta_data_object_folder + "Success_" + trigger_folder,
            )

        no_of_success_files = count_success_files(
            s3_client, metadata_s3_bucket, meta_data_object_folder
        )

        if no_of_success_files == total_no_of_trigger_files:
            logger.info(
                f"All files are processed. Creating merge file {control_file_path}"
            )

            try:
                s3_client.head_object(
                    Bucket=merge_trigger_bucket, Key=control_file_path
                )

            except ClientError as _:
                create_merge_trigger_file(
                    s3_client, merge_trigger_bucket, control_file_path
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

    if os.path.exists(lambda_write_path):
        rmtree(lambda_write_path, ignore_errors=True)

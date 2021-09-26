import os
from shutil import copyfile, rmtree

import boto3
import pandas as pd
import pdfkit
import pytesseract
from fpdf import FPDF
from PIL import Image
from PyPDF2 import PdfFileMerger
from reportlab.graphics import renderPM
from svglib.svglib import svg2rlg
import extract_msg
import sqlite3

FILE_PATTERN_TO_IGNORE = '_small'
FILE_PATTERN_TO_INCLUDE = '_unredacted_original'


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
        pdf_png = pytesseract.image_to_pdf_or_hocr(file_path, extension="pdf")
        with open(os.path.join(lambda_write_path, pdf_file_name), "w+b") as f:
            f.write(pdf_png)
        return True
    except Exception as e:
        print(e)
        return False


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


def init():
    """

    Initialises variables required for the program to operate
    Returns: all the initialised variables
    -------

    """

    lambda_write_path = "/tmp/"
    main_s3_bucket = os.environ["main_s3_bucket"]
    pdf_file_suffix = os.environ["pdf_file_suffix"]
    s3_output_folder = os.environ["s3_output_folder"]

    session = boto3.Session()
    s3_client = session.client(service_name="s3")

    return [s3_client, main_s3_bucket, lambda_write_path, pdf_file_suffix, s3_output_folder]


def get_pdf_object(font_size=10):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=font_size)

    return pdf


def process_document_folders(s3_client, bucket_name, s3_folder, s3_sub_folder, s3_document_directory, lambda_write_path,
                             pdf_file_suffix, s3_output_folder, trigger_folder):
    """
    This will process all files in the Trigger folder in a loop and put into Main S3.

    Args:
        s3_client boto3 object: S3 Session Client Object
        bucket_name str: main s3 bucket name
        s3_folder str: s3 folder i.e case_number
        s3_sub_folder str: s3 subfolder i.e exhibits
        s3_document_directory str: s3 document directory i.e document folder
        lambda_write_path str: lambda_write_path i.e /tmp
        pdf_file_suffix str: pdf file suffix value i.e _dv
        s3_output_folder str: S3 output folder i.e doc_pdf
        trigger_folder str: the trigger folder in main S3 for which the process will be executed.
    """
    for current_item in os.listdir(
            downloaded_folder_path := os.path.join(lambda_write_path, s3_folder, s3_sub_folder, s3_document_directory,
                                                   trigger_folder)):
        try:
            if os.path.isdir(file_path := os.path.join(downloaded_folder_path, current_item)) and file_path.endswith(
                    'full_marks'):
                for item_in_full_marks in os.listdir(file_path):
                    converted = False
                    file_path = os.path.join(file_path, item_in_full_marks)
                    filename, file_extension = os.path.splitext(file_path)
                    pdf_file_name = ''.join([filename, pdf_file_suffix, ".pdf"])
                    s3_location = os.path.join(s3_folder, s3_sub_folder, s3_document_directory, trigger_folder,
                                               current_item)
                    s3_object = pdf_file_name.split(os.sep)[-1]

                    if filename.endswith(FILE_PATTERN_TO_IGNORE):
                        continue
                    elif file_path.lower().endswith((".png", ".jpg", ".gif", ".tif", ".tiff")):
                        converted = create_pdf(file_path, lambda_write_path, pdf_file_name)
                    if converted:
                        print(f"Created - {os.path.join(lambda_write_path, pdf_file_name)}")
                        with open(os.path.join(lambda_write_path, pdf_file_name), "rb") as data:
                            s3_client.upload_fileobj(data, bucket_name, ''.join(
                                [s3_location.replace(s3_sub_folder, s3_output_folder), "/", s3_object]))
                    else:
                        print(f"PDF not created for - {current_item}")
            else:
                converted = False
                filename, file_extension = os.path.splitext(file_path)
                pdf_file_name = ''.join([filename, pdf_file_suffix, ".pdf"])
                s3_location = os.path.join(s3_folder, s3_sub_folder, s3_document_directory, trigger_folder)
                s3_object = pdf_file_name.split(os.sep)[-1]

                print(f"\nProcessing {file_path}")

                if filename.endswith(FILE_PATTERN_TO_IGNORE):
                    continue
                elif file_path.endswith(".pdf"):
                    copyfile(file_path, pdf_file_name)
                    converted = True
                elif file_path.endswith(".mif"):
                    try:
                        copyfile(file_path, temp_mif_file := ''.join([filename, ".txt"]))
                        pdfkit.from_file(temp_mif_file, os.path.join(lambda_write_path, pdf_file_name),
                                         options={'quiet': ''})
                        converted = True
                    except Exception as e:
                        print(e)
                elif file_path.endswith(".txt"):
                    pdf_txt = get_pdf_object(10)
                    with open(file_path, "r") as f:
                        lines = f.readlines()
                    for line in lines:
                        pdf_txt.write(5, str(line))
                    pdf_txt.output(os.path.join(lambda_write_path, pdf_file_name))
                    converted = True
                elif file_path.lower().endswith(''.join([".png", FILE_PATTERN_TO_INCLUDE])):
                    copyfile(file_path, temp_unredacted_file := ''.join(
                        [filename, FILE_PATTERN_TO_INCLUDE, pdf_file_suffix, ".png"]))
                    pdf_file_name = ''.join([filename, FILE_PATTERN_TO_INCLUDE, pdf_file_suffix, ".pdf"])
                    s3_object = pdf_file_name.split(os.sep)[-1]
                    converted = create_pdf(temp_unredacted_file, lambda_write_path, pdf_file_name, temp_file=True)
                elif file_path.lower().endswith((".png", ".jpg", ".gif", ".tif", ".tiff")):
                    converted = create_pdf(file_path, lambda_write_path, pdf_file_name)
                elif file_path.endswith((".pcd", ".bmp")):
                    Image.open(file_path).save(temp_file := ''.join([filename, ".png"]))
                    converted = create_pdf(temp_file, lambda_write_path, pdf_file_name, temp_file=True)
                elif file_path.endswith(".svg"):
                    drawing = svg2rlg(file_path, resolve_entities=True)
                    renderPM.drawToFile(drawing, temp_file := ''.join([filename, ".png"]), fmt="PNG")
                    converted = create_pdf(temp_file, lambda_write_path, pdf_file_name, temp_file=True)
                elif file_path.endswith((".html", ".htm", ".xml", ".mht", ".mhtml", ".csv", ".eml")):
                    if file_path.endswith("mht"):
                        copyfile(file_path, temp_file := ''.join([filename, ".html"]))
                        pdfkit.from_file(temp_file, os.path.join(lambda_write_path, pdf_file_name),
                                         options={"enable-local-file-access": "", "quiet": ""})
                    elif file_path.endswith(".csv"):
                        df = pd.read_csv(file_path)
                        df.to_html(temp_file := ''.join([filename, ".html"]))
                        pdfkit.from_file(temp_file, os.path.join(lambda_write_path, pdf_file_name),
                                         options={"enable-local-file-access": "", "quiet": ""})
                    elif file_path.endswith((".xls", ".xlsx")):
                        temp_pdfs = []
                        xls = pd.ExcelFile(file_path)
                        for sheet_name in xls.sheet_names:
                            df = pd.read_excel(file_path, sheet_name=sheet_name)
                            df.to_html(temp_file := ''.join([filename, "_", str(sheet_name), ".html"]))
                            pdfkit.from_file(temp_file, temp_pdf := ''.join([filename, "_", str(sheet_name), ".pdf"]),
                                             options={"enable-local-file-access": "", "quiet": ""})
                            temp_pdfs.append(temp_pdf)
                        merge_pdf(temp_pdfs, os.path.join(lambda_write_path, pdf_file_name))
                    else:
                        try:
                            pdfkit.from_file(file_path, os.path.join(lambda_write_path, pdf_file_name),
                                             options={"enable-local-file-access": "", "quiet": ""})
                        except Exception as e:
                            print(e)
                            print("\nTrying again\n")
                            copyfile(file_path, temp_file := ''.join([filename, ".txt"]))
                            pdfkit.from_file(temp_file, os.path.join(lambda_write_path, pdf_file_name),
                                             options={"quiet": ""})
                    converted = True
                elif file_path.endswith(".msg"):
                    msg_properties = []
                    msg = extract_msg.Message(file_path)
                    msg_properties.extend([msg.date, '', ''.join(['To:', msg.to]), '', msg.subject, msg.body,
                                           ''.join(['From:', msg.sender])])

                    pdf_email = get_pdf_object(12)
                    for i in msg_properties:
                        pdf_email.write(5, str(i))
                        pdf_email.ln()

                    pdf_email.output(os.path.join(lambda_write_path, pdf_file_name))
                    converted = True
                elif file_path.endswith('.db'):
                    con = sqlite3.connect(file_path)
                    df = pd.read_sql_query("select * from <table_name>", con)
                    df.to_html(temp_file := ''.join([filename, ".html"]))
                    pdfkit.from_file(temp_file, os.path.join(lambda_write_path, pdf_file_name))

        except Exception as e:
            print(e)

        if converted:
            print(f"Created - {os.path.join(lambda_write_path, pdf_file_name)}")
            with open(os.path.join(lambda_write_path, pdf_file_name), "rb") as data:
                s3_client.upload_fileobj(data, bucket_name, ''.join(
                    [s3_location.replace(s3_sub_folder, s3_output_folder), "/", s3_object]))
        else:
            print(f"PDF not created for - {current_item}")


def lambda_handler(event, context):
    """

    Parameters
    ----------
    event: lambda event
    context: lambda context
    """
    print(event)
    trigger_bucket_name = event['Records'][0]['s3']['bucket']['name']
    folder_path = event['Records'][0]['s3']['object']['key']
    s3_folder = folder_path.split('/')[0]
    s3_sub_folder = folder_path.split('/')[1]
    s3_document_folder = folder_path.split('/')[2]
    trigger_folder = folder_path.split('/')[3]

    s3_client, bucket_name, lambda_write_path, pdf_file_suffix, s3_output_folder = init()

    download_dir(prefix=''.join([s3_folder, "/", s3_sub_folder, "/", s3_document_folder, "/", trigger_folder]),
                 local=lambda_write_path,
                 bucket=bucket_name, client=s3_client)

    process_document_folders(s3_client, bucket_name, s3_folder, s3_sub_folder, s3_document_folder, lambda_write_path,
                             pdf_file_suffix, s3_output_folder, trigger_folder)
    rmtree(lambda_write_path + s3_folder)
    s3_client.delete_object(Bucket=trigger_bucket_name, Key=folder_path)

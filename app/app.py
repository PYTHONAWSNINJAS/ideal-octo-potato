import os
from shutil import copyfile

import boto3
import pandas as pd
import pdfkit
import pytesseract
from fpdf import FPDF
from PIL import Image
from PyPDF2 import PdfFileMerger
from reportlab.graphics import renderPM
from svglib.svglib import svg2rlg
import concurrent.futures
import extract_msg
import shutil
import sqlite3

FILE_PATTERN_TO_IGNORE = '_small'

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


def create_pdf(file_path, lambda_write_path, pdf_file_name, temp_file=False):
    """

    Parameters
    ----------
    file_path: file path of the image
    lambda_write_path: output path of the pdf file
    pdf_file_name: name of the output pdf file
    temp_file: temp file if True, file_path will be deleted

    Returns True if the pdf file is created
    -------

    """

    pdf_png = pytesseract.image_to_pdf_or_hocr(file_path, extension="pdf")
    with open(os.path.join(lambda_write_path, pdf_file_name), "w+b") as f:
        f.write(pdf_png)
    if temp_file:
        print(f"removing temp file {file_path}")
        os.remove(file_path)
    return True


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

    for pdf_file in pdfs:
        os.remove(pdf_file)


def init():
    """

    Initialises variables required for the program to operate
    Returns: all the initialised variables
    -------

    """

    access_key = os.environ["ACCESS_KEY"]
    secret_key = os.environ["SECRET_KEY"]
    bucket_name = os.environ["bucket_name"]
    s3_folder = os.environ["s3_folder"]
    s3_sub_folder = os.environ["s3_sub_folder"]
    s3_document_directory = os.environ["s3_document_directory"]
    lambda_write_path = "/tmp/"
    pdf_file_suffix = os.environ["pdf_file_suffix"]
    s3_output_folder = os.environ["s3_output_folder"]

    session = boto3.Session()
    s3_client = session.client(service_name="s3", endpoint_url="https://s3.amazonaws.com", region_name="us-east-1",
                               aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    return [s3_client, bucket_name, s3_folder, s3_sub_folder, s3_document_directory, lambda_write_path,
            pdf_file_suffix, s3_output_folder]


def get_pdf_object(font_size=10):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=font_size)

    return pdf


def process_document_folders(args):
    s3_client, bucket_name, s3_folder, s3_sub_folder, s3_document_directory, lambda_write_path, pdf_file_suffix, \
        s3_output_folder, item, sub_path, folder = args
    
    for current_file in os.listdir(sub_folder_path := os.path.join(sub_path, folder)):
        converted = False
        file_path = os.path.join(sub_folder_path, current_file)

        if len(file_path.split(".")) == 1:
            continue

        filename, file_extension = os.path.splitext(file_path)
        pdf_file_name = filename + pdf_file_suffix + ".pdf"
        s3_location = os.path.join(s3_folder, s3_sub_folder, item, folder)
        s3_object = pdf_file_name.split(os.sep)[-1]
        
        if filename.endswith(FILE_PATTERN_TO_IGNORE):
            continue
        if file_path.endswith(".pdf"):
            copyfile(file_path, pdf_file_name)
            converted = True
        if file_path.endswith(".txt"):
            pdfkit.from_file(file_path, os.path.join(lambda_write_path, pdf_file_name))
            converted = True
        if file_path.lower().endswith((".png", ".jpg", ".gif", ".tif", ".tiff")):
            converted = create_pdf(file_path, lambda_write_path, pdf_file_name)
        if file_path.endswith((".pcd", ".bmp")):
            Image.open(file_path).save(temp_file := filename + ".png")
            converted = create_pdf(temp_file, lambda_write_path, pdf_file_name, temp_file=True)
        if file_path.endswith(".svg"):
            drawing = svg2rlg(file_path, resolve_entities=True)
            renderPM.drawToFile(drawing, temp_file := filename + ".png", fmt="PNG")
            converted = create_pdf(temp_file, lambda_write_path, pdf_file_name, temp_file=True)
        if file_path.endswith((".html", ".htm", ".xml", ".mht", ".mhtml", ".csv", ".eml")):
            if file_path.endswith("mht"):
                copyfile(file_path, temp_file := filename + ".html")
                pdfkit.from_file(temp_file, os.path.join(lambda_write_path, pdf_file_name),
                                    options={"enable-local-file-access": ""})
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path)
                df.to_html(temp_file := filename + ".html")
                pdfkit.from_file(temp_file, os.path.join(lambda_write_path, pdf_file_name),
                                    options={"enable-local-file-access": ""})
                os.remove(temp_file)
            elif file_path.endswith((".xls", ".xlsx")):
                temp_pdfs = []
                xls = pd.ExcelFile(file_path)
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(file_path, sheet_name=sheet_name)
                    df.to_html(temp_file := filename + "_" + str(sheet_name) + ".html")
                    pdfkit.from_file(temp_file, temp_pdf := filename + "_" + str(sheet_name) + ".pdf",
                                        options={"enable-local-file-access": ""})
                    temp_pdfs.append(temp_pdf)
                    os.remove(temp_file)
                merge_pdf(temp_pdfs, os.path.join(lambda_write_path, pdf_file_name))
            else:
                pdfkit.from_file(file_path, os.path.join(lambda_write_path, pdf_file_name),
                                    options={"enable-local-file-access": ""})
            converted = True
        if file_path.endswith(".msg"):
            try:
                msg_properties = []
                msg = extract_msg.Message(file_path)
                msg_properties.extend([msg.date, '', 'To:'+msg.to, '', msg.subject, msg.body, 'From:'+msg.sender])
                
                pdf_email = get_pdf_object(12)
                for i in msg_properties:
                    pdf_email.write(5, str(i))
                    pdf_email.ln()

                pdf_email.output(os.path.join(lambda_write_path, pdf_file_name))
                converted = True
            except Exception as e:
                print(e)
        if file_path.endswith('.db'):
            try:
                con=sqlite3.connect(file_path)
                df=pd.read_sql_query("select * from <tablename>", con)
                df.to_html(temp_file := filename + ".html")
                pdfkit.from_file(temp_file, os.path.join(lambda_write_path, pdf_file_name))
                os.remove(temp_file)
            except Exception as e:
                print(e)
                os.remove(temp_file)

        if converted:
            print(f"Created - {os.path.join(lambda_write_path, pdf_file_name)}")
            with open(os.path.join(lambda_write_path, pdf_file_name), "rb") as data:
                s3_client.upload_fileobj(data, bucket_name,
                                            s3_location.replace(s3_sub_folder, s3_output_folder) + "/" + s3_object)
        else:
            print(f"PDF not created for - {current_file}")


def lambda_handler(event, context):
    """

    Parameters
    ----------
    event: lambda event
    context: lambda context
    """

    s3_client, bucket_name, s3_folder, s3_sub_folder, s3_document_directory, lambda_write_path, pdf_file_suffix, \
        s3_output_folder = init()

    download_dir(prefix=s3_folder + "/" + s3_sub_folder + "/" + s3_document_directory, local=lambda_write_path,
                 bucket=bucket_name, client=s3_client)
    
    for item in os.listdir(main_path := os.path.abspath(os.path.join(lambda_write_path, s3_folder, s3_sub_folder))):
        folders = []
        for folder in os.listdir(sub_path := os.path.join(main_path, item)):
            stuffs = []
            stuffs.extend([s3_client, bucket_name, s3_folder, s3_sub_folder, s3_document_directory, lambda_write_path, pdf_file_suffix, \
        s3_output_folder, item, sub_path, folder])
            folders.append(stuffs)
    
    with concurrent.futures.ThreadPoolExecutor() as executer:
        results_map = executer.map(process_document_folders, folders)
    
    if os.path.exists(os.path.join(lambda_write_path,s3_folder)):
        shutil.rmtree(os.path.join(lambda_write_path,s3_folder))
        

if __name__ == "__main__":
    import time

    start = time.perf_counter()

    if os.name == "nt":
        pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
        path_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
        config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
    else:
        pytesseract.pytesseract.tesseract_cmd = r"/usr/local/Cellar/tesseract/4.1.1/bin/tesseract"  # mac
        # r"tesseract/4.1.1/bin/tesseract" #linux

    lambda_handler(None, None)

    stop = time.perf_counter()
    print(f"Time Elapsed: {(stop - start)}")
    print("─" * int(os.get_terminal_size().columns))

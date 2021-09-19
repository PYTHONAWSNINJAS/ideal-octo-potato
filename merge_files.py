import json
import boto3
from PyPDF2 import PdfFileMerger
import os

def init():
    """

    Initialises variables required for the program to operate
    Returns: all the initialised variables
    -------

    """

    lambda_write_path = "/tmp/"
    main_s3_bucket = "pythonninjas"
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

def lambda_handler(event, context):
    """

    Parameters
    ----------
    event: lambda event
    context: lambda context
    """
    
    s3_client, bucket_name, lambda_write_path, pdf_file_suffix = init()
    
    s3_clientobj = s3_client.get_object(Bucket=bucket_name, Key='case_number/doc_pdf/control.json')
    data = json.loads(s3_clientobj['Body'].read().decode('utf-8'))
    exhibit_id = data['exhibit_id']
    pdf_file_name = exhibit_id+'.pdf'
    pdfs = []
    for item in data['files']:
        file = item['source'].split('/')[-1]
        filename, file_extension = os.path.splitext(file)
        print(f"downloading {file}")
        s3_client.download_file(bucket_name, item['source'], lambda_write_path+file)
        pdfs.append(lambda_write_path+file)

    merge_pdf(pdfs, lambda_write_path+pdf_file_name)

    print(f"Merged - {os.path.join(lambda_write_path, pdf_file_name)}")
    with open(os.path.join(lambda_write_path, pdf_file_name), "rb") as data:
        s3_client.upload_fileobj(data, bucket_name, 'case_number/doc_pdf/folder1/'+pdf_file_name)
    
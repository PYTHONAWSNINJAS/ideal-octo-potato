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

def process(file_type, exhibit_id, data, s3_client, bucket_name, lambda_write_path, pdf_file_suffix, s3_folder):
    pdf_file_name = file_type+pdf_file_suffix+'.pdf'
    pdfs = []
    for item in data['files']:
        file = item[file_type].split('/')[-1]
        filename, file_extension = os.path.splitext(file)
        print(f"downloading {file}")
        s3_client.download_file(bucket_name, item[file_type], lambda_write_path+file)
        pdfs.append(lambda_write_path+file)

    merge_pdf(pdfs, lambda_write_path+pdf_file_name)

    print(f"Merged - {os.path.join(lambda_write_path, pdf_file_name)}")
    with open(os.path.join(lambda_write_path, pdf_file_name), "rb") as data:
        s3_client.upload_fileobj(data, bucket_name, s3_folder+'/doc_pdf/'+exhibit_id+'/'+pdf_file_name)

def lambda_handler(event, context):
    """

    Parameters
    ----------
    event: lambda event
    context: lambda context
    """ 
    trigger_bucket_name = event['Records'][0]['s3']['bucket']['name']
    control_file = event['Records'][0]['s3']['object']['key']
    s3_folder = control_file.split('/')[0]
    try:
        s3_client, main_s3_bucket, lambda_write_path, pdf_file_suffix = init()
        
        s3_clientobj = s3_client.get_object(Bucket=main_s3_bucket, Key=control_file)
        data = json.loads(s3_clientobj['Body'].read().decode('utf-8'))
        exhibit_id = data['exhibit_id']
        
        for file_type in ['source','current']:
            process(file_type, exhibit_id, data, s3_client, main_s3_bucket, lambda_write_path, pdf_file_suffix, s3_folder)
        
        s3_client.delete_object(Bucket=trigger_bucket_name, Key=control_file)
        return {
                'statusCode': 200,
                'body': "Merged"
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }
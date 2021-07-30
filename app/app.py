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

new_files = []
pdf_files = []
not_converted = []

def download_dir(prefix, local, bucket, client):
    """
    params:
    - prefix: pattern to match in s3
    - local: local path to folder in which to place files
    - bucket: s3 bucket with target contents
    - client: initialized s3 client object
    """
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket':bucket,
        'Prefix':prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        client.download_file(bucket, k, dest_pathname)

def create_pdf(file_path, lambda_write_path, pdf_file_name, temp_file=False):
    try:
        pdf_png = pytesseract.image_to_pdf_or_hocr(file_path, extension='pdf')
        with open(os.path.join(lambda_write_path, pdf_file_name), 'w+b') as f:
            f.write(pdf_png)
        if temp_file:
            print(f"removing temp file {file_path}")
            os.remove(file_path)
        return True    
    except Exception as e:
        print(e)
        return False

def merge_pdf(pdfs, filename):
    merger = PdfFileMerger()
    
    for pdf in pdfs:
        merger.append(pdf)

    merger.write(filename)
    merger.close()
    
    for pdf in pdfs:
        os.remove(pdf)

def lambda_handler(event, context):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font('Arial', size=20)

    session = boto3.Session()
    s3_client = session.client('s3', 'us-east-1')
    bucket_name='pythonninjas'
    s3_folder='case_number'
    s3_sub_folder ='exhibits'
    lambda_write_path = '/tmp/'
    pdf_file_suffix = '_dv'
    # download_dir(prefix=s3_folder, local=lambda_write_path, bucket=bucket_name, client=s3_client)

    for item in os.listdir(main_path := os.path.abspath(os.path.join(lambda_write_path, s3_folder, s3_sub_folder))):
        for folder in os.listdir(sub_path := os.path.join(main_path, item)):
            for file in os.listdir(sub_folder_path := os.path.join(sub_path, folder)):
                Converted = False
                file_path = os.path.join(sub_folder_path, file)
                print(f'\nProcessing file...{file_path}')
                if len(file_path.split('.'))==1:
                    continue
                pdf_file_name = file_path.replace(file_path.split('.')[-1], 'pdf')
                pdf_file_name = pdf_file_name.split('.')[0]+pdf_file_suffix+'.pdf'
                s3_folder = s3_folder + '/' + s3_sub_folder + '/' + item + '/' + folder
                s3_object = pdf_file_name.split(os.sep)[-1]
                new_files.append(file_path)
                try:
                    if file_path.endswith('txt'):
                        pdf.cell(200, 10, txt="".join(open(file_path)))
                        pdf.output(os.path.join(lambda_write_path, pdf_file_name))
                        Converted=True
                    if file_path.lower().endswith(('.png', '.jpg', '.gif', '.tif', '.tiff')):
                        Converted = create_pdf(file_path, lambda_write_path, pdf_file_name)
                    if file_path.endswith(('.pcd', '.bmp')):                       
                        Image.open(file_path).save(temp_file:=file_path.replace(file_path.split('.')[-1], 'png'))
                        Converted = create_pdf(temp_file, lambda_write_path, pdf_file_name, temp_file=True)
                    if file_path.endswith('.svg'):
                        drawing = svg2rlg(file_path,resolve_entities=True)
                        renderPM.drawToFile(drawing, temp_file:=file_path.replace(file_path.split('.')[-1], 'png'), fmt='PNG') 
                        Converted = create_pdf(temp_file, lambda_write_path, pdf_file_name, temp_file=True)
                    if file_path.endswith(('.html','.htm', '.xml', '.mht', '.mhtml', '.csv')):
                        if file_path.endswith('mht'):
                            copyfile(file_path, temp_file:=file_path.replace(file_path.split('.')[-1], 'html'))
                            pdfkit.from_file(temp_file, os.path.join(lambda_write_path, pdf_file_name), options = {'enable-local-file-access': ''})
                        elif file_path.endswith('.csv'):
                            df = pd.read_csv(file_path)
                            df.to_html(temp_file:=file_path.replace(file_path.split('.')[-1], 'html'))
                            pdfkit.from_file(temp_file, os.path.join(lambda_write_path, pdf_file_name), options = {'enable-local-file-access': ''})
                            os.remove(temp_file)
                        elif file_path.endswith(('xlsx', 'xls')):
                            temp_pdfs = []
                            xls = pd.ExcelFile(file_path)
                            for item in xls.sheet_names:
                                df = pd.read_excel(file_path, sheet_name=item)
                                temp_file_name = file_path.split('.')[0]+'_'+str(item)+'.'+file_path.split('.')[1]
                                df.to_html(temp_file:=temp_file_name.replace(temp_file_name.split('.')[1], 'html'))
                                pdfkit.from_file(temp_file, temp_pdf:=temp_file.replace(temp_file.split('.')[1], 'pdf'), options = {'enable-local-file-access': ''})
                                temp_pdfs.append(temp_pdf)
                                os.remove(temp_file)
                            merge_pdf(temp_pdfs, os.path.join(lambda_write_path, pdf_file_name))
                        else:
                            pdfkit.from_file(file_path, os.path.join(lambda_write_path, pdf_file_name), options = {'enable-local-file-access': ''})
                        Converted=True
                
                except Exception as e:
                    if 'Done' in str(e):
                        if file_path.endswith('mht'):
                            print(f"removing temp_file - {temp_file}")
                            os.remove(temp_file)
                        Converted = True
                    else:
                        raise e

                if Converted:
                    print(f"Created - {os.path.join(lambda_write_path, pdf_file_name)}")
                    # with open(os.path.join(lambda_write_path, pdf_file_name), 'rb') as data:
                    #     s3_client.upload_fileobj(data, bucket_name, s3_folder + '/' + s3_object)
                    print(f"Uploaded to - {s3_folder + '/' + s3_object}")
                    pdf_files.append(os.path.join(lambda_write_path, pdf_file_name))
                else:
                    not_converted.append(file_path)
                    print(f"Not Created - {os.path.join(lambda_write_path, pdf_file_name)}")

if __name__ == "__main__":
    if os.name == 'nt':
        pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
    else:
        pytesseract.pytesseract.tesseract_cmd = r'/usr/local/Cellar/tesseract/4.1.1/bin/tesseract' #mac
        #r'tesseract/4.1.1/bin/tesseract' #linux
    
    lambda_handler(None, None)
    print(f"new files - {len(new_files)}\npdf files - {len(pdf_files)}\nnot converted - {len(not_converted)}")

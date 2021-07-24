from fpdf import FPDF
import os
import pytesseract
import boto3
from PIL import Image
from svglib.svglib import svg2rlg
from reportlab.graphics import renderPM
import pdfkit

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

def lambda_handler(event, context):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font('Arial', size=20)

    session = boto3.Session()
    s3_client = session.client('s3')
    bucket_name='filestorageexchange'
    s3_folder='case_number/exhibits'
    lambda_write_path = '/tmp/'
    download_dir(prefix=s3_folder, local=lambda_write_path, bucket=bucket_name, client=s3_client)

    for item in os.listdir(main_path := os.path.abspath(os.path.join(lambda_write_path, 'case_number','exhibits'))):
        for folder in os.listdir(sub_path := os.path.join(main_path, item)):
            for file in os.listdir(sub_folder_path := os.path.join(sub_path, folder)):
                Converted = False
                file_path = os.path.join(sub_folder_path, file)
                print(f'\nProcessing file...{file_path}')
                pdf_file_name = file_path.replace(file_path.split('.')[1], 'pdf')
                s3_folder = 'case_number' + '/' + 'exhibits' + '/' + item + '/' + folder
                s3_object = pdf_file_name.split(os.sep)[-1]
                try:
                    if file_path.endswith('txt'):
                        pdf.cell(200, 10, txt="".join(open(file_path)))
                        pdf.output(os.path.join(lambda_write_path, pdf_file_name))
                        Converted=True
                    if file_path.lower().endswith(('png', 'jpg', 'gif', 'tif', 'tiff')):
                        Converted = create_pdf(file_path, lambda_write_path, pdf_file_name)
                    if file_path.endswith(('pcd', 'bmp')):                       
                        Image.open(file_path).save(temp_file:=file_path.replace(file_path.split('.')[1], 'png'))
                        Converted = create_pdf(temp_file, lambda_write_path, pdf_file_name, temp_file=True)
                    if file_path.endswith('svg'):
                        drawing = svg2rlg(file_path,resolve_entities=True)
                        renderPM.drawToFile(drawing, temp_file:=file_path.replace(file_path.split('.')[1], 'png'), fmt='PNG') 
                        Converted = create_pdf(temp_file, lambda_write_path, pdf_file_name, temp_file=True)
                    if file_path.endswith(('html','htm')):
                        pdfkit.from_file(file_path, file_path.replace(file_path.split('.')[1], 'pdf'))
                        Converted=True
                
                except Exception as e:
                    print(e)

                if Converted:
                    print(f"Created - {os.path.join(lambda_write_path, pdf_file_name)}")
                    with open(os.path.join(lambda_write_path, pdf_file_name), 'rb') as data:
                        s3_client.upload_fileobj(data, bucket_name, s3_folder + '/' + s3_object)
                    print(f"Uploaded to - {s3_folder + '/' + s3_object}")
                else:
                    print(f"Not Created - {os.path.join(lambda_write_path, pdf_file_name)}")

if __name__ == "__main__":
    if os.name == 'nt':
        pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
    else:
        pytesseract.pytesseract.tesseract_cmd = r'tesseract/4.1.1/bin/tesseract'
    
    lambda_handler(None, None)
from fpdf import FPDF
import os
import pytesseract
import boto3
from PIL import Image

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

def lambda_handler(event, context):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font('Arial', size=20)

    session = boto3.Session(profile_name='lambdauser')
    s3_client = session.client('s3')
    bucket_name='filestorageexchange'
    s3_folder='case_number/exhibits'
    path = '.'
    download_dir(prefix=s3_folder, local=path, bucket=bucket_name, client=s3_client)

    for item in os.listdir(main_path := os.path.abspath(os.path.join('case_number','exhibits'))):
        for folder in os.listdir(sub_path := os.path.join(main_path, item)):
            for file in os.listdir(sub_folder_path := os.path.join(sub_path, folder)):
                Converted = False
                file_path = os.path.join(sub_folder_path, file)
                print(f'Processing text file...{file_path}')
                pdf_file_name = file_path.replace(file_path.split('.')[1], 'pdf')
                s3_folder = 'case_number' + '/' + 'exhibits' + '/' + item + '/' + folder
                s3_object = pdf_file_name.split(os.sep)[-1]
                try:
                    if file_path.endswith('txt'):
                        pdf.cell(200, 10, txt="".join(open(file_path)))
                        pdf.output(pdf_file_name)
                        Converted=True
                    if file_path.lower().endswith(('png', 'jpg', 'gif', 'tif')):
                        pdf_png = pytesseract.image_to_pdf_or_hocr(file_path, extension='pdf')
                        with open(pdf_file_name, 'w+b') as f:
                            f.write(pdf_png)
                        Converted=True    
                    if file_path.endswith(('pcd')):                       
                        Image.open(file_path).save(temp_file:=file_path.replace(file_path.split('.')[1], 'png'))
                        pdf_png = pytesseract.image_to_pdf_or_hocr(temp_file, extension='pdf')
                        with open(pdf_file_name, 'pdf', 'w+b') as f:
                            f.write(pdf_png)
                            os.remove(temp_file)
                        Converted=True
                except Exception as e:
                    print(e)

                if Converted:
                    print(f"Created - {pdf_file_name}")
                    with open(pdf_file_name, 'rb') as data:
                        s3_client.upload_fileobj(data, bucket_name, s3_folder + '/' + s3_object)
                    print(f"Uploaded to - {s3_folder + '/' + s3_object}")
                else:
                    print(f"Not Created - {pdf_file_name}")

if __name__ == "__main__":
    pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'#'tesseract/4.1.1/bin/tesseract'
    lambda_handler(None, None)
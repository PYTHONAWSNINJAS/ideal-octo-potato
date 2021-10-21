from PIL import Image, ImageSequence
import os
import traceback
import logging
import sys
import json
import pytesseract
from PyPDF2 import PdfFileMerger

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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


def tiff_to_pdf(file_path, lambda_write_path, pdf_file_name):
    """To convert tiff to pdf
    Args:
        file_path: path to tiff file
    """
    
    image = Image.open(file_path)
    images = []
    pdfs = []
    for i, page in enumerate(ImageSequence.Iterator(image)):
        tmp_image_path = os.path.join(lambda_write_path,"temp_image_"+str(i)+".png")
        tmp_pdf_file_name = tmp_image_path.replace(".png", ".pdf")
        x, y = page.size
        ratio = x/y
        page = page.resize((int(x-x*.15),int(y-y*.15)),Image.ANTIALIAS)
        page.save(tmp_image_path)
        converted = create_pdf(tmp_image_path, lambda_write_path, tmp_pdf_file_name)
        pdfs.append(os.path.join(lambda_write_path, tmp_pdf_file_name))
    if len(images) == 1:
        converted = create_pdf(file_path, lambda_write_path, pdf_file_name)
    else:
        merge_pdf(pdfs, pdf_file_name)
            
if __name__ == "__main__":
    pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
    file_path = r"D:\tmp\case_number\exhibits\folder1\3\J29.tiff"
    lambda_write_path = r"\tmp"
    pdf_file_name = file_path.split(r"\/")[-1].replace("tiff","pdf")
    converted = tiff_to_pdf(file_path, lambda_write_path, pdf_file_name)

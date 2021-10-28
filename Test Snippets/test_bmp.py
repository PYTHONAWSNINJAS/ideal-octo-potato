import os
import pytesseract
from PIL import Image, ImageSequence
import logging
import sys
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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


if __name__ == "__main__":
    pytesseract.pytesseract.tesseract_cmd = (
        r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    )
    file_path = r"D:\tmp\case_number\exhibits\folder1\3\ole2.bmp"
    lambda_write_path = r"\tmp"
    pdf_file_name = "ole2.pdf"
    filename, _ = os.path.splitext(file_path)

    Image.open(file_path).save(temp_file := "".join([filename, ".png"]))
    converted = create_pdf(temp_file, lambda_write_path, pdf_file_name)

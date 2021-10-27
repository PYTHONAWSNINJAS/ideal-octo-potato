from PIL import Image, ImageSequence
import os
import traceback
import logging
import sys
import json
import pytesseract
from PyPDF2 import PdfFileMerger
import concurrent.futures

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


def process_tiff(args):
    lambda_write_path, i, page = args
    tmp_image_path = os.path.join(lambda_write_path, "temp_image_" + str(i) + ".png")
    x, y = page.size
    page = page.resize((int(x - x * 0.25), int(y - y * 0.25)), Image.ANTIALIAS)
    print(page.size)
    page.save(tmp_image_path)

    tmp_pdf_file_name = tmp_image_path.replace(".png", ".pdf")
    _ = create_pdf(tmp_image_path, lambda_write_path, tmp_pdf_file_name)
    print(f"Created: {tmp_pdf_file_name}")
    return os.path.join(lambda_write_path, tmp_pdf_file_name)


def tiff_to_pdf1(file_path, lambda_write_path, pdf_file_name):
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
        image = Image.open(file_path)
        images = []
        pdfs = []
        for i, page in enumerate(ImageSequence.Iterator(image)):
            print(page.page)
            tmp_image_path = os.path.join(
                lambda_write_path, "temp_image_" + str(i) + ".png"
            )
            tmp_pdf_file_name = tmp_image_path.replace(".png", ".pdf")
            x, y = page.size
            page = page.resize((int(x - x * 0.45), int(y - y * 0.45)), Image.ANTIALIAS)
            page.save(tmp_image_path)
            _ = create_pdf(tmp_image_path, lambda_write_path, tmp_pdf_file_name)
            print(f"Created: {tmp_pdf_file_name}")
            pdfs.append(os.path.join(lambda_write_path, tmp_pdf_file_name))
        if len(images) == 1:
            _ = create_pdf(file_path, lambda_write_path, pdf_file_name)
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


def tiff_to_pdf2(file_path, lambda_write_path, pdf_file_name):
    """To convert tiff to pdf
    Args:
        file_path: path to tiff file
    """

    image = Image.open(file_path)
    pdfs = []
    args = []
    for i, page in enumerate(ImageSequence.Iterator(image)):
        stuffs = []
        stuffs.extend([lambda_write_path, i, page.convert("L")])
        args.append(stuffs)

    with concurrent.futures.ThreadPoolExecutor() as executer:
        results = executer.map(process_tiff, args)

        for res in results:
            pdfs.append(res)

    if len(pdfs) == 1:
        _ = create_pdf(file_path, lambda_write_path, pdf_file_name)
    else:
        merge_pdf(pdfs, pdf_file_name)


if __name__ == "__main__":
    pytesseract.pytesseract.tesseract_cmd = (
        r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    )
    file_path = r"D:\tmp\case_number\exhibits\folder1\3\J29.tiff"
    lambda_write_path = r"\tmp"
    pdf_file_name = file_path.split(r"\/")[-1].replace("tiff", "pdf")
    tiff_to_pdf2(file_path, lambda_write_path, pdf_file_name)

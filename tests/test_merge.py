import json
import os

import boto3
from PyPDF2 import PdfFileMerger, PdfFileReader

import traceback
import tempfile

import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def merge_pdf(pdfs, filename, batchsize):
    """

    Parameters
    ----------
    pdfs: pdf files to be merged
    filename: filename of the consolidated file
    """

    pdfs.sort(reverse=False)
    print(f"Number of pdfs to Merge: {str(len(pdfs))}")
    if len(pdfs) < batchsize:
        merger = PdfFileMerger()
        for pdf_file in pdfs:
            merger.append(pdf_file)
        merger.write(filename)
        merger.close()
    else:
        batch_pdfs = []
        list_of_batches = []
        for count, pdf in enumerate(pdfs, 1):
            batch_pdfs.append(pdf)
            if count % batchsize == 0:
                list_of_batches.append(batch_pdfs)
                batch_pdfs = []

            if count > len(pdfs) + 2:
                print("List count larger than number of PDFs. Exiting..")
                os.sys.exit(1)

        list_of_batches.append(batch_pdfs)
        list_of_batches = [x for x in list_of_batches if x]
        print(f"No of batches: {str(len(list_of_batches))}")

        final_pdfs = []
        for i, batchlist in enumerate(list_of_batches):
            print(f"Processing Batch: {str(i)} with length: {str(len(batchlist))}")
            if len(batchlist) > 0:
                merger = PdfFileMerger()
                for pdf in batchlist:
                    try:
                        with open(pdf, "rb") as file:
                            merger.append(PdfFileReader(file))

                    except Exception as e:
                        print(e)
                        print(f"error merging: {pdf}")

                merger.write(filename + str(i) + ".pdf")
                merger.close()
                final_pdfs.append(filename + str(i) + ".pdf")

        print(f"Merging Final {str(len(list_of_batches))} pdf files.")
        merger = PdfFileMerger()
        for pdf_file in final_pdfs:
            merger.append(pdf_file)
        print(f"Creating: {filename}")
        merger.write(filename)
        merger.close()


if __name__ == "__main__":

    pdfs = [r"\tmp\a.pdf", r"\tmp\b.pdf", r"\tmp\c.pdf"]
    lambda_write_path = r"\tmp"
    pdf_file_name = "final.pdf"
    batchsize = 1
    merge_pdf(pdfs, lambda_write_path + "\\" + pdf_file_name, batchsize)

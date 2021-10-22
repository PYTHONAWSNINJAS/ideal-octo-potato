from shutil import copyfile
from itertools import islice
import pdfkit
import os

def process(file_path, filename):
    copyfile(
        file_path, temp_file := "".join([filename, ".txt"])
    )

    with open(temp_file, "r") as myfile:
        head = list(islice(myfile, 1000))

    with open(temp_file, mode="w") as f2:
        for item in head:
            if item.startswith("Content-Disposition: attachment;"):
                break
            f2.write(item)
            
    pdfkit.from_file(
        temp_file,
        os.path.join(lambda_write_path, pdf_file_name),
    )

file_path = r"D:\tmp\case_number\exhibits\folder1\3\email_11.eml"
filename = "email_11"
lambda_write_path=r"\tmp"
pdf_file_name = "email_11_dv.pdf"
process(file_path, filename)
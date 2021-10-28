import pdfkit, os
from shutil import copyfile, rmtree
import sys, traceback, json

if __name__ == "__main__":
    try:
        file_path = (
            r"D:\tmp\case_number\exhibits\folder1\3\Rice_University_Campus_Map.mht"
        )
        lambda_write_path = r"\tmp"
        pdf_file_name = file_path.split(r"\/")[-1].replace("mht", "pdf")
        filename, _ = os.path.splitext(file_path)

        copyfile(file_path, temp_file := "".join([filename, ".html"]))

        pdfkit.from_file(
            temp_file,
            os.path.join(lambda_write_path, pdf_file_name),
            options={"enable-local-file-access": "", "load-error-handling": "ignore"},
        )
    except Exception as e:
        if "Done" not in str(e):
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
            print(err_msg)
        else:
            converted = True

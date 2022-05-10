"""
This module is trigger based on cloudwatch event rule and
checks in rds for any case completion. If numbers are equal,
it removes the entry and places a completed file.
If table is empty, it disables the cloudwatch event rule
"""

import sys
import logging
import pymysql
import boto3
import os
import traceback
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

session = boto3.Session()
s3_client = session.client(service_name="s3")


def count_unprocess_files(bucket, prefix):
    """
    Counts the number of unprocessed files in the given prefix.
    bucket: main bucket name
    prefix: prefix to count files in
    """
    path = []
    logger.info(f"Prefix - {prefix}")

    if s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)["KeyCount"] == 0:
        return 0

    for obj in s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)["Contents"]:
        path.append(obj["Key"])
    logger.info(f"unprocessed path - {path}")
    unprocess_paths = {x.split("/")[3] for x in path}
    return len(unprocess_paths)


def lambda_handler(event, context):
    rds_host = os.environ["db_endpoint"]
    name = os.environ["db_username"]
    password = os.environ["db_password"]
    db_name = os.environ["db_name"]
    main_s3_bucket = os.environ["main_s3_bucket"]

    try:
        conn = pymysql.connect(
            host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=50
        )
        logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

        with conn.cursor() as cur:
            logger.info("Updating unprocessed files count for all cases.")
            cur.execute("select case_id from jobexecution")
            for row in cur:
                case_folder = row[0]
                unprocess_file_count = count_unprocess_files(
                    main_s3_bucket, case_folder + "/doc_pdf/unprocessed_files/"
                )
                cur.execute(
                    "update docviewer.jobexecution set jobexecution.unprocessed_files_from_main\
                    =%s where jobexecution.case_id= %s;",
                    (
                        unprocess_file_count,
                        case_folder,
                    ),
                )
                conn.commit()

        with conn.cursor() as cur:
            logger.info("Checking for completed runs to place completed file in S3.")
            cur.execute(
                "select * from jobexecution where total_control_files=processed_control_files+\
                    unmerged_control_files+unprocessed_files_from_main"
            )
            for row in cur:
                logger.info(f"Found completed runs in rds - {row}")
                case_folder = row[0]

                with conn.cursor() as cur_insert:
                    cur_insert.execute(
                        "insert into jobexecution_history select * from jobexecution \
                                        where jobexecution.case_id=%s;",
                        (case_folder,),
                    )

                logger.info(f"Placed Entry in jobexecution_history for - {case_folder}")
                conn.commit()

                total_control_files = row[1]
                processed_control_files = row[2]
                unmerged_control_files = row[3]
                unprocessed_files_from_main = row[4]

                if total_control_files == processed_control_files:
                    s3_client.put_object(
                        Body="",
                        Bucket=main_s3_bucket,
                        Key=case_folder + "/runs/COMPLETED",
                    )
                    logger.info(
                        f"Placed Completed File for Case Folder - {case_folder}"
                    )

                if unmerged_control_files > 0 or unprocessed_files_from_main > 0:
                    s3_client.put_object(
                        Body="",
                        Bucket=main_s3_bucket,
                        Key=case_folder + "/runs/INCOMPLETE",
                    )
                    logger.info(
                        f"Placed InComplete File for Case Folder - {case_folder}"
                    )

                with conn.cursor() as cur_delete:
                    cur_delete.execute(
                        "delete from jobexecution where case_id = %s;", (case_folder,)
                    )
                logger.info(f"Deleted Entry from jobexecution for - {case_folder}")
                conn.commit()

            logger.info("Checking for empty table to disable cloudwatch.")
            cur.execute("select exists (select 1 from jobexecution);")
            for row in cur:
                if row[0] == 0:
                    client = boto3.client("events")
                    cwRulename = os.environ["cloudwatch_event_name"]
                    _ = client.disable_rule(Name=cwRulename)
                    logger.info(f"Disabled {cwRulename}")

        conn.close()
        return {"statusCode": 200, "body": "Done"}
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
        return {"statusCode": 500, "body": str(traceback.format_exc())}

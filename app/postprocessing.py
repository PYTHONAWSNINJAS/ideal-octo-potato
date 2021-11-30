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


def lambda_handler(event, context):
    rds_host = os.environ["db_endpoint"]
    name = os.environ["db_username"]
    password = os.environ["db_password"]
    db_name = os.environ["db_name"]
    main_s3_bucket = os.environ["main_s3_bucket"]

    try:
        conn = pymysql.connect(
            host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5
        )
        logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

        with conn.cursor() as cur:
            logger.info("Checking for completed runs to place completed file in S3.")
            cur.execute(
                "select * from jobexecution where total_triggers=processed_triggers"
            )
            for row in cur:
                logger.info(f"Found completed entries in rds - {row}")
                case_folder = row[0]
                logger.info(f"Deleting Entry from RDS for - {case_folder}")
                with conn.cursor() as cur_delete:
                    cur_delete.execute(
                        "delete from jobexecution where case_id = %s;", (case_folder,)
                    )
                conn.commit()
                logger.info(f"Placing Completed File for Case Folder - {case_folder}")
                s3_client.put_object(
                    Body="", Bucket=main_s3_bucket, Key=case_folder + "/runs/COMPLETED"
                )

            logger.info("Checking for empty table to disable cloudwatch.")
            cur.execute("select exists (select 1 from jobexecution);")
            for row in cur:
                if row[0] == 0:
                    client = boto3.client("events")
                    cwRulename = os.environ["cloudwatch_event_name"]
                    response = client.disable_rule(Name=cwRulename)
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

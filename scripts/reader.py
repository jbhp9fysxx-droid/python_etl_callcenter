import logging
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

logger=logging.getLogger(__name__)

def fil_reader(src_fil_dir):
    logger.debug("reading source files from local file system")
    try:
        with open(src_fil_dir,'r') as file:
            for record in file:
                yield record
    except FileNotFoundError as ferr:
        logger.error("File not found in s3")



def s3_reader(bucket_name,file_key):
    logger.debug("reading source files from s3")
    try:
        s3_client=boto3.client("s3")
        response=s3_client.get_object(Bucket=bucket_name, Key=file_key)
        for record in response['Body'].iter_lines():
            yield record.decode('utf-8')
    except ClientError as err:
         if err.response['Error']['Code']=="NoSuchKey":
            logger.error("No such file-File not found in s3")
            yield None
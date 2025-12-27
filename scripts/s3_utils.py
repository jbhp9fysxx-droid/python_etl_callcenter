import boto3
import logging

logger=logging.getLogger(__name__)

def upload_to_s3(config):
    storage_type = config["storage"]["type"]

    if storage_type!="s3":
        logger.debug("Skipping S3 upload — storage type is not s3")
        return

    s3=boto3.client("s3")

    local_target=config["storage"]["local"]["target_dir"]+"/"+config["storage"]["local"]["target_filename"]
    local_exception=config["storage"]["local"]["exception_dir"]+"/"+config["storage"]["local"]["exception_filename"]

    s3_bucket=config["storage"]["s3"]["bucket"]
    tgt_file_key=config["storage"]["s3"]["target_key"]
    excp_file_key= config["storage"]["s3"]["exception_key"]

    logger.info("Uploading target file to S3")
    s3.upload_file(local_target,s3_bucket,tgt_file_key)

    logger.info("Uploading exception file to S3")
    s3.upload_file(local_exception,s3_bucket,excp_file_key)




        
import os
from pathlib import Path
import csv
import sys
import boto3

def fil_reader(src_fil_dir):
    with open(src_fil_dir,'r') as file:
        for record in file:
            yield record

def s3_reader(bucket_name,file_key):
    s3_client=boto3.client("s3")
    response=s3_client.get_object(Bucket=bucket_name, Key=file_key)
    
    for record in response['Body'].iter_lines():
            yield record.decode('utf-8')
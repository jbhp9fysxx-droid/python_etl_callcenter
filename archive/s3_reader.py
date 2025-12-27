import os
from pathlib import Path
import csv
import sys
import boto3

def fil_reader(buket_name,file_key):
    s3_client=boto3.client("s3")
    response=s3_client.get_object(Bucket=buket_name, key=file_key)
    
    for record in response['Body'].iter_lines():
            yield record.decode('utf-8')

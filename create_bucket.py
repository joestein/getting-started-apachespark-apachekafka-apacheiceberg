#!/usr/bin/env python3
import boto3
from botocore.exceptions import ClientError
import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    s3 = boto3.resource('s3',
                    endpoint_url='http://localhost:9000',
                    aws_access_key_id='minioadmin',
                    aws_secret_access_key='minioadmin')

    s3.create_bucket(Bucket='miniotesting')
except ClientError as err:
    logger.error(
        "%s: %s",
        err.response["Error"]["Code"],
        err.response["Error"]["Message"],
    )
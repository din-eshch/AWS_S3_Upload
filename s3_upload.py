# Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# This file is licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License. A copy of the
# License is located at
#
# http://aws.amazon.com/apache2.0/
#
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

import boto3
import configparser
import logging
import os
from botocore.exceptions import ClientError

#  Path to config
INI_PATH = 'config.ini' 

config = configparser.ConfigParser()
try:
    fp_ini = config.read(INI_PATH)  
    if not fp_ini:
        logging.info('Config File: ' + INI_PATH +' not found')
        sys.exit(1)
except Exception as e:
    logging.info('Exception caught while reading Config file' + str(e))
    sys.exit(1)

# S3 Access ID
accesskey = config['S3Info']['accesskey']
# S3 secret
secretkey = config['S3Info']['secretkey']
# S3 Bucket name
bucket_name = config['S3Info']['bucket_name'] 
# local file path
base_path = config['LocalInfo']['base_path']


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then same as file_name
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3', aws_access_key_id=accesskey, aws_secret_access_key=secretkey)
    try:
        response = s3_client.upload_file(file_name, bucket, object_name, ExtraArgs={'ACL':'public-read'})
    except ClientError as e:
        logging.error(e)
        return False
    return True


def main():
    """Exercise upload_file()"""

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s: %(message)s')

    # Upload files
    for file_name in os.listdir(base_path):
        up_response = upload_file(base_path+file_name, bucket_name, file_name)
        if up_response:
            logging.info('File: ' + file_name +' was uploaded')

if __name__ == '__main__':
    main()
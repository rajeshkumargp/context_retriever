#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Get from S3 to Local directory

import logging
import os
import sys

import boto3
from botocore.exceptions import ClientError, ProfileNotFound

# ################


# ################

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

c_handler = logging.StreamHandler()
c_handler.setLevel(logging.DEBUG)

c_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
c_handler.setFormatter(c_format)

logger.addHandler(c_handler)

logger.info("Script is getting loaded")


def pull_from_s3(
    src_s3_bucket,
    src_s3_dir_path,
    dest_local_dir,
    overwrite=True,
    profile_name="default",
):

    # Check for profile existence
    try:
        boto3.setup_default_session(profile_name=profile_name)
    except ProfileNotFound as e:
        logger.error("Setup the Profile `aws configure in terminal`")
        print("\n\nSetup the Profile `aws configure in terminal`\n\n")
        logger.exception(e)
        sys.exit(0)

    s3_client = boto3.client("s3")

    # Check for Bucket existence
    if s3_client is not None:
        # Check whether the bucket is available
        bucket_availability = False
        try:
            bucket_availability = s3_client.head_bucket(Bucket=src_s3_bucket)
            bucket_availability = True
            logger.info(f"Bucket={src_s3_bucket} exists")

        except ClientError as ce:
            logger.error(f"Bucket={src_s3_bucket} Not exists")
            logger.exception(ce)

        except Exception as e:
            logger.error("Unexpected Exception")
            logger.exception(e)

        if bucket_availability is True:

            # reference:
            # https://stackoverflow.com/questions/30249069/listing-contents-of-a-bucket-with-boto3

            all_key_files = list()
            # Initial Call
            s3_result = s3_client.list_objects_v2(
                Bucket=src_s3_bucket, Prefix=src_s3_dir_path, MaxKeys=100
            )
            if "Contents" not in s3_result and "CommonPrefixes" not in s3_result:
                all_key_files = list()
            else:
                if s3_result.get("Contents"):
                    for a_key_file in s3_result["Contents"]:
                        all_key_files.append(a_key_file["Key"])

            while s3_result["IsTruncated"]:
                continuation_key = s3_result["NextContinuationToken"]
                s3_result = s3_client.list_objects_v2(
                    Bucket=src_s3_bucket,
                    Prefix=src_s3_dir_path,
                    MaxKeys=100,
                    ContinuationToken=continuation_key,
                )
                if s3_result.get("Contents"):
                    for a_key_file in s3_result["Contents"]:
                        all_key_files.append(a_key_file["Key"])
            pass

            all_key_files = sorted(all_key_files)

            # Create dest dir
            if not os.path.exists(dest_local_dir):
                os.makedirs(dest_local_dir)

            # Retrieving each file
            for afile_index, afile_key in enumerate(all_key_files, 1):
                dest_file = os.path.join(dest_local_dir, afile_key)
                parent_dir = os.path.dirname(dest_file)

                if not os.path.exists(parent_dir):
                    os.makedirs(parent_dir)
                try:
                    if overwrite is True or os.path.exists(dest_file) is False:
                        s3_client.download_file(
                            Bucket=src_s3_bucket, Key=afile_key, Filename=dest_file
                        )
                        logger.info(
                            f"Downloaded {afile_index}/{len(all_key_files)} key={afile_key} local_file={dest_file}"
                        )
                    else:
                        logger.info(
                            f"Skipped {afile_index}/{len(all_key_files)} key={afile_key} local_file={dest_file}"
                        )

                except ClientError as e:
                    logger.error(f"Error while downloading the object={afile_key}")
                    logger.exception(e)
    logger.info("Script ended")


if __name__ == "__main__":
    src_s3_bucket = "ttb-context-retriever-study-materials"
    dest_local_dir = "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/books_dummy"
    pull_from_s3(
        src_s3_bucket=src_s3_bucket,
        src_s3_dir_path="books/class12/vyavasai_adhyan-i",
        dest_local_dir=dest_local_dir,
        overwrite=False,
        profile_name="ttb_con_ret",
    )

    print("Done Uploading Script")

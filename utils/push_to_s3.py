#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Push Local directory to S3
# It may re-upload if a file already exists already.

import glob
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


def push_to_s3(
    source_dir,
    dest_s3_bucket,
    dest_bucket_dir_path="",
    overwrite=False,
    profile_name="default",
):
    if not os.path.exists(source_dir):
        raise Exception("Directory Not Exists")

    all_files_raw_path = glob.glob(
        os.path.join(source_dir, "**", "*.pdf"), recursive=True
    )
    all_files_key_path = [afile.split(source_dir)[1] for afile in all_files_raw_path]
    all_files_key_path = [
        afile[1:] if afile[0] == "/" else afile for afile in all_files_key_path
    ]
    all_files_key_path_norm = [
        afile.lower().replace(" ", "_") for afile in all_files_key_path
    ]

    all_files_key_path_norm_inc_prefix = [afile for afile in all_files_key_path_norm]
    if dest_bucket_dir_path is not None and len(dest_bucket_dir_path) > 0:
        all_files_key_path_norm_inc_prefix = [
            os.path.join(dest_bucket_dir_path, afile)
            for afile in all_files_key_path_norm_inc_prefix
        ]

    all_files_upload_status = [False] * len(all_files_key_path_norm_inc_prefix)

    # ##########################################################
    try:
        boto3.setup_default_session(profile_name=profile_name)
    except ProfileNotFound as e:
        logger.error("Setup the Profile `aws configure in terminal`")
        print("\n\nSetup the Profile `aws configure in terminal`\n\n")
        logger.exception(e)
        sys.exit(0)

    s3_client = boto3.client("s3")
    # ##########################################################

    if s3_client is not None:
        # Check whether the bucket is available
        bucket_availability = False
        try:
            bucket_availability = s3_client.head_bucket(Bucket=dest_s3_bucket)
            bucket_availability = True
            logger.info(f"Bucket={dest_s3_bucket} exists")

        except ClientError as ce:
            logger.error(f"Bucket={dest_s3_bucket} Not exists")
            logger.exception(ce)

        except Exception as e:
            logger.error("Unexpected Exception")
            logger.exception(e)

        if bucket_availability is True:
            all_files_key_path_new = [True] * len(all_files_key_path_norm_inc_prefix)

            if overwrite is False:
                for afile_index, afile_key_path_norm_inc_prefix in enumerate(
                    all_files_key_path_norm_inc_prefix
                ):
                    try:
                        _ = s3_client.head_object(
                            Bucket=dest_s3_bucket, Key=afile_key_path_norm_inc_prefix
                        )
                        all_files_key_path_new[afile_index] = False
                    except ClientError as e:
                        all_files_key_path_new[afile_index] = True
                        logger.warning(e)

                    except Exception as e:
                        logger.error("Unknown Exception occurred")
                        logger.exception(e)

            for (
                afile_index,
                (afile_raw_path, afile_key_path_norm_inc_prefix, is_new),
            ) in enumerate(
                zip(
                    all_files_raw_path,
                    all_files_key_path_norm_inc_prefix,
                    all_files_key_path_new,
                ),
                1,
            ):
                if is_new is True:
                    try:
                        _ = s3_client.upload_file(
                            Filename=afile_raw_path,
                            Bucket=dest_s3_bucket,
                            Key=afile_key_path_norm_inc_prefix,
                        )
                        all_files_upload_status[afile_index - 1] = True
                        logger.info(
                            f"Uploaded {afile_index}/{len(all_files_key_path_norm_inc_prefix)}"
                            + f"file={afile_raw_path} key={afile_key_path_norm_inc_prefix}"
                        )

                    except (ClientError, Exception) as e:
                        logger.error(
                            f"Failure {afile_index}/{len(all_files_key_path_norm_inc_prefix)}"
                            + f"file={afile_raw_path} key={afile_key_path_norm_inc_prefix}"
                        )
                        logger.exception(e)
                else:
                    logger.info(
                        f"Skipping {afile_index}/{len(all_files_key_path_norm_inc_prefix)}"
                        + f"file={afile_raw_path} key={afile_key_path_norm_inc_prefix}"
                    )

    else:
        logger.error("S3 Connection establishing error")


if __name__ == "__main__":
    local_dir = (
        "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/books"
    )
    bucket_name = "ttb-context-retriever-study-materials"
    push_to_s3(
        source_dir=local_dir,
        dest_s3_bucket=bucket_name,
        dest_bucket_dir_path="books",
        profile_name="ttb_con_ret",
    )
    print("Done Uploading Script")

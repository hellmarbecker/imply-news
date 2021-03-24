#!/bin/bash
mkdir -p /data/bedrock-data/m6web_core_dim_catalog_v1/video_folder
aws s3 cp --recursive s3://6cloud-dev-data-m6web-core-data/m6web_core_dim_catalog_v1/video_folder /data/bedrock-data/m6web_core_dim_catalog_v1/video_folder
mkdir -p /data/bedrock-data/m6web_core_dim_catalog_v1/video_program
aws s3 cp --recursive s3://6cloud-dev-data-m6web-core-data/m6web_core_dim_catalog_v1/video_program /data/bedrock-data/m6web_core_dim_catalog_v1/video_program
mkdir -p /data/bedrock-data/m6web_core_dim_catalog_v1/video_publication_history
aws s3 cp --recursive s3://6cloud-dev-data-m6web-core-data/m6web_core_dim_catalog_v1/video_publication_history /data/bedrock-data/m6web_core_dim_catalog_v1/video_publication_history
mkdir -p /data/bedrock-data/m6web_core_dim_catalog_v1/video_tag
aws s3 cp --recursive s3://6cloud-dev-data-m6web-core-data/m6web_core_dim_catalog_v1/video_tag /data/bedrock-data/m6web_core_dim_catalog_v1/video_tag
mkdir -p /data/bedrock-data/m6web_core_dim_catalog_v1/video_video
aws s3 cp --recursive s3://6cloud-dev-data-m6web-core-data/m6web_core_dim_catalog_v1/video_video /data/bedrock-data/m6web_core_dim_catalog_v1/video_video
mkdir -p /data/bedrock-data/m6web_core_fact_beneficiaries_v1/user_view_video
aws s3 cp --recursive s3://6cloud-dev-data-m6web-core-data/m6web_core_fact_beneficiaries_v1/user_view_video /data/bedrock-data/m6web_core_fact_beneficiaries_v1/user_view_video
mkdir -p /data/bedrock-data/m6web_core_dim_users_v1/user_account
aws s3 cp --recursive s3://6cloud-dev-data-m6web-core-data/m6web_core_dim_users_v1/user_account /data/bedrock-data/m6web_core_dim_users_v1/user_account

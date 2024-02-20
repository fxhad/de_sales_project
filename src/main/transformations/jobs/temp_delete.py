import datetime
import logging
import os.path
import shutil
from fileinput import filename

from pyspark.sql.functions import concat_ws, lit, expr
from pyspark.sql.types import StructField, IntegerType, StringType, FloatType, DateType, StructType

from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter
from src.test.sales_data_upload_s3 import s3_client

source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_clie nt, config.bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")

logger.info("#######Deleting sales data from local########")
delete_local_file(config.local_directory)
logger.info("########## Deleted sales data from local########")

logger.info("#######Deleting customer data from local########")
delete_local_file(config.customer_data_mart_local_file)
logger.info("########## Deleted customer data from local########")


logger.info("#######Deleting sales team data from local########")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("########## Deleted customer data from local########")

logger.info("#######Deleting sales team data from local########")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("########## Deleted sales data from local########")
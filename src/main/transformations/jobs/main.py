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

############Get s3 client#################

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()
print(response)
logger.info('List of buckets: %s', response['Buckets'])

# check idf local directory already has a file
# if file is present, then check if the same file is present in staging area
# with status as A. If so then dont delete and try to rerun
# else give an error and process the next file.

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    statement = f"""select distinct file_name from
                sales_data.product_staging_table
                where file_name in ({str(total_csv_files)[1:-1]}) and status = 'A'"""

    # statement = f"select distinct file_name from" \
    #             f"sales_data.product_staging_table " \
    #             f" where file_name in ({str(total_csv_files)[1:-1]}) and status = A"

    logger.info(f"dynamically statement created: {statement} ")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("your last run was failed, please check")
    else:
        logger.info("No records match")

else:
    logger.info("last run was successful")

#####################################################################################################

try:

    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,
                                                 config.bucket_name,
                                                 folder_path=folder_path)
    logger.info("Absolute path on S3 bucket for csv file %s", s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"no files available in {config.s3_source_directory}")
        raise Exception("No file available for processing")


except Exception as e:
    logger.error("Exited with an error: %s", e)
    raise e

bucket_name = config.bucket_name
local_directory = config.local_directory

# s3://fahad-sales-project/sales_data/

# below prefix saying remove a certain part of ulr from s3 url
prefix = f"s3://{bucket_name}/"
file_path = [url[len(prefix):] for url in s3_absolute_file_path]
logging.info("file path is under %s bucket and folder name is %s", bucket_name, file_path)
logging.info(f"file path is under {bucket_name} bucket and folder name is {file_path}")

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_path)
except Exception as e:
    logger.error('File download error: %s', e)
    sys.exit()

# get a list of all the files in our local directory

all_files = os.listdir(local_directory)
logger.info(f"list of all files after download: {all_files}")

# filter files with ".csv" in their names and create absolute paths
if all_files:
    csv_files = []
    error_files = []
    for file_name in all_files:
        if file_name.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, file_name)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, file_name)))
    if not csv_files:
        logger.error('No csv data to process')
        raise Exception('No csv data to process')

else:
    logger.error('No  data to process')
    raise Exception('No  data to process')

########################################################################################################
logger.info('Listing the files that need to be processed %s', csv_files)

logger.info("############Creating Spark Session##############")

spark = spark_session()

logger.info('############Spark session created##################') \
 \
    ##schema validation and to check for any missing columns which will be sent to error_files

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv") \
        .option('header', 'true') \
        .load(data).columns
    logger.info(f"schema for the {data} is {data_schema}")
    logger.info(f"Mandatory columns schema are {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"missing columns: {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing columns for the {data} ")
        correct_files.append(data)

logger.info(f"############List of correct files##############{correct_files}")
logger.info(f"############List of error files##############{error_files}")
logger.info(f"############moving error files to error directory if any#######")

##move data to error directory in local and s3

error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_folder = os.path.join(error_folder_local_path, file_name)

            shutil.move(file_path, destination_folder)
            logger.info(f"Moved '{file_name}'from s3 to '{destination_folder}'")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
            logger.info(f"{message}")
        else:
            logger.error(f"'{file_path}' does not exist")
else:
    logger.info(f"##############There is no error file available#############")

# Additional columns need to be taken care of
# Determine additional columns

# Before running the process
# stage table needs to be updated with status as Active(A) or Inactive(I)

logger.info(f'########Updating the product_staging_table that we have started the process')
insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"INSERT INTO {db_name}.{config.product_staging_table} " \
                     f"(file_name, file_location, created_date, status) " \
                     f"VALUES ('{filename}','{filename}','{formatted_date}','A')"

        insert_statements.append(statements)
    logger.info(f"Insert statements created for staging table ---{insert_statements}")
    logger.info("###########Connecting to my sql server###########")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("#######MYsql server connected successfully############")

    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("###########There is no files to process ##########")
    raise Exception("No dat available with correct files")

logger.info("######Fixing extra column coming from source##########")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_columns", StringType(), True)
])

final_df_to_process = spark.createDataFrame([], schema=schema)

# create a new column with concatenated values for exxtra columns
for data in correct_files:
    data_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
        .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns present at source: {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn("additional_columns", concat_ws(",", *extra_columns)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price",
                    "quantity", "total_cost", "additional_columns")
        logger.info(f"processed {data} and added 'additional_column'")
    else:
        data_df = data_df.withColumn("additional_columns", lit(None)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price",
                    "quantity", "total_cost", "additional_columns")

    final_df_to_process = final_df_to_process.union(data_df)

logger.info(f"###############Final dataframe from source which will be going to processing#################")

final_df_to_process.show()

# Enrich the data from all dimension tables
# also create a datamart for sales_team and their incentive, address and all
# another datamart for customer who bought how much each days of month
# for every month there should be a file inside that
# There should be a store_id regestration
# read the data from parquet and generate a csv file
# in which there wil be a sales_person_name and store-id

# connecting with data base reader
database_client = DatabaseReader(config.url, config.properties)

# creating df for all tables
# customer_table
logger.info("###########Loading customer table into customer_table_df#########################")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)
# product_table
logger.info("#########Loading product table into product_table_df###########")
product_table_df = database_client.create_dataframe(spark, config.product_table)
# product_staging_table
logger.info("#########Loading product table into product_staging_table_df###########")
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)

# sales_team table
logger.info("#########Loading product table into sales_team_table_df###########")
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

# store_table
logger.info("#########Loading product table into store_table_df###########")
store_table_df = database_client.create_dataframe(spark, config.store_table)

s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,
                                                       customer_table_df,
                                                       store_table_df,
                                                       sales_team_table_df)

# final enriched data

logger.info("#########FINAL ENRICHED DATA#########")
s3_customer_store_sales_df_join.show()

# write  the customer data into customer data mart in parquet format
# file will be written in local first
# move the RAW data to s3 bucket for reporting tool
# write reporting dat into mySQL table

logger.info("#######Write the data into customer data mart########")
final_customer_data_mart_df = s3_customer_store_sales_df_join \
    .select("ct.customer_id",
            "ct.first_name", "ct.last_name", "ct.address",
            "ct.pincode", "phone_number",
            "sales_date", "total_cost")

logger.info("########### Final data for customer data mart##############")
final_customer_data_mart_df.show()

parquet_writer = ParquetWriter("overwrite", "parquet")

parquet_writer.dataframe_writer(final_customer_data_mart_df,
                                config.customer_data_mart_local_file)

# moved data to s3 bucket for customer_data_mart
logger.info(f"######customer data written to local disk at {config.customer_data_mart_local_file} ")

s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
logger.info(f"{message}")

# sales_data_mart
logger.info("########### write data into sales team data mart#########")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join \
    .select("store_id", "sales_person_id", "sales_person_first_name",
            "sales_person_last_name", "store_manager_name", "manager_id", "is_manager",
            "sales_person_address", "sales_person_pincode", "sales_date", "total_cost",
            expr("SUBSTRING(sales_date,1,7) as sales_month"))

logger.info("########### Final data for sales team data mart##############")
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,
                                config.sales_team_data_mart_local_file)
logger.info(f"######sales team data written to local disk at {config.sales_team_data_mart_local_file} ")

# moved data to s3 bucket for sales_team_data_mart
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.sales_team_data_mart_local_file)
logger.info(f"{message}")

# # Move files into s3 processed folder and delete the local files
# source_prefix = config.s3_source_directory
# destination_prefix = config.s3_processed_directory
# message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
# logger.info(f"{message}")
#
# logger.info("#######Deleting sales data from local########")
# delete_local_file(config.local_directory)
# logger.info("########## Deleted sales data from local########")
#
# logger.info("#######Deleting customer data from local########")
# delete_local_file(config.customer_data_mart_local_file)
# logger.info("########## Deleted customer data from local########")
#
# logger.info("#######Deleting sales team data from local########")
# delete_local_file(config.sales_team_data_mart_local_file)
# logger.info("########## Deleted customer data from local########")
#
# logger.info("#######Deleting sales team data from local########")
# delete_local_file(config.sales_team_data_mart_partitioned_local_file)
# logger.info("########## Deleted sales data from local########")
#
# update_statement = []
# if correct_files:
#     for file in correct_files:
#         filename = os.path.basename(file)
#         statements = f"UPDATE {db_name}.{config.product_staging_table}" \
#                      f"SET status = 'I', updated_date = '{formatted_date}'" \
#                      f"WHERE file_name = '{filename}'"
#
#         update_statement.append(statements)
#     logger.info("###########Connecting to my sql server###########")
#     connection = get_mysql_connection()
#     cursor = connection.cursor()
#     logger.info("#######MYsql server connected successfully############")
#     for statement in update_statement:
#         cursor.execute(statement)
#         connection.comit()
#     cursor.close()
#     connection.close()
# else:
#     logger.error("######### there is some error in process############")
#     sys.exit()
#
# input("press enter to exit ")

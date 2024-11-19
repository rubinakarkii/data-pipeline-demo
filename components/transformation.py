import json
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

spark = SparkSession.builder \
      .appName("DataPipeline") \
      .getOrCreate()


def load_common_schema():
    with open("components/schema/common.json", 'r') as schema_file:
        schema_dict = json.load(schema_file)
        schema_common = StructType.fromJson(schema_dict)
        logging.info(f"Loading common schema for data transformatio")
        return schema_common


def transform_qualys_data(api_data_qualys):
    schema = load_common_schema()
    logging.info(f"Starting the transformation of qualys data")
    transformed_df_qualys = spark.read.json(spark.sparkContext.parallelize(api_data_qualys), schema=schema)
    logging.info(f"Completed the transformation of qualys data successfully")
    return transformed_df_qualys


def transform_crowdstrike_data(api_data_crowdstrike):
    schema = load_common_schema()
    logging.info(f"Starting the transformation of crowdstrike data ")
    transformed_df_crowdstrike = spark.read.json(spark.sparkContext.parallelize(api_data_crowdstrike), schema=schema)
    logging.info(f"Completed the transformation of crowdstrike data successfully")
    return transformed_df_crowdstrike

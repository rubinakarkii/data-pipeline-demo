import json
import logging
import re

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
    df = spark.sparkContext.parallelize(api_data_qualys)
    transformed_df_qualys = spark.read.json(df, schema=schema)
    logging.info(f"Completed the transformation of qualys data successfully")
    return transformed_df_qualys


def to_camel_case(snake_str):
    """Convert snake_case string to camelCase."""
    if snake_str == '_id':
        return snake_str 
    elif snake_str == 'os_version':
        return "os"
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def convert_keys_to_camel_case(d):
    """Recursively convert dictionary keys to camelCase."""
    if isinstance(d, list):
        return [convert_keys_to_camel_case(item) for item in d]
    elif isinstance(d, dict):
        return {to_camel_case(k): convert_keys_to_camel_case(v) for k, v in d.items()}
    else:
        return d


def transform_crowdstrike_data(api_data_crowdstrike):
    schema = load_common_schema()
    logging.info(f"Starting the transformation of crowdstrike data ")
    data_with_camel_case_keys = convert_keys_to_camel_case(api_data_crowdstrike)
    transformed_df_crowdstrike = spark.read.json(spark.sparkContext.parallelize(data_with_camel_case_keys), schema=schema)
    logging.info(f"Completed the transformation of crowdstrike data successfully")
    return transformed_df_crowdstrike

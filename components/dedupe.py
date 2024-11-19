import logging

def remove_duplicates_from_transformed_data(transformed_data):
    logging.info("Removing duplicates")
    df_deduplicated = transformed_data.dropDuplicates(["_id", "hostname", "osVersion", "agentVersion", "platformName"])
    logging.info("Removed duplicates")
    return df_deduplicated


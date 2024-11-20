import logging

def merge_duplicates_from_transformed_data(transformed_data):
    logging.info("Finding and merging duplicates")
    df_deduplicated = transformed_data.dropDuplicates(["_id", "hostname", "osVersion", "agentVersion", "platformName"])
    logging.info("Merged duplicates")
    return df_deduplicated


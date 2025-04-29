##################################################################################################
#                                         SCRIPT OVERVIEW                                        #
#                                                                                                #
# This script analyzes a JSON file with documents to find duplicate values based on a specified  #
# field. It generates reports listing duplicate groups, IDs to delete, and summary statistics.   #
#                                                                                                #
# Key Features:                                                                                  #
# - Detects duplicates based on any configurable field.                                          #
# - Outputs detailed reports for review and cleanup.                                             #
# - Modular design with error handling and logging.                                              #
##################################################################################################

##################################################################################################
#                                            IMPORTS                                             #
##################################################################################################

import json
import os
from collections import defaultdict, Counter
from utils.logs_config import logger  # Logs and events

##################################################################################################
#                                          CONSTANTS                                             #
##################################################################################################

INPUT_FILE = "data/input_data.json"
OUTPUT_DIR = "dups_analysis"

FIELD_NAME = "url"   # Field to detect duplicates (e.g., "url")
ID_FIELD = "_id"             # Field representing unique document ID

DUPLICATES_FILE = os.path.join(OUTPUT_DIR, "duplicates.json")
DELETE_IDS_FILE = os.path.join(OUTPUT_DIR, "duplicated_ids_to_delete.txt")
STATS_FILE = os.path.join(OUTPUT_DIR, "stats.txt")

##################################################################################################
#                                    LOAD AND VALIDATE DATA                                      #
#                                                                                                #
# Loads the input JSON file and ensures it is a valid list of documents.                         #
##################################################################################################

def load_input_data(input_file):
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("Input JSON must be a list of documents.")
        logger.info(f"✅ Loaded {len(data)} documents from {input_file}.")
        return data
    except Exception as e:
        logger.error(f"❌ Failed to load input data: {e}")
        raise

##################################################################################################
#                                   ANALYZE DUPLICATES                                           #
#                                                                                                #
# Builds an index of documents by the specified field and finds duplicates.                      #
##################################################################################################

def analyze_duplicates(data):
    index = defaultdict(list)
    for doc in data:
        field_value = doc.get(FIELD_NAME)
        if field_value is not None:
            index[field_value].append(doc[ID_FIELD])

    duplicates = {k: v for k, v in index.items() if len(v) > 1}
    delete_ids = [id_ for ids in duplicates.values() for id_ in ids[1:]]

    logger.info(f"🔍 Found {len(duplicates)} duplicate groups.")
    return index, duplicates, delete_ids

##################################################################################################
#                                    WRITE OUTPUT FILES                                          #
#                                                                                                #
# Saves duplicates mapping, IDs to delete, and summary statistics into output files.             #
##################################################################################################

def write_outputs(index, duplicates, delete_ids, total_items):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Duplicates JSON
    with open(DUPLICATES_FILE, "w", encoding="utf-8") as f:
        json.dump(duplicates, f, indent=2)
    logger.info(f"✅ Saved duplicates mapping to {DUPLICATES_FILE}")

    # IDs to delete TXT
    with open(DELETE_IDS_FILE, "w", encoding="utf-8") as f:
        for _id in delete_ids:
            _id_str = _id.get("$oid") if isinstance(_id, dict) and "$oid" in _id else str(_id)
            f.write(f"{_id_str}\n")
    logger.info(f"✅ Saved IDs to delete to {DELETE_IDS_FILE}")

    # Statistics TXT
    total_keys = len(index)
    total_duplicates = len(duplicates)
    total_duplicated_ids = sum(len(v) for v in duplicates.values())
    most_common = Counter({k: len(v) for k, v in duplicates.items()}).most_common(10)

    with open(STATS_FILE, "w", encoding="utf-8") as f:
        f.write(f"Total documents: {total_items}\n")
        f.write(f"Unique {FIELD_NAME} values: {total_keys}\n")
        f.write(f"Duplicated {FIELD_NAME} entries: {total_duplicates}\n")
        f.write(f"Total duplicated IDs: {total_duplicated_ids}\n")
        f.write(f"Total IDs to delete: {len(delete_ids)}\n")
        f.write("Top 10 most duplicated values:\n")
        for val, count in most_common:
            f.write(f"  {val} ({count} times)\n")
    logger.info(f"✅ Saved stats to {STATS_FILE}")

##################################################################################################
#                                        MAIN SCRIPT                                             #
#                                                                                                #
# Coordinates loading, analyzing, and writing results for duplicates detection.                  #
##################################################################################################

if __name__ == "__main__":
    try:
        logger.info("🚀 Starting duplicate analysis...")
        data = load_input_data(INPUT_FILE)
        index, duplicates, delete_ids = analyze_duplicates(data)
        write_outputs(index, duplicates, delete_ids, total_items=len(data))
        logger.info("🏁 Duplicate analysis completed successfully.")
    except Exception as e:
        logger.error(f"❌ Process failed: {e}")

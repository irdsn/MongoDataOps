##################################################################################################
#                                        SCRIPT OVERVIEW                                         #
#                                                                                                #
# This script copies a specific field from documents in one MongoDB collection to another.       #
# The `_id` values are read from a text file to identify which documents to process.             #
# If the field exists in the target document, it is updated; otherwise, it is added.             #
# The script ensures that the order of fields in the target document remains intact.             #
#                                                                                                #
# Configuration Variables:                                                                       #
# - `TXT_FILE_PATH`: Path to the text file containing `_id` values, one per line.                #
# - `SOURCE_COLLECTION`: Source collection containing the field to copy.                         #
# - `TARGET_COLLECTION`: Target collection where the field will be copied.                       #
# - `FIELD_TO_COPY`: The specific field to copy between collections.                             #
# - `BATCH_SIZE`: Number of documents to process per batch.                                      #
# - `MAX_WORKERS`: Number of parallel threads to use for processing.                             #
##################################################################################################

##################################################################################################
#                                            IMPORTS                                             #
##################################################################################################

from utils.database_connections import MongoDBConnection            # Database connection
from utils.logs_config import logger                                # Logs and events
from tqdm import tqdm                                               # Progress bar
from concurrent.futures import ThreadPoolExecutor, as_completed     # Multithreading support
from bson.objectid import ObjectId                                  # MongoDB ObjectId
from os import cpu_count                                            # Optimized MAX_WORKERS num

##################################################################################################
#                                          CONSTANTS                                             #
##################################################################################################

BATCH_SIZE = 500            # Number of documents per batch
MAX_WORKERS = cpu_count()   # Number of parallel threads

SOURCE_DATABASE = "SOURCE_DATABASE"  # Source database name
TARGET_DATABASE = "TARGET_DATABASE"  # Target database name

SOURCE_COLLECTION = "SOURCE_COLLECTION"   # Source collection name
TARGET_COLLECTION = "TARGET_COLLECTION"  # Target collection name

FIELD_TO_COPY = "FIELD_NAME"  # Replace with the field to be copied

TXT_FILE_PATH = "data/ids.txt"  # Path to the text file with _id (Mongo Primary Key) list

##################################################################################################
#                                 READ IDS FROM FILE                                             #
##################################################################################################

def read_ids_from_file(file_path):
    with open(file_path, 'r') as file:
        return [ObjectId(line.strip()) for line in file if line.strip()]

##################################################################################################
#                                COPY FIELD IN PARALLEL                                          #
##################################################################################################

def copy_field_in_parallel(batch, source_conn, target_conn):
    updated_count = 0
    for _id in batch:
        try:
            source_doc = source_conn.collection.find_one({"_id": _id}, {FIELD_TO_COPY: 1})
            if source_doc and FIELD_TO_COPY in source_doc:
                field_value = source_doc[FIELD_TO_COPY]
                result = target_conn.collection.update_one(
                    {"_id": _id},
                    {"$set": {FIELD_TO_COPY: field_value}},
                    #upsert=True  # Ensures insertion if the document does not exist
                )
                if result.modified_count > 0 or result.upserted_id:
                    updated_count += 1
        except Exception as e:
            logger.error(f"Error copying field for document {_id}: {e}")
    return updated_count

##################################################################################################
#                                     MAIN SCRIPT                                                #
##################################################################################################

try:
    # Read IDs from the file
    ids_to_process = read_ids_from_file(TXT_FILE_PATH)
    total_docs = len(ids_to_process)
    logger.debug(f"Total IDs to process: {total_docs}")

    # Create batches of IDs
    batches = [ids_to_process[i:i + BATCH_SIZE] for i in range(0, total_docs, BATCH_SIZE)]

    # Connect to source and target collections
    with MongoDBConnection(database_name=SOURCE_DATABASE, collection_name=SOURCE_COLLECTION) as source_conn, \
            MongoDBConnection(database_name=TARGET_DATABASE, collection_name=TARGET_COLLECTION) as target_conn:

        # Progress bar
        with tqdm(total=total_docs, desc="Copying field") as pbar:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(copy_field_in_parallel, batch, source_conn, target_conn): batch
                    for batch in batches
                }

                # Update progress bar as threads complete
                for future in as_completed(futures):
                    try:
                        updated_count = future.result()
                        if updated_count > 0:
                            pbar.update(updated_count)
                    except Exception as e:
                        logger.error(f"Error in batch {futures[future]}: {e}")

    logger.info("✅ Field copy process completed.")

except Exception as e:
    logger.error(f"❌ Error during processing: {e}")
finally:
    logger.info("✅ Process completed.")

##################################################################################################
#                                        SCRIPT OVERVIEW                                         #
#                                                                                                #
# This script connects to a MongoDB collection and updates a specified field in documents        #
# matching a given query. Updates are performed in batches with multi-threading to optimize      #
# performance. Logging and progress visualization are included for easy monitoring.              #
#                                                                                                #
# Configuration is customizable via constants at the top of the script.                          #
##################################################################################################

##################################################################################################
#                                            IMPORTS                                             #
##################################################################################################

from utils.database_connections import MongoDBConnection            # Database connection
from utils.logs_config import logger                                # Logs and events
from tqdm import tqdm                                               # Progress bar
from concurrent.futures import ThreadPoolExecutor, as_completed     # Multithreading support
from pymongo import UpdateOne                                       # Bulk operation
from datetime import datetime                                       # Timestamp
from os import cpu_count                                            # Optimized MAX_WORKERS num

##################################################################################################
#                                         CONFIGURATION                                          #
##################################################################################################

BATCH_SIZE = 500            # Number of documents per batch
MAX_WORKERS = cpu_count()   # Number of parallel threads

DATABASE_NAME = "DATABASE_NAME" # Source database
COLLECTION_NAME = "COLLECTION_NAME" # Source collection

# Query to find documents with the field to be modified/updated
QUERY = {"FIELD_NAME": "FIELD_VALUE"}

# New value to set
FIELD_TO_UPDATE = "FIELD_NAME"
UPDATED_VALUE = "FIELD_VALUE_NEW"

# Timestamp updated
TIMESTAMP = "timestamp"

##################################################################################################
#                                         PROCESS BATCH                                          #
##################################################################################################

def process_batch(batch_ids, collection):
    try:
        bulk_ops = [
            UpdateOne(
                {"_id": _id},
                {"$set": {
                    FIELD_TO_UPDATE: UPDATED_VALUE,
                    #TIMESTAMP : datetime.utcnow()
                }}
            ) for _id in batch_ids
        ]
        if bulk_ops:
            collection.bulk_write(bulk_ops, ordered=False)
        return len(batch_ids)
    except Exception as e:
        logger.error(f"❌ Error in batch: {e}")
        return 0

##################################################################################################
#                                          UPDATE FIELD                                          #
##################################################################################################

def update_field(collection):
    try:
        cursor = collection.find(QUERY, projection={"_id": 1}).batch_size(BATCH_SIZE)
        ids = [doc["_id"] for doc in cursor]
        total_docs = len(ids)

        logger.info(f"📊 Total documents to update: {total_docs}")
        batches = [ids[i:i + BATCH_SIZE] for i in range(0, total_docs, BATCH_SIZE)]

        with tqdm(total=total_docs, desc=f"Updating '{FIELD_TO_UPDATE}' field") as pbar:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_batch, batch, collection) for batch in batches]
                for future in as_completed(futures):
                    updated_count = future.result()
                    pbar.update(updated_count)

    except Exception as e:
        logger.error(f"❌ Error during update: {e}")

##################################################################################################
#                                        MAIN SCRIPT                                             #
##################################################################################################

if __name__ == "__main__":
    try:
        with MongoDBConnection(database_name=DATABASE_NAME, collection_name=COLLECTION_NAME) as db_conn:
            logger.info("🔗 MongoDB connection opened.")
            update_field(db_conn.collection)
            logger.info("✅ Update process completed successfully.")

    except Exception as exc:
        logger.error(f"❌ Connection or execution error: {exc}")

    finally:
        logger.info("🔄 Process finished.")

##################################################################################################
#                                        SCRIPT OVERVIEW                                         #
#                                                                                                #
# This script processes all documents in a MongoDB collection that match a specific query and    #
# adds or updates a specified field with a default value. Includes a TEST_MODE for testing.      #
#                                                                                                #
# Key Features:                                                                                  #
# - Utilizes batch processing for efficient handling of large datasets.                          #
# - Includes a progress bar for real-time feedback on processing status.                         #
# - Supports dynamic field addition using configurable global variables.                         #
# - Multithreading to improve performance on large datasets.                                     #
#                                                                                                #
# Configuration Variables:                                                                       #
# - `DATABASE_NAME`: The name of the database to connect to.                                     #
# - `COLLECTION_NAME`: The name of the collection containing the documents to process.           #
# - `QUERY`: Defines the MongoDB query to filter the documents to be processed.                  #
# - `FIELD_TO_ADD`: Specifies the field to be added or updated in the documents.                 #
# - `DEFAULT_VALUE`: Default value to set for the field.                                         #
# - `BATCH_SIZE`: Number of documents processed in each batch for efficient memory usage.        #
# - `MAX_WORKERS`: Number of CPU threads used for parallel processing.                           #
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
#                                        CONFIGURATION                                           #
##################################################################################################

BATCH_SIZE = 500            # Number of documents per batch
MAX_WORKERS = cpu_count()   # Number of parallel threads

DATABASE_NAME = "DATABASE_NAME"     # Source database
COLLECTION_NAME = "COLLECTION_NAME" # Source collection

# MongoDB query to select documents
QUERY = {"FIELD_NAME": { "$exists": True }}

# The field to add or update and its default value
FIELD_1 = ("FIELD_NAME_1", [])
FIELD_2 = ("FIELD_NAME_2", "FIELD_VALUE_2")
FIELD_3 = ("timestamp", datetime.utcnow())

##################################################################################################
#                                        IMPLEMENTATION                                          #
##################################################################################################

def process_batch(batch, collection):
    """
    Processes a batch of MongoDB documents by adding or updating predefined fields.

    For each document in the batch, it creates a new version with added or modified fields,
    then performs a bulk replacement in the collection using `$replaceRoot`.

    Args:
        batch (list): List of MongoDB documents to process.
        collection: PyMongo collection object where documents reside.

    Returns:
        int: Number of successfully updated documents in the batch.
    """

    try:
        bulk_ops = []
        for doc in batch:
            new_doc = {}
            for key, value in doc.items():
                new_doc[key] = value
            new_doc[FIELD_1[0]] = FIELD_1[1]
            new_doc[FIELD_2[0]] = FIELD_2[1]
            new_doc[FIELD_3[0]] = FIELD_3[1]
            bulk_ops.append(
                UpdateOne({"_id": doc["_id"]}, {"$replaceRoot": {"newRoot": new_doc}})
            )
        if bulk_ops:
            collection.bulk_write(bulk_ops, ordered=False)
        return len(bulk_ops)
    except Exception as e:
        print(f"❌ Error in batch: {e}")
        return 0

def chunk_cursor(cursor, batch_size):
    """
    Splits a MongoDB cursor into smaller batches for memory-efficient processing.

    Iterates through the cursor and yields sublists of documents, each with a maximum
    size defined by `batch_size`.

    Args:
        cursor: MongoDB cursor object.
        batch_size (int): Number of documents per batch.

    Yields:
        list: A batch (sublist) of documents.
    """

    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

##################################################################################################
#                                               MAIN                                             #
##################################################################################################

if __name__ == "__main__":
    try:
        # Connect to MongoDB target collection
        with MongoDBConnection(database_name=DATABASE_NAME, collection_name=COLLECTION_NAME) as db_conn:
            # Retrieve documents matching the query
            cursor = db_conn.collection.find(QUERY)

            total_docs = db_conn.collection.count_documents(QUERY)
            logger.info(f"Total documents found: {total_docs}")

            # Process documents in batches with multithreading
            with tqdm(total=total_docs, desc="Adding field/s") as pbar:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [
                        executor.submit(process_batch, batch, db_conn.collection)
                        for batch in chunk_cursor(cursor, BATCH_SIZE)
                    ]

                    # Update the progress bar as threads complete
                    for future in as_completed(futures):
                        pbar.update(future.result())

        logger.info("✅ Process completed: Field/s added or updated in all matching documents.")

    except Exception as e:
        logger.error(f"❌ Error during processing: {e}")

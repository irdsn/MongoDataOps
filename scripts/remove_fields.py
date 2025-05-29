##################################################################################################
#                                        SCRIPT OVERVIEW                                         #
#                                                                                                #
# This script processes all documents in a MongoDB collection that match a specific query and    #
# removes a specified field from those documents.                                                #
#                                                                                                #
# Key Features:                                                                                  #
# - Utilizes batch processing for efficient handling of large datasets.                          #
# - Includes a progress bar for real-time feedback on processing status.                         #
# - Supports dynamic field removal using a configurable global variable.                         #
# - Multithreading to improve performance on large datasets.                                     #
#                                                                                                #
# Configuration Variables:                                                                       #
# - `DATABASE_NAME`: The name of the database to connect to.                                     #
# - `COLLECTION_NAME`: The name of the collection containing the documents to process.           #
# - `QUERY`: Defines the MongoDB query to filter the documents to be processed.                  #
# - `FIELDS_TO_REMOVE`: Specifies the fields to be removed from the documents.                   #
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
from os import cpu_count                                            # Optimized MAX_WORKERS num

##################################################################################################
#                                        CONFIGURATION                                           #
##################################################################################################

BATCH_SIZE = 500            # Number of documents per batch
MAX_WORKERS = cpu_count()   # Number of parallel threads

DATABASE_NAME = "DATABASE_NAME" # Source database
COLLECTION_NAME = "COLLECTION_NAME" # Source collection

# MongoDB query to select documents with the specified field
QUERY = {"FIELD_NAME": {"$exists": True}}

# Fields to remove from the documents
FIELDS_TO_REMOVE = ["FIELD_NAME"]
#FIELDS_TO_REMOVE = ["FIELD_NAME_1", "FIELD_NAME_2", "FIELD_NAME_3"]

##################################################################################################
#                                        IMPLEMENTATION                                          #
##################################################################################################

def remove_fields(batch, collection):
    """
    Removes specified fields from a list of documents in a MongoDB collection.

    Executes a bulk update operation that unsets (deletes) the fields defined
    in the global `FIELDS_TO_REMOVE` list for each document in the batch.

    Args:
        batch (list): List of MongoDB documents to process.
        collection: pymongo Collection object where updates will be applied.

    Returns:
        int: Number of documents updated in the current batch.
    """

    try:
        unset_fields = {field: "" for field in FIELDS_TO_REMOVE}
        bulk_ops = [
            UpdateOne({"_id": doc["_id"]}, {"$unset": unset_fields})
            for doc in batch
        ]
        if bulk_ops:
            collection.bulk_write(bulk_ops, ordered=False)
        return len(bulk_ops)
    except Exception as e:
        print(f"❌ Error in batch: {e}")
        return 0

def chunk_cursor(cursor, batch_size):
    """
    Splits a MongoDB cursor into smaller batches for memory-efficient processing.

    Iterates over the cursor and yields lists of documents in fixed-size chunks.

    Args:
        cursor: MongoDB cursor to iterate through.
        batch_size (int): Number of documents per batch.

    Yields:
        list: A batch of MongoDB documents.
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
            cursor = db_conn.collection.find(QUERY, {"_id": 1})  # Only fetch `_id` for efficiency
            total_docs = db_conn.collection.count_documents(QUERY)
            logger.info(f"📋 Total documents found with fields: {', '.join(FIELDS_TO_REMOVE)}")

            # Process documents in batches with multithreading
            with tqdm(total=total_docs, desc=f"Removing fields: {', '.join(FIELDS_TO_REMOVE)}") as pbar:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [
                        executor.submit(remove_fields, batch, db_conn.collection)
                        for batch in chunk_cursor(cursor, BATCH_SIZE)
                    ]

                    # Update the progress bar as threads complete
                    for future in as_completed(futures):
                        pbar.update(BATCH_SIZE)

            logger.info(f"✅ Process completed: `{FIELDS_TO_REMOVE}` removed from all matching documents.")

    except Exception as e:
        logger.error(f"❌ Error during processing: {e}")

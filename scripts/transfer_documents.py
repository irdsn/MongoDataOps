##################################################################################################
#                                        SCRIPT OVERVIEW                                         #
#                                                                                                #
# This script processes documents from one MongoDB collection (SOURCE_COLLECTION) and transfers  #
# them to another collection (target).                                                           #
#                                                                                                #
# Key Features:                                                                                  #
# - Uses batch processing for efficiency.                                                        #
# - Utilizes multithreading to improve performance.                                              #
#                                                                                                #
# Configuration Variables:                                                                       #
# - SOURCE_COLLECTION: Name of the collection to process documents from.                         #
# - TARGET_COLLECTION: Name of the collection to transfer documents to.                          #
# - BATCH_SIZE: Number of documents to process in each batch.                                    #
# - MAX_WORKERS: Number of threads for parallel processing.                                      #
##################################################################################################

##################################################################################################
#                                            IMPORTS                                             #
##################################################################################################

from utils.database_connections import MongoDBConnection            # Database connection
from utils.logs_config import logger                                # Logs and events
from tqdm import tqdm                                               # Progress bar
from concurrent.futures import ThreadPoolExecutor                   # Multithreading support
from pymongo import InsertOne                                       # Bulk operation
from os import cpu_count                                            # Optimized MAX_WORKERS num

##################################################################################################
#                                        CONFIGURATION                                           #
##################################################################################################

BATCH_SIZE = 500            # Number of documents per batch
MAX_WORKERS = cpu_count()   # Number of parallel threads

SOURCE_DATABASE = "SOURCE_DATABASE"         # Source database name
SOURCE_COLLECTION = "SOURCE_COLLECTION"     # Source collection name

TARGET_DATABASE = "TARGET_DATABASE"         # Target database name
TARGET_COLLECTION = "TARGET_COLLECTION"     # Target collection name

# MongoDB query to filter documents
QUERY = {"FIELD_NAME": {"$exists": True}}

MOVE_MODE = False  # If True, documents will be deleted from source after copying (MOVED). If False, they will be preserved (COPIED).

LIMIT = None  # Limit on the number of documents to transfer (None for no limit)

##################################################################################################
#                                        IMPLEMENTATION                                          #
##################################################################################################

def chunk_cursor(cursor, batch_size):
    """
    Splits a MongoDB cursor into smaller, manageable batches for memory-efficient processing.

    Args:
        cursor: MongoDB cursor object pointing to the result set.
        batch_size (int): Number of documents to include in each batch.

    Yields:
        list: A batch (chunk) of documents from the cursor.
    """

    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

'''
def process_batch_upsert(batch, source_conn, target_conn):
    """
    Inserts or updates documents in the target collection based on their `_id`.

    - New documents (not present in the target) are inserted using `insert_many`.
    - Existing documents (same `_id` already in target) are overwritten using `ReplaceOne`.
    - If MOVE_MODE is enabled, inserted documents are deleted from the source collection after transfer.

    This function supports efficient synchronization of inputs between collections.

    Args:
        batch (list): List of MongoDB documents to process.
        source_conn (MongoDBConnection): MongoDB connection to the source collection.
        target_conn (MongoDBConnection): MongoDB connection to the target collection.
    """

    try:
        # Extract _id values from the current batch
        ids_batch = [doc["_id"] for doc in batch]

        # Retrieve existing _id values in the target collection
        existing_ids = set(
            target_conn.collection.distinct("_id", {"_id": {"$in": ids_batch}})
        )

        # Separate documents into new (not in target) and existing (already in target)
        new_documents = [doc for doc in batch if doc["_id"] not in existing_ids]
        existing_documents = [doc for doc in batch if doc["_id"] in existing_ids]

        # Insert new documents using insert_many
        if new_documents:
            target_conn.collection.insert_many(new_documents, ordered=False)
            logger.info(f"Inserted {len(new_documents)} new documents into {TARGET_COLLECTION}")

        # Update existing documents using bulk_write for efficiency
        if existing_documents:
            bulk_updates = [
                ReplaceOne({"_id": doc["_id"]}, doc, upsert=True)
                for doc in existing_documents
            ]
            target_conn.collection.bulk_write(bulk_updates, ordered=False)
            logger.info(f"Updated {len(existing_documents)} documents in {TARGET_COLLECTION}")

        if MOVE_MODE:
                # (MOVE MODE) Delete documents from source collection only if they were inserted at destination
                source_conn.collection.delete_many({"_id": {"$in": [doc["_id"] for doc in new_documents]}})
                logger.info(f"Moved {len(new_documents)} documents from {SOURCE_COLLECTION} to {TARGET_COLLECTION}")
            else:
                # (COPY MODE) Delete documents from source collection
                logger.info(f"Copied {len(new_documents)} documents from {SOURCE_COLLECTION} to {TARGET_COLLECTION}")

    except Exception as e:
        logger.error(f"❌ Failed to process batch: {e}")

'''

def process_batch_insert_missing(batch, source_conn, target_conn):
    """
    Inserts only documents that are not already present in the target collection.

    Ensures no duplicates by checking if each document's `_id` already exists in the target.
    Optionally deletes transferred documents from the source collection if MOVE_MODE is enabled.

    Args:
        batch (list): List of documents to be processed.
        source_conn (MongoDBConnection): Connection to the source MongoDB collection.
        target_conn (MongoDBConnection): Connection to the target MongoDB collection.
    """

    try:
        ids_batch = [doc["_id"] for doc in batch]

        # Get the existing _id in the destination collection
        existing_ids = set(
            target_conn.collection.distinct("_id", {"_id": {"$in": ids_batch}})
        )

        # Filter only documents that are not at destination
        new_documents = [doc for doc in batch if doc["_id"] not in existing_ids]

        if new_documents:
            bulk_inserts = [InsertOne(doc) for doc in new_documents]
            target_conn.collection.bulk_write(bulk_inserts, ordered=False)
            logger.info(f"Inserted {len(new_documents)} new documents into {TARGET_COLLECTION}")

            if MOVE_MODE:
                # (MOVE MODE) Delete documents from source collection only if they were inserted at destination
                source_conn.collection.delete_many({"_id": {"$in": [doc["_id"] for doc in new_documents]}})
                logger.info(f"Moved {len(new_documents)} documents from {SOURCE_COLLECTION} to {TARGET_COLLECTION}")
            else:
                # (COPY MODE) Delete documents from source collection
                logger.info(f"Copied {len(new_documents)} documents from {SOURCE_COLLECTION} to {TARGET_COLLECTION}")

        else:
            logger.info("No new documents to insert in this batch.")

    except Exception as e:
        logger.error(f"❌ Failed to process batch: {e}")


##################################################################################################
#                                               MAIN                                             #
##################################################################################################

try:
    # Connect to MongoDB source collection
    with MongoDBConnection(database_name=SOURCE_DATABASE, collection_name=SOURCE_COLLECTION) as source_conn:
        cursor = source_conn.collection.find(QUERY)

        # Apply limit if specified
        if LIMIT is not None:
            cursor = cursor.limit(LIMIT)

        total_docs = source_conn.collection.count_documents(QUERY)
        logger.info(f"Total documents found: {total_docs}")

        # Create progress bar
        with tqdm(total=total_docs, desc="Processing documents") as pbar:
            # Connect to MongoDB target collection
            with MongoDBConnection(database_name=TARGET_DATABASE, collection_name=TARGET_COLLECTION) as target_conn:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = []
                    batch_sizes = []
                    batch_count = 0  # Processed batch counter

                    for batch in chunk_cursor(cursor, BATCH_SIZE):
                        batch_count += 1
                        logger.info(f"Processing batch {batch_count} with {len(batch)} documents.")

                        future = executor.submit(
                            process_batch_insert_missing,
                            batch,
                            source_conn,
                            target_conn
                        )
                        futures.append((future, len(batch)))

                    for future, batch_len in futures:
                        try:
                            future.result()         # Ensures that there are no exceptions in the threads
                            pbar.update(batch_len)  # Update progress bar
                        except Exception as e:
                            logger.error(f"Error in thread: {e}")

    logger.info(f"✅ Data successfully transferred from {SOURCE_COLLECTION} to {TARGET_COLLECTION}.")

except Exception as e:
    logger.error(f"❌ Error during processing: {e}")

finally:
    logger.info("✅ Process completed and connection closed.")

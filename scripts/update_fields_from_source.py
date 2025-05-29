##################################################################################################
#                                        SCRIPT OVERVIEW                                         #
#                                                                                                #
# This script updates specific fields of documents in a target MongoDB collection                #
# based on matching `_id` values from a source collection.                                       #
#                                                                                                #
# Instead of inserting new documents, it finds existing ones in the target collection            #
# and updates only the specified fields from the source collection.                              #
#                                                                                                #
# Key Features:                                                                                  #
# - Uses batch processing to improve efficiency and reduce memory usage.                         #
# - Matches documents by `_id` between source and target collections.                            #
# - Updates only selected fields while preserving other existing inputs in the target.             #
# - Utilizes multithreading to enhance performance.                                              #
#                                                                                                #
# Configuration Variables:                                                                       #
# - `SOURCE_COLLECTION`: Name of the collection to read documents from.                          #
# - `TARGET_COLLECTION`: Name of the collection where documents will be updated.                 #
# - `BATCH_SIZE`: Number of documents to process in each batch.                                  #
# - `MAX_WORKERS`: Number of threads for parallel processing.                                    #
# - `FIELDS_TO_COPY`: List of specific fields to update in the target collection.                #
##################################################################################################

##################################################################################################
#                                            IMPORTS                                             #
##################################################################################################

from utils.database_connections import MongoDBConnection            # Database connection
from utils.logs_config import logger                                # Logs and events
from tqdm import tqdm                                               # Progress bar
from concurrent.futures import ThreadPoolExecutor, as_completed     # Multithreading support
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

# MongoDB query to select documents
QUERY = {"FIELD_NAME": { "$exists": True }}

# Specific fields to copy
FIELDS_TO_COPY = ["FIELD_NAME_1", "FIELD_NAME_2"]
LIMIT = None  # Limit on the number of documents to transfer (None for no limit)

##################################################################################################
#                                        IMPLEMENTATION                                          #
##################################################################################################

def chunk_cursor(cursor, batch_size):
    """
    Splits a MongoDB cursor into smaller batches for memory-efficient processing.

    Args:
        cursor (pymongo.cursor.Cursor): MongoDB cursor with documents to process.
        batch_size (int): Number of documents per batch.

    Yields:
        list[dict]: A batch (list) of documents.
    """

    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def filter_document_fields(doc):
    """
    Extracts only the specified fields from a document.

    This function filters a MongoDB document and returns a new dictionary containing
    only the keys defined in `FIELDS_TO_COPY`.

    Args:
        doc (dict): The original MongoDB document.

    Returns:
        dict: A filtered dictionary containing only relevant fields.
    """

    return {key: doc[key] for key in FIELDS_TO_COPY if key in doc}

def process_batch_update_matching(batch, source_conn, target_conn):
    """
    Updates existing documents in the target collection by matching `_id` values.

    For each document in the batch that already exists in the target collection,
    this function updates only the fields defined in `FIELDS_TO_COPY`, preserving
    all other existing inputs.

    Args:
        batch (list[dict]): List of source documents to use for updates.
        source_conn (MongoDBConnection): Connection to the source MongoDB collection.
        target_conn (MongoDBConnection): Connection to the target MongoDB collection.
    """

    try:
        # Get the existing _id in the destination collection
        cursor = target_conn.collection.find(
            {"_id": {"$in": [doc["_id"] for doc in batch]}},
            {"_id": 1}
        )
        existing_ids = set(doc["_id"] for doc in cursor)

        # Filter only documents whose _id already exists in the target collection
        matching_documents = [doc for doc in batch if doc["_id"] in existing_ids]

        if matching_documents:
            for doc in matching_documents:
                update_fields = filter_document_fields(doc)
                target_conn.collection.update_one({"_id": doc["_id"]}, {"$set": update_fields})
            logger.info(f"Updated {len(matching_documents)} documents in {TARGET_COLLECTION}")
    except Exception as e:
        logger.error(f"Failed to process batch: {e}")

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
                    batch_count = 0 # Processed batch counter

                    for batch in chunk_cursor(cursor, BATCH_SIZE):
                        batch_count += 1
                        logger.info(f"Processing batch {batch_count} with {len(batch)} documents.")

                        futures.append(
                            executor.submit(
                                process_batch_update_matching,
                                batch,
                                source_conn,
                                target_conn
                            )
                        )

                    for future, batch_len in futures:
                        try:
                            future.result()         # Ensures that there are no exceptions in the threads
                            pbar.update(batch_len)  # Update progress bar
                        except Exception as e:
                            logger.error(f"Error in thread: {e}")

    logger.info(f"✅ Data successfully updated in {TARGET_COLLECTION} from {SOURCE_COLLECTION}.")

except Exception as e:
    logger.error(f"❌ Error during processing: {e}")

finally:
    logger.info("✅ Process completed and connection closed.")

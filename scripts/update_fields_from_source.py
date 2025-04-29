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
# - Updates only selected fields while preserving other existing data in the target.             #
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
#                                          CONSTANTS                                             #
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
#                                        CHUNK CURSOR                                            #
#                                                                                                #
# Efficiently splits a MongoDB cursor into smaller batches to optimize processing.               #
#                                                                                                #
# This function helps to process large datasets by breaking them into manageable chunks.         #
# Instead of loading all documents into memory at once, it yields smaller lists of documents,    #
# reducing memory usage and improving performance.                                               #
#                                                                                                #
# :param cursor: MongoDB cursor to iterate through documents.                                    #
# :param batch_size: The number of documents per batch to process at a time.                     #
# :yield: Yields batches of documents as lists, ensuring efficient memory usage.                 #
##################################################################################################

def chunk_cursor(cursor, batch_size):
    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

##################################################################################################
#                               FILTER DOCUMENT FIELDS                                           #
#                                                                                                #
# Extracts only the specified fields from a MongoDB document.                                    #
#                                                                                                #
# This function ensures that only relevant fields are copied from the source collection.         #
# It does not modify the original document but returns a filtered dictionary containing          #
# only the fields defined in `FIELDS_TO_COPY`.                                                   #
#                                                                                                #
# :param doc: The original MongoDB document from the source collection.                          #
# :return: A new dictionary with only the fields specified in `FIELDS_TO_COPY`.                  #
##################################################################################################

def filter_document_fields(doc):
    return {key: doc[key] for key in FIELDS_TO_COPY if key in doc}

##################################################################################################
#                           PROCESS BATCH - UPDATE MATCHING IDs                                  #
#                                                                                                #
# Updates existing documents in the target collection based on `_id` matches.                    #
#                                                                                                #
# This function scans a batch of documents from the source collection, extracts only the         #
# specified fields, and updates matching documents in the target collection where `_id`          #
# already exists. It ensures that existing documents are updated without modifying other fields. #
#                                                                                                #
# :param batch: A list of documents from the source collection.                                  #
# :param source_conn: MongoDB connection instance for the source collection.                     #
# :param target_conn: MongoDB connection instance for the target collection.                     #
##################################################################################################

def process_batch_update_matching(batch, source_conn, target_conn):
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
#                                        MAIN SCRIPT                                             #
#                                                                                                #
# Orchestrates the process of transferring and updating documents in MongoDB.                    #
#                                                                                                #
# - Establishes connections to the source and target collections.                                #
# - Retrieves documents based on the specified query.                                            #
# - Uses multithreading for parallel processing to improve performance.                          #
# - Processes documents in batches, updating only those with a matching `_id` in the target.     #
# - Logs progress and handles exceptions to ensure reliable execution.                           #
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

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
# - `SOURCE_COLLECTION`: Name of the collection to process documents from.                       #
# - `TARGET_COLLECTION`: Name of the collection to transfer documents to.                        #
# - `BATCH_SIZE`: Number of documents to process in each batch.                                  #
# - `MAX_WORKERS`: Number of threads for parallel processing.                                    #
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

# MongoDB query to filter documents
QUERY = {"FIELD_NAME": {"$exists": True}}

# Fields from the source collection to keep in new documents of the target collection
FIELDS_TO_KEEP = ["FIELD_NAME"]

LIMIT = None  # Limit on the number of documents to create (None for no limit)

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
# :param batch_size: The number of documents per batch to process at a time.                    #
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
# Extracts only fields listed in FIELDS_TO_KEEP from a MongoDB document.                         #
# Ensures the document structure is controlled and reduced.                                      #
#                                                                                                #
# :param doc: MongoDB document from the source collection.                                       #
# :return: A filtered dictionary with only desired fields.                                       #
##################################################################################################

def filter_document_fields(doc):
    filtered_doc = {key: doc[key] for key in FIELDS_TO_KEEP if key in doc}
    return filtered_doc

##################################################################################################
#                        PROCESS BATCH - INSERT MISSING DOCUMENTS                                #
#                                                                                                #
# Inserts only new documents (by `_id`) into the target collection.                              #
# Applies FIELDS_TO_KEEP filtering to control what is inserted.                                  #
# Skips documents already present to avoid overwriting.                                          #
#                                                                                                #
# :param batch: List of MongoDB documents to process.                                            #
# :param source_conn: MongoDBConnection object for the source.                                   #
# :param target_conn: MongoDBConnection object for the target.                                   #
##################################################################################################

def process_batch_insert_missing(batch, source_conn, target_conn):
    try:
        # Get the existing _id in the destination collection
        existing_ids = set(target_conn.collection.distinct("_id", {"_id": {"$in": [doc["_id"] for doc in batch]}}))

        # Filter documents and keep only the necessary fields
        new_documents = [filter_document_fields(doc) for doc in batch if doc["_id"] not in existing_ids]

        if new_documents:
            target_conn.collection.insert_many(new_documents)
            logger.info(f"Inserted {len(new_documents)} new documents into {TARGET_COLLECTION}")

        logger.info(f"Moved {len(new_documents)} documents from {SOURCE_COLLECTION} to {TARGET_COLLECTION}")

    except Exception as e:
        logger.error(f"Failed to process batch: {e}")

##################################################################################################
#                                    MAIN SCRIPT                                                 #
#                                                                                                #
# Connects to MongoDB collections, retrieves documents in batches, and processes them based on   #
# the configuration.                                                                             #
##################################################################################################

try:
    if not FIELDS_TO_KEEP:
        raise ValueError("FIELDS_TO_KEEP must not be empty.")

    # Connect to MongoDB source collection
    with MongoDBConnection(database_name=SOURCE_DATABASE, collection_name=SOURCE_COLLECTION) as source_conn:
        cursor = source_conn.collection.find(QUERY)  # Regular cursor without no_cursor_timeout

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
                    batch_count = 0  # Processed batch counter

                    for batch in chunk_cursor(cursor, BATCH_SIZE):
                        batch_count += 1
                        logger.info(f"Processing batch {batch_count} with {len(batch)} documents.")

                        futures.append(
                            executor.submit(
                                process_batch_insert_missing,
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
                            logger.error(f"❌ Error processing batch: {e}")

    logger.info(f"✅ Data successfully transferred from {SOURCE_COLLECTION} to {TARGET_COLLECTION}.")

except Exception as e:
    logger.error(f"❌ Failed to process: {e}")

finally:
    logger.info("✅ Process completed and connection closed.")

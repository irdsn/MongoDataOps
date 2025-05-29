##################################################################################################
#                                        SCRIPT OVERVIEW                                         #
#                                                                                                #
# This script compares the value of a specified field from documents in a source MongoDB         #
# collection against a target collection (which may be in a different database).                 #
# If a matching value is found in the target collection, the document in the source collection   #
# is marked with a "duplicated": true field.                                                     #
#                                                                                                #
# Key Features:                                                                                  #
# - Compares field values between two collections.                                               #
# - Updates documents in the source collection by adding a "duplicated": true field.             #
# - Uses batch processing for efficiency.                                                        #
# - Implements multithreading to process large datasets faster.                                  #
#                                                                                                #
# Configuration Variables:                                                                       #
# - SOURCE_DATABASE: Database name of the source collection.                                     #
# - TARGET_DATABASE: Database name of the destination collection.                                #
# - SOURCE_COLLECTION: Name of the source collection to mark duplicates in.                      #
# - TARGET_COLLECTION: Name of the collection that holds the reference values.                   #
# - FIELD_TO_COMPARE: The field name whose values will be compared.                              #
# - BATCH_SIZE: Number of documents to process in each batch.                                    #
# - MAX_WORKERS: Number of threads for parallel processing.                                      #
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

FIELD_TO_COMPARE = "FIELD_NAME"             # Field to check for duplicates

##################################################################################################
#                                        IMPLEMENTATION                                          #
##################################################################################################

def load_target_field_values():
    """
    Loads all unique values of the comparison field from the target MongoDB collection.

    Retrieves the field values in batches to avoid memory issues and stores them in a set
    for fast lookup during duplicate detection in the source collection.

    Returns:
        set: A set containing all unique values of the specified field from the target collection.
    """

    target_values = set()
    with MongoDBConnection(database_name=TARGET_DATABASE, collection_name=TARGET_COLLECTION) as target_conn:
        cursor = target_conn.collection.find({}, {FIELD_TO_COMPARE: 1}) # Fetch only the FIELD_TO_COMPARE field

        batch = []
        for doc in cursor:
            if FIELD_TO_COMPARE in doc:
                batch.append(doc[FIELD_TO_COMPARE])

            if len(batch) >= BATCH_SIZE:
                target_values.update(batch)
                batch = []  # Clear batch for next iteration

        if batch:
            target_values.update(batch) # Add remaining documents

    return target_values

def process_duplicates(batch, source_conn, target_values):
    """
    Marks documents in the source collection as duplicated based on field value comparison.

    If a document's comparison field value is found in the target values set,
    it is updated with `"duplicated": true`.

    Args:
        batch (list): List of documents from the source collection.
        source_conn: MongoDBConnection instance for the source collection.
        target_values (set): Set of field values from the target collection.
    """

    for doc in batch:
        if FIELD_TO_COMPARE in doc and doc[FIELD_TO_COMPARE] in target_values:
            source_conn.collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {"duplicated": True}}
            )

def chunk_cursor(cursor, batch_size):
    """
    Splits a MongoDB cursor into smaller batches for memory-efficient processing.

    Iterates over the cursor and yields documents in fixed-size batches.

    Args:
        cursor: MongoDB cursor to iterate over.
        batch_size (int): Number of documents per batch.

    Yields:
        list: A batch of documents.
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
        logger.info(f"🔍 Loading field values from '{TARGET_COLLECTION}' to check for duplicates...")
        target_values = load_target_field_values()
        logger.info(f"✅ Loaded {len(target_values)} unique field values from '{TARGET_COLLECTION}'.")

        # Connect to MongoDB source collection
        with MongoDBConnection(database_name=SOURCE_DATABASE, collection_name=SOURCE_COLLECTION) as source_conn:
            cursor = source_conn.collection.find({}, {"_id": 1, FIELD_TO_COMPARE: 1})   # Fetch only necessary fields
            total_docs = source_conn.collection.count_documents({})
            logger.info(f"📄 Total documents found in '{SOURCE_COLLECTION}': {total_docs}")

            with tqdm(total=total_docs, desc="Checking for duplicates") as pbar:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [
                        executor.submit(process_duplicates, batch, source_conn, target_values)
                        for batch in chunk_cursor(cursor, BATCH_SIZE)
                    ]
                    for future in as_completed(futures):
                        pbar.update(BATCH_SIZE)

        logger.info(f"✅ Duplicate checking process completed successfully for '{SOURCE_COLLECTION}'.")

    except Exception as e:
        logger.error(f"❌ Error during processing: {e}")

    finally:
        logger.info("🔄 Process finished.")

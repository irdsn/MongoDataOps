##################################################################################################
#                                        SCRIPT OVERVIEW                                         #
#                                                                                                #
# This script moves documents from one MongoDB collection to another in parallel using           #
# threading for faster execution. A list of `_id` values is read from a text file to identify    #
# which documents to move. The script processes the documents in batches to optimize memory      #
# usage and ensures safe deletion from the source collection after insertion into the target.    #
#                                                                                                #
# Configuration Variables:                                                                       #
# - `TXT_FILE_PATH`: Path to the text file containing `_id` values, one per line.                #
# - `SOURCE_COLLECTION`: The MongoDB collection from which documents will be moved.              #
# - `TARGET_COLLECTION`: The MongoDB collection to which documents will be moved.                #
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

SOURCE_DATABASE = "SOURCE_DATABASE"         # Source database name
SOURCE_COLLECTION = "SOURCE_COLLECTION"     # Source collection name

TARGET_DATABASE = "TARGET_DATABASE"         # Target database name
TARGET_COLLECTION = "TARGET_COLLECTION"     # Target collection name

MOVE_MODE = False  # If True, documents will be deleted from source after copying (MOVED). If False, they will be preserved (COPIED).

TXT_FILE_PATH = "data/ids.txt"  # Path to the text file with _id (Mongo Primary Key) list

##################################################################################################
#                                 READ IDS FROM FILE                                             #
#                                                                                                #
# Reads a list of ObjectIds from a text file.                                                    #
#                                                                                                #
# :param file_path: Path to the text file.                                                       #
# :return: List of ObjectIds.                                                                    #
##################################################################################################

def read_ids_from_file(file_path):
    with open(file_path, 'r') as file:
        return [ObjectId(line.strip()) for line in file if line.strip()]

##################################################################################################
#                                MOVE DOCUMENTS IN PARALLEL                                      #
#                                                                                                #
# Moves a batch of documents from source to target collection.                                   #
# Deletes the document from the source collection after successful insertion into the target.    #
#                                                                                                #
# :param batch: List of ObjectIds to move.                                                       #
# :param source_conn: MongoDB connection to source collection.                                   #
# :param target_conn: MongoDB connection to target collection.                                   #
##################################################################################################

def move_documents_in_parallel(batch, source_conn, target_conn):
    moved_count = 0
    for _id in batch:
        try:
            document = source_conn.collection.find_one({"_id": _id})
            if document:
                # Insert into the target collection
                target_conn.collection.insert_one(document)

                if MOVE_MODE:
                    # (MOVE MODE) Delete documents from source collection only if they were inserted at destination
                    source_conn.collection.delete_one({"_id": _id})
                    logger.info(f"Moved document {_id} from {SOURCE_COLLECTION} to {TARGET_COLLECTION}")
                else:
                    # (COPY MODE) Delete documents from source collection
                    logger.info(f"Copied document {_id} from {SOURCE_COLLECTION} to {TARGET_COLLECTION}")

                moved_count += 1

        except Exception as e:
            logger.error(f"Error moving document {_id}: {e}")
    return moved_count

##################################################################################################
#                                     MAIN SCRIPT                                                #
#                                                                                                #
# Connects to MongoDB collections, reads a list of IDs, processes them in parallel batches,      #
# and moves documents from the source collection to the target collection.                      #
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
        with tqdm(total=total_docs, desc="Moving documents") as pbar:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(move_documents_in_parallel, batch, source_conn, target_conn): batch
                    for batch in batches
                }

                # Update progress bar as threads complete
                for future in as_completed(futures):
                    try:
                        moved_count = future.result()
                        pbar.update(moved_count)
                    except Exception as e:
                        logger.error(f"Error in batch {futures[future]}: {e}")

    logger.info("✅ Document transfer completed.")

except Exception as e:
    logger.error(f"❌ Error during processing: {e}")
finally:
    logger.info("✅ Process completed.")

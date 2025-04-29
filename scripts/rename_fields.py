##################################################################################################
#                                        SCRIPT OVERVIEW                                         #
#                                                                                                #
# This script processes all documents in a MongoDB collection that match a specific query and    #
# renames specified fields in those documents.                                                   #
#                                                                                                #
# Key Features:                                                                                  #
# - Utilizes batch processing for efficient handling of large datasets.                          #
# - Includes a progress bar for real-time feedback on processing status.                         #
# - Supports dynamic field renaming using a configurable global variable.                        #
# - Multithreading to improve performance on large datasets.                                     #
#                                                                                                #
# Configuration Variables:                                                                       #
# - `DATABASE_NAME`: The name of the database to connect to.                                     #
# - `COLLECTION_NAME`: The name of the collection containing the documents to process.           #
# - `QUERY`: Defines the MongoDB query to filter the documents to be processed.                  #
# - `FIELDS_TO_RENAME`: Specifies the fields to be renamed and their new names.                  #
# - `BATCH_SIZE`: Number of documents processed in each batch for efficient memory usage.        #
# - `MAX_WORKERS`: Number of CPU threads used for parallel processing.                           #
##################################################################################################

##################################################################################################
#                                           IMPORTS                                              #
##################################################################################################

from utils.database_connections import MongoDBConnection            # Database connection
from utils.logs_config import logger                                # Logs and events
from tqdm import tqdm                                               # Progress bar
from concurrent.futures import ThreadPoolExecutor, as_completed     # Multithreading support
from pymongo import UpdateOne                                       # Bulk operation
from os import cpu_count                                            # Optimized MAX_WORKERS num

##################################################################################################
#                                          CONSTANTS                                             #
##################################################################################################

BATCH_SIZE = 500            # Number of documents per batch
MAX_WORKERS = cpu_count()   # Number of parallel threads

DATABASE_NAME = "DATABASE_NAME" # Source database
COLLECTION_NAME = "COLLECTION_NAME" # Source collection

# MongoDB query to select documents with the specified field
QUERY = {"FIELD_NAME": { "$exists": True }}

# Fields to rename in the documents (old_name: new_name)
FIELDS_TO_RENAME = {
    "FIELD_NAME": "FIELD_NAME_NEW",
    #"FIELD_NAME_2": "FIELD_NAME_2_NEW",
    #"FIELD_NAME_3": "FIELD_NAME_3_NEW",
}

##################################################################################################
#                                     RENAME SPECIFIED FIELD                                     #
#                                                                                                #
# Renames the specified fields in all documents in the batch that match the query,               #
# ensuring that THE RENAMED FIELDS REMAIN IN THEIR ORIGINAL POSITION WITHIN THE DOCUMENT.        #
#                                                                                                #
# :param batch: List of documents to process                                                     #
# :param collection: MongoDB collection object                                                   #
##################################################################################################

def rename_fields(batch, collection):
    try:
        bulk_ops = []
        for document in batch:
            updated_document = {}
            for key, value in document.items():
                if key in FIELDS_TO_RENAME:
                    updated_document[FIELDS_TO_RENAME[key]] = value
                else:
                    updated_document[key] = value
            bulk_ops.append(
                UpdateOne({"_id": document["_id"]}, {"$replaceRoot": {"newRoot": updated_document}})
            )
        if bulk_ops:
            collection.bulk_write(bulk_ops, ordered=False)
    except Exception as e:
        print(f"❌ Error in batch: {e}")

##################################################################################################
#                                     RENAME SPECIFIED FIELD                                     #
#                                                                                                #
# Renames the specified fields in all documents in the batch that match the query and MOVES      #
# THE RENAMED FIELDS TO THE END.                                                                 #
##################################################################################################

def rename_fields_move_to_end(batch, collection):
    try:
        bulk_ops = []
        for document in batch:
            updated_document = {}
            renamed_fields = {}
            for key, value in document.items():
                if key in FIELDS_TO_RENAME:
                    renamed_fields[FIELDS_TO_RENAME[key]] = value
                else:
                    updated_document[key] = value
            updated_document.update(renamed_fields)
            bulk_ops.append(
                UpdateOne({"_id": document["_id"]}, {"$replaceRoot": {"newRoot": updated_document}})
            )
        if bulk_ops:
            collection.bulk_write(bulk_ops, ordered=False)
    except Exception as e:
        print(f"❌ Error in batch (move to end): {e}")

##################################################################################################
#                                        CHUNK CURSOR                                            #
#                                                                                                #
# Breaks a MongoDB cursor into smaller batches for efficient processing.                         #
#                                                                                                #
# :param cursor: MongoDB cursor to iterate through documents                                     #
# :param batch_size: The size of each batch to process                                           #
# :yield: Yields batches of documents as lists                                                   #
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
#                                        MAIN SCRIPT                                             #
#                                                                                                #
# Connects to the target MongoDB collection, retrieves documents in batches,                     #
# and renames the specified fields in all matching documents.                                    #
##################################################################################################

if __name__ == "__main__":
    try:
        with MongoDBConnection(database_name=DATABASE_NAME, collection_name=COLLECTION_NAME) as db_conn:
            cursor = db_conn.collection.find(QUERY)
            total_docs = db_conn.collection.count_documents(QUERY)
            logger.info(f"📋 Total documents found with fields to rename: {', '.join(FIELDS_TO_RENAME.keys())}")

            with tqdm(total=total_docs, desc=f"Renaming fields: {', '.join(FIELDS_TO_RENAME.keys())}") as pbar:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [
                        executor.submit(rename_fields, batch, db_conn.collection)
                        for batch in chunk_cursor(cursor, BATCH_SIZE)
                    ]
                    for future in as_completed(futures):
                        pbar.update(BATCH_SIZE)

        logger.info(f"✅ Process completed: `{FIELDS_TO_RENAME}` renamed in all matching documents while preserving order.")

    except Exception as e:
        logger.error(f"❌ Error during processing: {e}")


##################################################################################################
#                                         SCRIPT OVERVIEW                                        #
#                                                                                                #
# This script deletes documents from a MongoDB collection based on a list of `_id` values        #
# provided in a text file.                                                                       #
#                                                                                                #
# Key Features:                                                                                  #
# - Reads `_id` values from a specified text file.                                               #
# - Connects to the MongoDB collection and removes matching documents.                           #
# - Logs the number of documents successfully deleted.                                           #
#                                                                                                #
# Configuration Variables:                                                                       #
# - `TXT_FILE_PATH`: Path to the text file containing `_id` values, one per line.                #
# - `DATABASE_NAME`: MongoDB database containing the target collection.                          #
# - `COLLECTION_NAME`: MongoDB collection from which documents will be deleted.                  #
##################################################################################################

##################################################################################################
#                                            IMPORTS                                             #
##################################################################################################

from utils.database_connections import MongoDBConnection            # Database connection
from utils.logs_config import logger                                # Logs and events
from bson.objectid import ObjectId                                  # MongoDB ObjectId

##################################################################################################
#                                        CONFIGURATION                                           #
##################################################################################################

SOURCE_DATABASE = "SOURCE_DATABASE"     # Source database name
SOURCE_COLLECTION = "SOURCE_COLLECTION" # Source collection name

TXT_FILE_PATH = "inputs/ids.txt"  # Path to the text file with _id (Mongo Primary Key) list

##################################################################################################
#                                        IMPLEMENTATION                                          #
##################################################################################################

def delete_documents_by_ids(file_path, db_name, collection_name):
    """
    Deletes documents from a MongoDB collection based on `_id` values listed in a text file.

    Each line in the file must contain a valid MongoDB ObjectId. All matching documents
    in the specified collection are deleted in bulk using `$in`.

    Args:
        file_path (str): Path to the text file containing one `_id` per line.
        db_name (str): Name of the MongoDB database.
        collection_name (str): Name of the collection from which documents will be deleted.
    """

    try:
        # Read `_id` values from the file
        with open(file_path, 'r') as file:
            ids_to_delete = [ObjectId(line.strip()) for line in file if line.strip()]
        logger.info(f"📋 Total IDs loaded for deletion: {len(ids_to_delete)}")

        # Connect to MongoDB and delete matching documents
        with MongoDBConnection(database_name=db_name, collection_name=collection_name) as conn:
            result = conn.collection.delete_many({"_id": {"$in": ids_to_delete}})
            logger.info(f"✅ Total documents deleted from '{collection_name}': {result.deleted_count}")

    except Exception as e:
        logger.error(f"❌ Error during deletion: {e}")

##################################################################################################
#                                               MAIN                                             #
##################################################################################################

if __name__ == "__main__":
    delete_documents_by_ids(TXT_FILE_PATH, SOURCE_DATABASE, SOURCE_COLLECTION)

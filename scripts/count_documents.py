##################################################################################################
#                                        SCRIPT OVERVIEW                                         #
#                                                                                                #
# This script connects to a MongoDB collection and counts documents that match a custom query.   #
# It is optimized for large collections by using a projection-based approach instead of the      #
# native `count_documents()` method, which can cause timeouts on very large datasets.            #
#                                                                                                #
# Key Features:                                                                                  #
# - Uses a streaming cursor with `_id` projection for better performance on large datasets.      #
# - Only performs a read operation; no inputs is modified.                                         #
# - Logs the total number of matching documents.                                                 #
# - Accepts a customizable MongoDB query.                                                        #
##################################################################################################

##################################################################################################
#                                            IMPORTS                                             #
##################################################################################################

from utils.database_connections import MongoDBConnection            # Database connection
from utils.logs_config import logger                                # Logs and events

##################################################################################################
#                                        CONFIGURATION                                           #
##################################################################################################

DATABASE_NAME = "DATABASE_NAME"     # Source database
COLLECTION_NAME = "COLLECTION_NAME" # Source collection

QUERY = {"FIELD_NAME": { "$exists": True }} # Query to filter documents

##################################################################################################
#                                        IMPLEMENTATION                                          #
##################################################################################################

def count_documents():
    """
    Counts the number of documents in a MongoDB collection that match a custom query.

    Uses a projection-based streaming cursor to efficiently count documents,
    especially in large collections where `count_documents()` may time out.

    Features:
        - Uses `_id`-only projection for performance.
        - Prints the total count of matching documents to the logs.
        - Does not modify any inputs; read-only operation.

    Raises:
        Logs exceptions if a connection or query error occurs.
    """

    try:
        # Connect to MongoDB collection
        with MongoDBConnection(database_name=DATABASE_NAME, collection_name=COLLECTION_NAME) as conn:
            logger.info(f"🔗 Connected to MongoDB | DATABASE: {DATABASE_NAME} | COLLECTION: {COLLECTION_NAME}")
            logger.info(f"🔗 QUERY: {QUERY}")

            # Count total matching documents
            #total_docs = conn.collection.count_documents(QUERY)

            cursor = conn.collection.find(QUERY, projection={"_id": 1}).batch_size(10000)
            total_docs = sum(1 for _ in cursor)

            logger.info(f"📊 Total documents matching query: {total_docs}")

        logger.info("✅ Counting process completed successfully.")

    except Exception as e:
        logger.error(f"❌ Error counting documents: {e}")

    finally:
        logger.info("🔄 Process finished.")


##################################################################################################
#                                               MAIN                                             #
##################################################################################################

if __name__ == "__main__":
    count_documents()

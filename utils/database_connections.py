##################################################################################################
#                                        SCRIPT OVERVIEW                                         #
#                                                                                                #
# This module handles the MongoDB database connection setup and provides helper methods          #
# for common operations such as finding and updating documents. It uses a context manager        #
# pattern to ensure proper opening and closing of connections.                                   #
# Configuration parameters are loaded securely from environment variables using dotenv.          #
##################################################################################################

##################################################################################################
#                                            IMPORTS                                             #
##################################################################################################

import os
import time

from dotenv import load_dotenv
from pymongo import MongoClient         # MongoDB
import urllib.parse                     # MongoDB
from utils.logs_config import logger    # Logs and events

##################################################################################################
#                                        CONFIGURATION                                           #
##################################################################################################

load_dotenv()  # Load environment variables from .env

# MongoDB Settings
MONGO_USER = os.getenv("MONGO_USER", "default_user")
MONGO_PASS = os.getenv("MONGO_PASS", "default_pass")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")

ESCAPED_USR = urllib.parse.quote_plus(MONGO_USER)
ESCAPED_PWD = urllib.parse.quote_plus(MONGO_PASS)

MONGO_URI = f"mongodb://{ESCAPED_USR}:{ESCAPED_PWD}@{MONGO_HOST}:{MONGO_PORT}/"

##################################################################################################
#                                        IMPLEMENTATION                                          #
##################################################################################################

class MongoDBConnection:
    """
    Context-managed MongoDB connection handler.

    This class establishes a connection to a MongoDB instance using parameters loaded from
    environment variables. It supports connection pooling, timeouts, and retry logic.

    Attributes:
        client (MongoClient): PyMongo client instance.
        database (Database): Reference to the target MongoDB database.
        collection (Collection): Reference to the target MongoDB collection.
    """

    def __init__(self, database_name, collection_name):
        self.uri = MONGO_URI
        self.client = MongoClient(
            self.uri,
            # serverSelectionTimeoutMS=30000,  # Timeout when connecting to the server (30 seconds)
            connectTimeoutMS=60000,
            socketTimeoutMS=120000,  # Socket operation timeout time
            maxPoolSize=50,  # Maximum connection pool size
            retryWrites=True  # Allows automatic retry of writes
        )

        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
        logger.info("MongoDB connection initialized.")

    def __enter__(self):
        logger.info("MongoDB connection opened.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()
        logger.info("MongoDB connection closed.")

    def find_documents(self, filter_query, projection=None, limit_size=None):
        """
        Retrieves documents from the MongoDB collection based on a filter.

        Args:
            filter_query (dict): MongoDB filter query to match documents.
            projection (dict, optional): Dictionary specifying fields to include or exclude.
            limit_size (int, optional): Limits the number of returned documents.

        Returns:
            list[dict]: List of documents matching the filter.
        """

        try:
            cursor = self.collection.find(filter_query, projection)
            # Applies the limit only if `limit` has an integer value
            if limit_size is not None:
                cursor = cursor.limit(limit_size)
            documents = list(cursor)

            logger.info(f"Retrieved {len(documents)} documents for processing.")
            return documents
        except Exception as e:
            logger.error(f"Error fetching documents from MongoDB: {e}")
            if "server selection timeout" in str(e).lower():
                logger.error("MongoDB server connection failed. Retrying...")
            return []

    def update_document(self, filter_query, update_values, retries=3, delay=5):
        """
        Updates a single document in the MongoDB collection with retry logic.

        Args:
            filter_query (dict): Query to match the document to update.
            update_values (dict): Dictionary of fields to update (`$set`).
            retries (int): Number of retry attempts on failure.
            delay (int): Seconds to wait between retry attempts.

        Returns:
            UpdateResult | None: The result of the update operation, or None if all retries fail.
        """

        for attempt in range(retries):
            try:
                # Performs update using `$set` to create or update the field
                result = self.collection.update_one(filter_query, {'$set': update_values})
                if result.matched_count:
                    logger.info("Document updated successfully.")
                    return result
                else:
                    logger.warning("No document found to update.")
                return result
            except Exception as e:
                logger.error(f"Error updating document in MongoDB: {e}")
                if attempt < retries - 1:  # Do not sleep at the last attempt
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)  # Wait 'delay' seconds before retrying
                else:
                    logger.error("Final retry failed. Skipping this document.")
                    return None # If after 'retries' attempts it still fails, ignore it
# MongoDataOps: A clean and modular toolkit for MongoDB data workflows

![Python](https://img.shields.io/badge/Python-3.11-blue.svg)
![Last Updated](https://img.shields.io/badge/Last%20Updated-May%202025-brightgreen)


A MongoDB data operations toolkit, born from real-world experience as a Data Engineering Lead & Artificial Intelligence Engineer.

---

## Author

Íñigo Rodríguez Sánchez  
Data & Artificial Intelligence Engineer

---

## Table of Contents

- [Introduction](#introduction)
- [Key Features](#key-features)
- [Project Structure](#project-structure)
- [Script Overview](#script-overview)
- [Installation](#installation)
- [Usage](#usage)
- [Final Words](#final-words)

---

## Introduction

**MongoDataOps** is a collection of Python scripts designed to automate and streamline frequent data management tasks in MongoDB collections.  

This repository is the result of valuable knowledge and practices acquired during my professional journey. It originated from a position I greatly enjoyed, where I grew extensively both technically and personally.  

The purpose of this repository is to preserve and share that learning, ensuring that the lessons and best practices are never lost.

MongoDataOps aims to provide:
- Efficient, production-grade MongoDB scripts
- Consistency and clarity in database maintenance
- Robust, parallelized, and scalable operations

---

## Key Features

- **Batch processing & multithreading**: All scripts are optimized for speed and scalability using batch-based logic and parallel processing.
- **Safe and controlled operations**: Scripts support precise control over field updates, document transfers, deletions, and renaming — with options to preview or limit scope.
- **Duplicate detection & cleansing**: Includes tools to analyze and flag duplicate documents either from local JSON files or between MongoDB collections.
- **Highly modular and configurable**: Configuration is managed through constants in each script, allowing easy adaptation to different datasets or environments.
- **Robust MongoDB integration**: Built-in connection handling via a reusable `MongoDBConnection` class, with support for retries, timeouts, and environment-based credentials.
- **Specialized utilities**:
  - `count_documents.py` uses an optimized projection-based method to handle large collections without timeout issues.
  - `count_duplicated.py` works on local JSON files, offering offline analysis of duplicates.

> ⚠️ **Caution**: Some operations are **destructive** (e.g., deleting or moving documents). Always validate queries and test with small samples before full execution.

---

## Project Structure

```bash
MongoDataOps/
├── inputs/                                  # Input inputs files for processing
│   ├── ids.txt                              # List of MongoDB _id values
│   └── input_data.json                      # Sample documents for analysis
│
├── dups_analysis/                           # Output folder for duplicate analysis results
│   ├── duplicated_ids_to_delete.txt         # IDs marked for deletion
│   ├── duplicates.json                      # Grouped duplicate records
│   └── stats.txt                            # Summary statistics report
│
├── scripts/                                 # Automation and inputs operation scripts
│   ├── add_fields.py                        # Adds or updates fields in documents
│   ├── copy_field_by_ids.py                 # Copies a field between collections using a list of _id
│   ├── count_documents.py                   # Efficiently counts documents matching a query
│   ├── count_duplicated.py                  # Detects duplicates in a JSON file based on a field
│   ├── create_docs_with_selected_fields.py  # Creates documents using selected fields only
│   ├── delete_documents_by_ids.py           # Deletes documents from a collection by _id list
│   ├── mark_duplicates_by_field.py          # Flags documents as duplicated based on field comparison
│   ├── remove_fields.py                     # Removes specified fields from documents
│   ├── rename_fields.py                     # Renames fields in MongoDB documents
│   ├── transfer_documents.py                # Transfers documents between collections (by query)
│   ├── transfer_documents_by_ids.py         # Transfers documents between collections (by _id list)
│   ├── update_field.py                      # Updates a single field value in matched documents
│   └── update_fields_from_source.py         # Updates selected fields in target collection using source
│
├── utils/                                   # Utility modules
│   ├── database_connections.py              # Context-managed MongoDB connector
│   └── logs_config.py                       # Centralized color-coded logger setup
│
├── requirements.txt                         # Python dependencies list
├── .env.example                             # Template for MongoDB credentials
├── .gitignore                               # Files and folders to ignore in Git
└── README.md                                # Project documentation

```

---

## Script Overview

| Script                                        | Purpose                                                                                                   |
|:----------------------------------------------|:----------------------------------------------------------------------------------------------------------|
| `scripts/add_fields.py`                       | Add or update predefined fields with default values in documents matching a query.                        |
| `scripts/copy_field_by_ids.py`                | Copy a specific field from a source collection to a target collection using `_id` values from a file.     |
| `scripts/count_documents.py`                  | Efficiently count documents that match a query using projection-only cursor to avoid timeouts.            |
| `scripts/count_duplicated.py`                 | Detect duplicate values in a local JSON file based on a configurable field and generate deletion reports. |
| `scripts/create_docs_with_selected_fields.py` | Transfer only selected fields from documents in one collection to another, skipping existing ones.        |
| `scripts/delete_documents_by_ids.py`          | Permanently delete documents from a MongoDB collection based on `_id` values provided in a text file.     |
| `scripts/mark_duplicates_by_field.py`         | Flag documents in a source collection as duplicates if a field matches values in a target collection.     |
| `scripts/remove_fields.py`                    | Remove one or more specified fields from all documents that match a given query.                          |
| `scripts/rename_fields.py`                    | Rename fields in documents, preserving the original order or moving renamed fields to the end.            |
| `scripts/transfer_documents.py`               | Copy or move documents between collections based on a query, with batch processing and parallelism.       |
| `scripts/transfer_documents_by_ids.py`        | Copy or move documents from one collection to another using a list of `_id` values from a text file.      |
| `scripts/update_field.py`                     | Update a specific field (and optionally a timestamp) for all documents that match a condition.            |
| `scripts/update_fields_from_source.py`        | Update selected fields in a target collection using matching `_id` documents from a source collection.    |
| `utils/database_connections.py`               | Context-managed MongoDB connection handler. Includes helpers for querying and updating with retry logic.  |
| `utils/logs_config.py`                        | Centralized logging configuration with color-coded console output for INFO, DEBUG, and ERROR levels.      |

> **Note:** Scripts use controlled multithreading, batch sizes, and robust MongoDB connection management.

---

## Installation

1. Clone this repository:
```bash
git clone https://github.com/YOUR_USERNAME/MongoDataOps.git
cd MongoDataOps
```

2. (Optional but recommended) Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install required Python packages:
```bash
pip install -r requirements.txt
```

---

## Usage

Each script is standalone and configurable via the constants section at the top of the script.

Example:
```bash
python scripts/add_fields.py
```

Before running a script:
- Review and configure `DATABASE`, `COLLECTION`, `QUERY`, and other parameters.
- Place input files (e.g., `ids.txt`, `input_data.json`) in the `/data` directory.

> Scripts automatically handle logs and progress reporting.

---

## Final Words

MongoDataOps isn't just a set of scripts. It's a toolkit born from real data challenges.  
Thank you for checking it out! Hope it saves you time, simplifies your workflows, and sparks new ideas.  
It is an evolving tool, open for experimentation, extension, or integration into larger pipelines.

Feel free to explore, extend, or integrate it into your own applications. Contributions, feedback, or improvements are always welcome.

**If you’ve found this project useful or inspiring — feel free to build on it, break it, or just drop a star ⭐.**
# MongoDataOps

![Python](https://img.shields.io/badge/Python-3.11-blue.svg)
![Last Updated](https://img.shields.io/badge/Last%20Updated-April%2029,%202025-green)

A MongoDB data operations toolkit, born from real-world experience as a Data Engineering Lead & Artificial Intelligence Engineer.

---

## Author

Íñigo Rodríguez Sánchez  
Data Engineering Lead & Artificial Intelligence Engineer

---

## Table of Contents

- [Introduction](#introduction)
- [Key Features](#key-features)
- [Project Structure](#project-structure)
- [Script Overview](#script-overview)
- [Utilities](#utilities)
- [Installation](#installation)
- [Usage](#usage)
- [Important Notes](#important-notes)
- [Final Words](#final-words)

---

## Introduction

**MongoDataOps** is a collection of Python scripts designed to automate and streamline frequent data management tasks in MongoDB collections.  

This repository is the result of valuable knowledge and practices acquired during my professional journey.  
It originated from a position I greatly enjoyed, where I grew extensively both technically and personally.  
The purpose of this repository is to preserve and share that learning, ensuring that the lessons and best practices are never lost.

MongoDataOps aims to provide:
- Efficient, production-grade MongoDB scripts
- Consistency and clarity in database maintenance
- Robust, parallelized, and scalable operations

---

## Key Features

- Batch processing and multithreading for speed and scalability
- Controlled document updates, moves, and deletions
- Duplicate detection and data cleansing utilities
- Highly modular and configurable scripts
- Structured, reusable MongoDB connection and logging utilities

---

## Project Structure

```bash
MongoDataOps/
├── data/                      # Input data files (e.g., ids.txt, input_data.json)
├── dups_analysis/             # Output folder for duplicate analysis results
├── scripts/                   # Main Python scripts (automation tasks)
├── utils/                     # Reusable utilities (database connections, logging)
├── requirements.txt           # Python dependencies list
├── .env.example               # Template for environment variables
├── .gitignore                 # Files and folders to ignore in Git
└── README.md                  # This documentation file (you are here)
```

---

## Script Overview

| Script | Purpose |
|:---|:---|
| `add_fields.py` | Add new fields with default values to existing documents. |
| `copy_field_by_ids.py` | Copy a single field based on a list of `_id` values. |
| `count_documents.py` | Count documents matching a query, optimized for large collections. |
| `count_duplicated.py` | Analyze a JSON file for duplicate field values and generate deletion plans. |
| `create_docs_with_selected_fields.py` | Create new documents keeping only selected fields from source. |
| `delete_documents_by_ids.py` | Delete documents from a collection based on `_id` list from a file. |
| `mark_duplicates.py` | Mark documents as duplicates based on field comparison between collections. |
| `remove_fields.py` | Remove one or more fields from existing documents. |
| `rename_fields.py` | Rename fields in all documents of a collection. |
| `transfer_documents.py` | Transfer (copy or move) documents between collections based on a query. |
| `transfer_documents_by_ids.py` | Transfer (copy or move) documents between collections based on a list of `_id` values. |
| `update_field.py` | Update the value of a specific field in documents matching a condition. |
| `update_fields_from_source.py` | Update specific fields from a source to a target collection, matching by `_id`. |

> **Note:** Scripts use controlled multithreading, batch sizes, and robust MongoDB connection management.

---

## Utilities

- `utils/database_connections.py` : Secure, reusable MongoDB connection class.
- `utils/logs_config.py` : Centralized logging configuration with info/debug/error levels.

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

## Important Notes

- MongoDB credentials are handled internally via `MongoDBConnection` class (adjust as needed).
- Some operations are destructive (e.g., moving or deleting documents). Use with caution.
- `count_documents.py` uses an optimized method for counting large datasets without timeouts.
- `count_duplicated.py` operates only on local JSON files, not directly on MongoDB.


---

## Final Words

MongoDataOps isn't just a set of scripts. It's a toolkit born from real data challenges.  
Thank you for checking it out! Hope it saves you time, simplifies your workflows, and sparks new ideas.


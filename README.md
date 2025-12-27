# Python ETL Framework – Call Center Data Pipeline

## Overview
This project implements a **modular, config-driven Python ETL framework** for processing call center data files.
It validates incoming files at multiple layers (file, schema, row), routes valid records to a target output, and
captures invalid records with detailed rejection reasons for audit and debugging.

The design mirrors **real-world enterprise ETL pipelines**, emphasizing maintainability, observability, and
separation of concerns.

---

## Why This Project?

This project was built to simulate real-world data engineering challenges commonly encountered in production batch pipelines, including:

- Processing large CSV files efficiently without loading entire datasets into memory
- Detecting and handling data quality issues through layered validation strategies
- Working with object storage systems such as Amazon S3 alongside local file systems
- Designing ETL pipelines that are modular, extensible, and reusable across datasets and environments

The primary goal was to gain hands-on experience with how production-grade ETL pipelines are **structured, validated, observed, and operated**, using Python as a data engineering tool.

---

## Key Features
- Config-driven execution (no hardcoded paths or rules)
- Pluggable storage support:
  - Local filesystem
  - Amazon S3 (object storage)
- Layered validation approach:
  - File-level validation
  - Schema/header validation
  - Row-level data validation
- Generator-based streaming for memory-efficient processing
- Structured logging per module
- Exception handling with reject reasons
- Modular architecture (reader / validations / writer / main)

## Future Enhancements

- Partitioned data layout for efficient rewrites
- Hash-based bucketing for partial updates
- Integration with table formats like Delta Lake or Apache Hudi
- Distributed execution using Apache Spark or AWS Glue

---

## Architecture
**Execution flow:**

The pipeline follows a validation-first, routing-second design:

```text
Source Data
(Local CSV or S3 Object)
   |
   v
File-Level Validation
(filename, extension)
   |
   v
Header / Schema Validation
(column count, order, names)
   |
   v
Row-Level Validation
(mandatory fields, time format, IDs, status)
   |
   v
Record Routing
   ├── Valid Records   → Local Target CSV
   └── Invalid Records → Local Exception CSV
                                |
                                v
                       (Optional S3 Upload)

```
---

## Storage Design (Local vs S3)

This framework supports both **local filesystem** and **Amazon S3** as storage backends, controlled via configuration.

### Local Storage
- Source, target, and exception files are read and written directly to disk.
- Supports iterative development and testing.
- Uses append semantics for record-level processing.

### Amazon S3 (Object Storage)
- Source data is streamed from S3 using the object key.
- Target and exception outputs are first written locally.
- Final outputs are uploaded to S3 as full object overwrites.

**Design Rationale:**
- S3 is an object store (not a filesystem) and does not support in-place appends.
- Writing locally first simplifies validation, auditing, and retry logic.
- Full-object overwrite ensures idempotent batch execution.

This design mirrors common data lake ingestion patterns used in production data platforms.

---
## Design Decisions

- Generator-based file reading is used to support large files without loading them into memory.
- Validation logic is split into file, schema, and row layers to isolate concerns and simplify extensibility.
- The pipeline is config-driven to avoid hardcoded rules and enable reuse across datasets.
- Logging is centralized and module-specific to improve observability and debugging in batch workflows.

---
## Project Structure

```text
python_etl_callcenter/
│
├── scripts/          # Modular, production-ready ETL pipeline
│ │
│ ├── main.py         # Pipeline entry point
│ ├── reader/         # File readers (generator-based)
│ ├── file_validations/
│ ├── schema_validations/
│ ├── row_validations/
│ ├── writer/
│ ├── s3_utils.py      # S3 upload utilities
│ ├── logging_factory.py
│ └── config_loader.py
│
├── archive/          # Legacy procedural implementation (for reference)
│ └── call_center_etl.py
│
├── config/           # Configuration files
│ └── pipeline_config.json
│
├── src_files/        # Input data
├── output/           # Target and exception outputs
├── logs/             # Application logs
│
└── README.md

```

---

## Configuration
All runtime behavior is controlled via `pipeline_config.json`.

Example configuration sections:
- File paths (source, target, exception)
- Expected filename rules
- Schema definition (column count, column names, positions)
- Logging configuration

This allows the pipeline to be adapted to new files **without code changes**.

---

## Validations Implemented

### File-Level
- Filename length validation
- Allowed file extensions

### Schema-Level
- Expected column count
- Column name and positional validation

### Row-Level
- Mandatory field checks
- Time format validation (`HH:MM:SS`)
- Numeric ID validation
- Valid call status enforcement

Each failed validation produces a **human-readable reject reason**.

---

## Logging
- Centralized logging via `logging_factory`
- Module-specific loggers
- Configurable log level
- Execution summary and error traceability

Logs are written to the `/logs` directory.

---

## How to Run
1. Update `pipeline_config.json` with correct paths and rules
2. Place source file in configured source directory
3. Execute:

```bash
python scripts/main.py
```

## Outputs

- **Target file**  
  Contains fully validated records

- **Exception file**  
  Contains rejected records with rejection reasons appended

- **Log files**  
  Execution details, errors, and summary metrics


## Notes

This project intentionally avoids database dependencies to focus on:

- ETL design patterns  
- Validation strategies  
- Modular Python architecture
- The S3 integration models object-store semantics rather than database-style row-level updates.  

The same framework can be extended to load data into databases, data warehouses, or APIs.

---

## Author

**Vinay Kovera**

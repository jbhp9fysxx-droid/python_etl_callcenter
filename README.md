# Python ETL Framework – Call Center Data Pipeline

## Overview
This project implements a **modular, config-driven Python ETL framework** for processing call center data files.
It validates incoming files at multiple layers (file, schema, row), routes valid records to a target output, and
captures invalid records with detailed rejection reasons for audit and debugging.

The design mirrors **real-world enterprise ETL pipelines**, emphasizing maintainability, observability, and
separation of concerns.

---

## Key Features
- Config-driven execution (no hardcoded paths or rules)
- Layered validation approach:
  - File-level validation
  - Schema/header validation
  - Row-level data validation
- Structured logging per module
- Exception handling with reject reasons
- Modular architecture (reader / validations / writer / main)

---

## Architecture
**Execution flow:**

The pipeline follows a validation-first, routing-second design:

```text
Source CSV
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
   ├── Valid Records   → Target CSV
   └── Invalid Records → Exception CSV (with reject reason)

---

## Project Structure


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
python main.py


Outputs

Target file: Contains fully validated records

Exception file: Contains rejected records with rejection reasons appended

Log file: Execution details, errors, and summary metrics

Notes

This project intentionally avoids database dependencies to focus on:

ETL design patterns

Validation strategies

Modular Python architecture

The same framework can be extended to load data into databases, data warehouses, or APIs.

Author

Vinay Kovera



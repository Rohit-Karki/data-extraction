# Multi-Threaded Data Pulling from Oracle/MySQL to HDFS/Iceberg

## To design and implement a high-performance, multi-threaded solution for parallel data
extraction from Oracle/MySQL databases and loading into:

- HDFS files (CSV/Parquet/ORC formats)
- Iceberg tables

## Functional Requirements:

- Support configurable thread counts / process counts for parallel extraction.
- Implement efficient partitioning strategies for source data.
- Handle incremental data pulls (CDC or timestamp-based).
- Ensure fault tolerance and data consistency.
- Provide monitoring capabilities for job progress.

Configurable threads counts / process counts can be achieved using Celery in built concurrency features.

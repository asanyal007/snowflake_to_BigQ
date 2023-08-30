# snowflake_to_BigQ

# Snowflake to BigQuery Data Migration

This repository contains Python code that facilitates the migration of data from Snowflake to BigQuery. The code establishes a connection to a Snowflake database, extracts schema information about tables, and then uses this information to create corresponding datasets and tables in BigQuery. It also handles table creation with partitioning if specified.

## Prerequisites

Before using the code in this repository, ensure that you have the following:

- Python installed (version X.Y or later)
- `snowflake-connector-python` package: Install using `pip install --upgrade snowflake-connector-python`
- `gcsfs` package: Install using `pip install gcsfs`
- Google Cloud Platform (GCP) project with BigQuery enabled
- GCP service account JSON key file with appropriate permissions

## Setup

1. Clone this repository to your local machine.

2. Install the required Python packages by running the following commands:

   ```
   pip install --upgrade snowflake-connector-python
   pip install gcsfs
   ```

3. Replace the placeholders in the code with your specific configurations:
   - Replace `user`, `password`, `account`, `database`, `project`, and `path` with your Snowflake and GCP project details.
   
4. Prepare a CSV file named `map.csv` containing the mapping of Snowflake tables to BigQuery tables and partitioning information. The file should have the following columns: `Schema`, `SourceTab`, `DestinationTab`, `ParitionCol`. Upload this CSV file to a GCS bucket specified in the `path` variable.

5. Run the Python script to initiate the data migration:

   ```
   python migration_script.py <user> <password> <account> <database> <project> <path>
   ```

   Replace `<user>`, `<password>`, `<account>`, `<database>`, `<project>`, and `<path>` with the appropriate values.

## Functionality

The provided Python script performs the following actions:

1. Connects to the Snowflake database using the provided credentials.

2. Reads the `map.csv` file from the specified GCS bucket to get the mapping of Snowflake tables to BigQuery tables and partitioning information.

3. Retrieves schema information (column names, data types, nullability) for tables from Snowflake's `information_schema`.

4. Creates datasets in BigQuery based on the schema information obtained.

5. Generates Data Definition Language (DDL) statements for table creation in BigQuery, considering partitioning if specified in the CSV.

6. Executes the DDL statements to create tables in BigQuery.

## Notes

- Make sure you have the necessary permissions and configurations in both Snowflake and BigQuery to perform the migration.

- The script uses the provided Snowflake and GCP credentials for authentication. Ensure that you handle credentials securely.

- It's recommended to thoroughly review and test the code in a controlled environment before using it for production migrations.

- This script provides a basic framework for migrating data between Snowflake and BigQuery. Depending on your specific requirements, you might need to adapt and extend the code.

- Always follow best practices for data migration and handle any potential errors or exceptions gracefully.

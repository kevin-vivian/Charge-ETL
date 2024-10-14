# Electric Vehicle Charge Points ETL Job

## Overview

This Python program defines an ETL (Extract, Transform, Load) job using PySpark for analyzing electric vehicle charge point data. The input data consists of charging event information from a CSV file, which is processed to calculate statistics for each charge point, such as the maximum and average plugin durations. The results are saved as a Parquet file.

## Objective

The goal of the ETL job is to:
- **Extract**: Read a CSV file containing charge point data.
- **Transform**: Process the data to compute the maximum and average plugin duration for each charge point.
- **Load**: Save the transformed data to an output location in Parquet format.

## Input Data

The input data file `electric-chargepoints-2017.csv` contains the following columns:
- `ChargingEvent` (INTEGER)
- `CPID` (VARCHAR): Unique identifier for each charge point.
- `StartDate` (DATE)
- `StartTime` (TIMESTAMP)
- `EndDate` (DATE)
- `EndTime` (TIMESTAMP)
- `Energy` (DECIMAL)
- `PluginDuration` (DECIMAL): Duration (in hours) for which a vehicle was plugged into the charging point.

## Output Data

The output data is saved in Parquet format and contains the following columns:
- `chargepoint_id` (VARCHAR): Unique identifier for the charge point (renamed from `CPID`).
- `max_duration` (FLOAT): The maximum plugin duration (in hours) for each charge point, rounded to 2 decimal places.
- `avg_duration` (FLOAT): The average plugin duration (in hours) for each charge point, rounded to 2 decimal places.

## Project Structure

```plaintext
.
├── data/
│   ├── input/
│   │   └── electric-chargepoints-2017.csv
│   └── output/
│       └── chargepoints-2017-analysis/  # Parquet output directory
├── chargepoints_etl.py  # Python script containing the ETL job
└── README.md  # This file
```

## Installation

### 1. Install Python
Make sure you have Python installed (preferably Python 3.7+).

### 2. Install PySpark
Install PySpark using `pip`:

```bash
pip install pyspark
```

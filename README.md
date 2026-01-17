# Overview of Azure Data Engineering End-to-End Project
This repository demonstrates an end-to-end Azure Data Engineering project built from scratch using modern Azure cloud services. 
The project showcases how to design, build, and orchestrate a scalable data pipeline for ingestion, transformation, and analytics.

## 🛠️ Technologies Used

Azure Data Factory (ADF) – Data ingestion and pipeline orchestration

Azure Data Lake Storage (ADLS Gen2) – Centralized data storage

Azure Databricks – Data transformation using PySpark

Apache Spark – Distributed data processing

Azure Synapse Analytics – Data warehousing and analytical querying

Power BI - Data visualization

## 📌 Project Architecture

The project follows a medallion architecture approach:

Bronze Layer – Raw data ingestion from source systems

Silver Layer – Cleaned and transformed data using PySpark in Databricks

Gold Layer – Curated data stored in Azure Synapse for reporting and analytics

<img width="1083" height="532" alt="image" src="https://github.com/user-attachments/assets/7ea54c72-09f3-46fe-9f46-be82b11ca716" />

## 🔄 Data Pipeline Workflow

### Data Ingestion
Azure Data Factory pipelines ingest raw data into Azure Data Lake.

### Data Transformation
Azure Databricks processes and transforms data using PySpark.

### Data Storage
Processed data is stored in structured layers in ADLS.

### Analytics & Reporting
Azure Synapse Analytics is used for querying and analytical workloads.

## Future Enhancement
Automate the Pipeline in Azure Data Factory

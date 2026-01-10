# SQL Data Warehouse Project

This project demonstrates the design and implementation of a modern data warehouse using SQL, including ETL processes, data modeling, and analytical querying.

The project is inspired by learning materials from Baraa, where the original implementation was based on SQL Server. In this version, the solution has been adapted to use **PostgreSQL running in Docker**, with **TablePlus** as the primary database management tool, to explore a more portable and modern data engineering workflow.

Designed as a portfolio project, this work highlights practical data warehousing concepts and industry best practices, from raw data ingestion to analytics-ready datasets.


# Project Overview 
This project aims to build a modern data warehouse for sales analytics by designing a structured data architecture, implementing ETL pipelines, and delivering analytics-ready data for reporting and decision-making.

**The project involves** : 
![Data Architecture](images/architecture.png)
**1. Data Architecture** :
The data architecture is designed using the **Medallion Architecture**, which organizes data into **Bronze**, **Silver**, and **Gold** layers to ensure data quality, 


   **1. Bronze Layer**
   Loads and stores raw data from source systems in its original form (as-is) by ingesting CSV files into PostgreSQL.
   **2. Silver Layer**
   Prepares and Transform data for analysis by applying data cleansing, standardization, and normalization processes.
   **3. Gold Layer** 
   Delivers business-ready data by modeling cleaned data into a star schema to support reporting and analytics.

**2. ETL Pipelines** :
     Implements ETL pipelines to extract data from source systems, transform it through multiple layers, and load it into the data warehouse in a structured and reliable manner.

**3. Data Modeling** :   
     Designs fact and dimension tables using a star schema to support efficient analytical querying and meaningful business analysis. 
      
**4. Analytics & Reporting** :
     Uses SQL-based queries to analyze sales performance and support data-driven decision-making.



# Links & Tools : 
- **Notion Project Workspace**: A project planning space , used to plan the project, track implementation progress, and document design decisions throughout development.


## Design Scope and Assumptions

- The data warehouse integrates sales data from two source systems (ERP and CRM) provided as CSV files.
- The project focuses on building an analytics-ready data model and does not include historization of data.
- Data quality issues are addressed during the transformation process to ensure reliable analytical outputs.


## Future Work: Analytics & Reporting

**Objective**: Identify potential analytical directions to gain deeper insights, such as:

- Customer behavior and segmentation
- Product lifecycle performance
- Sales trends and seasonality


# Repository Structure 
```
Data-Warehouse-Project/
│
├── datasets/                           # Source CSV datasets used in the project (ERP and CRM)
│
├── docs/                               # Design notes and architecture-related detail files
│   ├── etl.drawio                      # ETL process design and data transformation flow
│   ├── data_architecture.drawio        # Overall data warehouse architecture diagram
│   ├── data_catalog.md                 # Overview of datasets and key fields
│   ├── data_flow.drawio                # Data flow from source systems to analytical layers
│   ├── data_models.drawio              # Data modeling diagrams (star schema)
│   ├── naming-conventions.md           # Naming guidelines applied across tables,columns and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for ingesting raw data (as-is)
│   ├── silver/                         # Scripts for data cleansing and transforming
│   ├── gold/                           # Scripts for building analytics-ready models
│
├── tests/                              # Data quality checks and validation scripts (optional)
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information
├── .gitignore                          # Files and directories ignored by Git
└── requirements.txt                    # Project dependencies (if applicable)

```






# Liscen

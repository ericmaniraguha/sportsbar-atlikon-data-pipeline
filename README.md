# FMCG M&A Data Consolidation — SportsBar → Atlikon Lakehouse

![Databricks](https://img.shields.io/badge/Platform-Databricks-FF3621?logo=databricks\&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Engine-Apache%20Spark-E25A1C?logo=apachespark\&logoColor=white)
![PySpark](https://img.shields.io/badge/Processing-PySpark-FDEE21?logo=apachespark\&logoColor=black)
![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-00ADD8)
![AWS S3](https://img.shields.io/badge/Storage-AWS%20S3-569A31?logo=amazons3\&logoColor=white)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)
![License](https://img.shields.io/badge/License-MIT-green)

> **End-to-end data engineering project demonstrating the integration of an acquired FMCG retail business into an enterprise Lakehouse using Databricks, PySpark, Delta Lake, and AWS S3.**

---

## 📌 Project Overview

This project simulates a real-world **Mergers & Acquisitions (M&A) data integration scenario** in the Fast-Moving Consumer Goods (FMCG) retail sector.

**SportsBar**, an acquired retail business, operates with its own transactional data structures, naming conventions, and historical datasets. The objective is to integrate SportsBar's data into **Atlikon's enterprise data platform** while preserving historical information and creating standardized analytical models.

The solution implements a **Medallion Lakehouse Architecture**:

```text
                    ┌──────────────────────┐
                    │   SportsBar OLTP     │
                    │   Source Systems     │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │      AWS S3          │
                    │ Raw / Staging /      │
                    │ Archive              │
                    └──────────┬───────────┘
                               │
                               ▼
              ┌─────────────────────────────────┐
              │         BRONZE LAYER            │
              │ Raw Delta Tables                │
              │ Minimal Transformation          │
              │ Ingestion Metadata              │
              └───────────────┬─────────────────┘
                              │
                              ▼
              ┌─────────────────────────────────┐
              │          SILVER LAYER           │
              │ Cleaning & Standardization      │
              │ Deduplication                   │
              │ Schema Conformance               │
              │ Historical + Incremental Loads   │
              └───────────────┬─────────────────┘
                              │
                              ▼
              ┌─────────────────────────────────┐
              │           GOLD LAYER            │
              │ Enterprise Star Schema          │
              │ Sales & Margin Analytics        │
              │ Store Performance               │
              └───────────────┬─────────────────┘
                              │
                 ┌────────────┴────────────┐
                 ▼                         ▼
        ┌──────────────────┐      ┌──────────────────┐
        │ Databricks SQL   │      │ Databricks Genie │
        │ Dashboards       │      │ Natural Language │
        │                  │      │ Analytics        │
        └──────────────────┘      └──────────────────┘
```

---

# 🎯 Business Objective

The project addresses a common enterprise data engineering problem:

> **How can an acquired company's data be integrated into an existing enterprise Lakehouse without losing historical information, while maintaining data quality, scalability, and consistent business definitions?**

The pipeline is designed to:

* Integrate SportsBar transactional data into Atlikon's enterprise data platform.
* Standardize schemas and business definitions.
* Preserve historical transactions.
* Support incremental data processing.
* Reduce unnecessary compute through incremental pipelines.
* Create enterprise-ready analytical models.
* Enable cross-company sales and performance analysis.
* Provide data products for dashboards and conversational analytics.

---

# 🏗️ Architecture

The solution follows the **Medallion Architecture** pattern.

### Bronze — Raw Data

The Bronze layer preserves source data with minimal transformation.

Key characteristics:

* Append-oriented ingestion.
* Raw source preservation.
* Delta Lake storage.
* Source-file tracking.
* Ingestion timestamp.
* Load metadata.
* Reproducibility and auditability.

Example metadata:

```text
ingestion_timestamp
source_file
source_system
batch_id
load_date
```

---

### Silver — Conformed Data

The Silver layer transforms raw data into clean, standardized datasets.

Key activities include:

* Schema standardization.
* Column name normalization.
* Data type enforcement.
* Null handling.
* Duplicate removal.
* Business-rule validation.
* Dimension conformance.
* Historical baseline processing.
* Incremental processing.
* Cross-system entity mapping.

The Silver layer creates a consistent representation of SportsBar and Atlikon data.

---

### Gold — Business & Analytics Layer

The Gold layer provides business-ready analytical datasets.

The model follows a **Star Schema** approach consisting of:

```text
              ┌─────────────────┐
              │   Dim_Date      │
              └────────┬────────┘
                       │
                       │
┌─────────────┐   ┌────▼─────────┐   ┌─────────────┐
│ Dim_Store   │───│  Fact_Sales  │───│ Dim_Product │
└─────────────┘   └────┬─────────┘   └─────────────┘
                       │
                       │
                ┌──────▼───────┐
                │ Dim_Customer  │
                └──────────────┘
```

The Gold layer supports:

* Revenue analysis.
* Sales performance.
* Product/category analysis.
* Store performance.
* Margin analysis.
* SportsBar vs. Atlikon comparisons.
* Enterprise-level reporting.

---

# ⚙️ Technology Stack

| Technology                               | Purpose                                        |
| ---------------------------------------- | ---------------------------------------------- |
| **Databricks**                           | Lakehouse platform and development environment |
| **Apache Spark**                         | Distributed data processing                    |
| **PySpark**                              | Data transformation and engineering            |
| **Spark SQL**                            | SQL-based transformations and analytics        |
| **Delta Lake**                           | ACID transactional storage                     |
| **Unity Catalog**                        | Data governance and access management          |
| **AWS S3**                               | Cloud object storage                           |
| **Databricks Workflows / Lakeflow Jobs** | Pipeline orchestration                         |
| **Databricks SQL**                       | Analytical queries and dashboards              |
| **Databricks Genie**                     | Natural-language data exploration              |
| **Python**                               | Pipeline development and automation            |
| **Git/GitHub**                           | Version control and collaboration              |

---

# 🔄 Data Engineering Pipeline

## 1. Source & Landing

Raw transactional datasets are delivered from SportsBar source systems.

```text
SportsBar OLTP
      │
      ▼
AWS S3
├── raw/
├── staging/
└── archive/
```

The landing architecture separates:

* **Raw** — original source data.
* **Staging** — temporary processing area.
* **Archive** — historical source files.

---

## 2. Bronze Ingestion

Source files are ingested into Delta tables.

Example:

```python
df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(source_path)
)

df = (
    df.withColumn("ingestion_timestamp", current_timestamp())
      .withColumn("source_file", input_file_name())
)

df.write.format("delta").mode("append").save(bronze_path)
```

The Bronze layer intentionally performs minimal transformation to preserve source fidelity.

---

# 🧹 3. Silver Transformation

The Silver pipeline performs data quality and standardization operations.

### Standardization

```text
Source Systems
      │
      ├── Different column names
      ├── Different data types
      ├── Different business codes
      └── Different naming conventions
                  │
                  ▼
          Conformed Silver Model
```

Typical transformations include:

* Column standardization.
* Data type conversion.
* Date normalization.
* Currency normalization.
* Null handling.
* Duplicate detection.
* Referential integrity checks.
* Business-rule validation.

---

# 🔁 4. Historical & Incremental Processing

The project demonstrates two major processing patterns.

### Historical Processing

Used for the initial migration and historical baseline.

```text
Historical Source Data
        │
        ▼
Historical Processing
        │
        ▼
Silver Baseline
        │
        ▼
Gold Enterprise Model
```

Implementation:

```text
process_facts_historical.py
```

---

### Incremental Processing

After the historical baseline is established, only new or changed records are processed.

```text
New / Changed Data
        │
        ▼
Incremental Detection
        │
        ▼
Transform
        │
        ▼
Delta MERGE
        │
        ▼
Updated Silver / Gold
```

Implementation:

```text
process_facts_incremental.py
```

This approach helps reduce unnecessary processing and improves pipeline efficiency.

---

# 🔀 5. Data Consolidation

The central M&A challenge is bringing SportsBar data into Atlikon's enterprise model.

```text
SportsBar                 Atlikon
    │                        │
    │                        │
    ▼                        ▼
SportsBar Schema       Atlikon Schema
    │                        │
    └──────────┬─────────────┘
               ▼
       Schema Conformance
               │
               ▼
       Entity Mapping
               │
               ▼
       Enterprise Model
               │
               ▼
        Gold Analytics
```

The consolidated model enables cross-entity analysis using common business definitions.

---

# 📊 Analytics & Business Use Cases

The Gold layer enables analytical questions such as:

### Sales

* What is total revenue by month?
* How does SportsBar revenue compare with Atlikon?
* Which stores generate the highest sales?
* Which product categories are growing fastest?

### Store Performance

* Which stores have the highest revenue?
* Which locations have declining sales?
* What is the average sales per store?

### Product Performance

* Which products contribute the most revenue?
* Which categories generate the highest margins?
* How does product performance differ between entities?

### M&A Performance

* What is SportsBar's contribution to enterprise revenue?
* How does SportsBar compare with existing Atlikon stores?
* What are the post-acquisition sales trends?
* Which stores or categories require management attention?

---

# 📈 Databricks SQL Dashboards

The project includes executive-style dashboards for monitoring:

* Total Revenue
* Sales Growth
* Store Performance
* Product/Category Performance
* Gross Margin
* SportsBar vs. Atlikon Performance
* Monthly and quarterly trends

Example executive view:

```text
┌──────────────────────────────────────────────────────┐
│              ATLIKON ENTERPRISE SALES                │
├──────────────┬──────────────┬──────────────┬─────────┤
│ Total Sales  │ Growth       │ Stores       │ Margin  │
│ $XXX.XM      │ XX.X%        │ XXX          │ XX.X%   │
├──────────────┴──────────────┴──────────────┴─────────┤
│                                                      │
│              Revenue Trend                           │
│                                                      │
├──────────────────────────────────────────────────────┤
│                                                      │
│       SportsBar vs Atlikon Performance               │
│                                                      │
└──────────────────────────────────────────────────────┘
```

---

# 🤖 Databricks Genie

A curated Gold-layer semantic model enables business users to interact with the data using natural language.

Example questions:

> "What was the sales difference between SportsBar and Atlikon stores last quarter?"

> "Which SportsBar stores generated the highest revenue?"

> "What product category has the highest margin?"

> "Show monthly revenue trends since the acquisition."

This demonstrates how a modern Lakehouse can support both **traditional BI and AI-assisted analytics**.

---

# 🔐 Data Governance

The project uses **Unity Catalog** concepts for centralized data governance.

Governance considerations include:

* Catalog/schema organization.
* Table ownership.
* Access control.
* External locations.
* Cloud storage credentials.
* Data lineage.
* Controlled access to analytical datasets.

Example logical organization:

```text
Unity Catalog
│
└── Atlikon Catalog
    │
    ├── bronze
    │
    ├── silver
    │
    └── gold
```

---

# 🔍 Data Quality

Data quality checks are incorporated throughout the pipeline.

Examples include:

```text
✓ Null validation
✓ Duplicate detection
✓ Data type validation
✓ Referential integrity
✓ Schema validation
✓ Business-rule validation
✓ Record-count reconciliation
✓ Source-to-target reconciliation
```

For an M&A migration, reconciliation is particularly important to ensure that records are not lost during consolidation.

---

# 🚀 Orchestration

Databricks Workflows / Lakeflow Jobs orchestrate the pipeline.

Example dependency graph:

```text
              ┌───────────────┐
              │ Source Files  │
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │ Bronze Load   │
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │ Silver Clean  │
              └───────┬───────┘
                      │
             ┌────────┴────────┐
             ▼                 ▼
       Historical         Incremental
       Processing         Processing
             │                 │
             └────────┬────────┘
                      ▼
              ┌───────────────┐
              │ Gold Models   │
              └───────┬───────┘
                      │
             ┌────────┴────────┐
             ▼                 ▼
        SQL Dashboards       Genie
```

Pipeline monitoring includes:

* Task dependencies.
* Job execution status.
* Failure handling.
* Run history.
* Alerting.
* Processing metrics.

---

# 📁 Repository Structure

```text
fmcg-ma-data-consolidation/
│
├── data/
│   └── sample_raw/
│       └── README.md
│
├── notebooks/
│   ├── 01_setup/
│   │   ├── catalog_setup
│   │   └── storage_configuration
│   │
│   ├── 02_bronze/
│   │   ├── ingest_sources
│   │   └── create_bronze_tables
│   │
│   ├── 03_silver/
│   │   ├── process_dimensions
│   │   ├── process_facts_historical.py
│   │   ├── process_facts_incremental.py
│   │   └── data_quality_checks
│   │
│   ├── 04_gold/
│   │   ├── create_fact_sales
│   │   ├── create_dimensions
│   │   └── enterprise_consolidation
│   │
│   └── 05_serving/
│       ├── sql_views
│       └── genie_preparation
│
├── workflows/
│   └── databricks_pipeline.json
│
├── docs/
│   ├── architecture_diagram.png
│   ├── data_model.png
│   └── screenshots/
│
├── tests/
│   └── data_quality/
│
├── .gitignore
├── LICENSE
└── README.md
```

---

# 🧪 Testing & Validation

The project validates both pipeline processing and business correctness.

Validation includes:

```text
Source Records
      │
      ▼
Bronze Record Count
      │
      ▼
Silver Record Count
      │
      ▼
Gold Record Count
      │
      ▼
Business Reconciliation
```

Example validation metrics:

* Source vs. Bronze record counts.
* Bronze vs. Silver record counts.
* Duplicate counts.
* Null rates.
* Referential integrity.
* Revenue reconciliation.
* Incremental-load validation.

---

# 💡 Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

### Data Engineering

* ETL / ELT pipelines
* Batch processing
* Incremental processing
* Historical data migration
* Data consolidation
* Schema evolution
* Data quality
* Data reconciliation

### Big Data

* Apache Spark
* PySpark
* Spark SQL
* Distributed transformations
* Partition-aware processing

### Lakehouse

* Medallion Architecture
* Delta Lake
* Bronze / Silver / Gold layers
* ACID transactions
* Lakehouse modeling

### Cloud

* AWS S3
* Cloud object storage
* External locations
* Cloud-based data pipelines

### Databricks

* Unity Catalog
* Databricks Workflows
* Databricks SQL
* Delta tables
* Genie
* Lakehouse architecture

### Analytics

* Dimensional modeling
* Star schemas
* KPI development
* Executive dashboards
* Self-service analytics

---

# 📚 What This Project Demonstrates

This project is designed to demonstrate the ability to build a **production-oriented data engineering solution**, rather than simply perform isolated Spark transformations.

It combines:

```text
Cloud Storage
      +
Distributed Processing
      +
Lakehouse Architecture
      +
Data Modeling
      +
Incremental Processing
      +
Data Quality
      +
Orchestration
      +
Analytics
      +
AI-Assisted BI
```

The result is an end-to-end platform capable of taking raw transactional data and transforming it into **trusted enterprise analytical data products**.

---

# 🔮 Future Improvements

Potential extensions include:

* Implementing Delta Live Tables / Lakeflow Declarative Pipelines.
* Adding automated data-quality expectations.
* Implementing Change Data Feed (CDF).
* Adding CI/CD using GitHub Actions.
* Infrastructure-as-Code using Terraform.
* Adding automated unit and integration tests.
* Implementing dbt models for the analytical layer.
* Adding data lineage and observability.
* Implementing role-based access control.
* Adding automated anomaly detection.
* Implementing ML-based sales forecasting.
* Adding real-time/streaming ingestion with Structured Streaming.

---

# 👨‍💻 Author

**Eric Maniraguha**

Data Engineer | Data Science & Digital Transformation Consultant | ICT Solutions Consultant

**Focus Areas**

`Data Engineering` · `PySpark` · `Databricks` · `SQL` · `Oracle` · `PostgreSQL` · `ETL/ELT` · `Lakehouse` · `Cloud Data Platforms` · `Data Analytics`

---

## ⭐ Project Highlights

**End-to-end M&A Data Engineering**

**SportsBar → Atlikon Enterprise Integration**

**Databricks + PySpark + Delta Lake + AWS S3**

**Historical + Incremental Processing**

**Medallion Lakehouse Architecture**

**Star Schema & Enterprise Analytics**

**Databricks SQL + Genie**

---

> **Note:** This repository is a portfolio implementation that simulates an FMCG M&A data integration scenario using representative/sample data. It does not contain confidential enterprise data.
# sportsbar-atlikon-data-pipeline

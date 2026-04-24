
-----

# Atliq Data Pipeline: End-to-End AWS & Databricks ETL

[](https://aws.amazon.com/)
[](https://www.databricks.com/)
[](https://spark.apache.org/)

## 📌 Project Overview

This project was developed during the **Data Engineering Virtual Internship**. It implements a scalable, automated data pipeline for **Atliq**, a simulated retail company. The pipeline ingests, validates, and transforms messy finance and sales data into analytics-ready Delta tables using a **Medallion Architecture**.

### ⚡ Key Features

  * **Event-Driven Ingestion:** Automated triggers using S3 Events and AWS Lambda.
  * **Automated Data Quality:** Schema validation and null-checks integrated into the Glue (PySpark) layer.
  * **Medallion Architecture:** Multi-stage processing (Bronze → Silver) using Databricks and Delta Lake.
  * **Observability:** Metadata logging with DynamoDB and SQL-based monitoring via Amazon Athena.

-----

## 🏗 Architecture & Workflow

The solution combines AWS serverless capabilities with the high-performance processing power of Databricks.

1.  **Ingestion Layer (Landing Zone):** Raw files are uploaded to Amazon S3.
2.  **Validation Layer (Bronze):** An **AWS Lambda** function triggers an **AWS Glue (PySpark)** job.
      * *Success:* Data moves to the **Bronze** folder (Parquet).
      * *Failure:* Invalid records are routed to a **Rejected** folder for auditing.
3.  **Observability Layer:** Metadata (run IDs, record counts, timestamps) is logged to **Amazon DynamoDB** and exported to **Amazon Athena** for querying.
4.  **Transformation Layer (Silver):** **Databricks** processes Bronze data, performing currency normalization (USD to INR), date standardization, and business logic joins.
5.  **Storage Layer:** Final datasets are stored as **Delta Lake** tables, supporting ACID transactions and schema enforcement.

<img width="311" height="477" alt="image" src="https://github.com/user-attachments/assets/c9bcc21e-eec0-4d31-aac7-ded998435ef5" />

-----

## 🛠 Tech Stack

| Category | Tools |
| :--- | :--- |
| **Cloud Platform** | AWS (Amazon Web Services) |
| **Data Lake / Storage** | Amazon S3, Delta Lake |
| **Processing** | AWS Glue (PySpark), Apache Spark (Databricks) |
| **Orchestration / Triggers** | AWS Lambda, S3 Event Notifications |
| **Metadata & Logs** | Amazon DynamoDB, AWS Glue Data Catalog |
| **Query Engine** | Amazon Athena (Presto-based) |
| **Formats** | CSV, Parquet, Delta |

-----

## 📊 Data Transformations (Silver Layer)

In the Databricks environment, the following logic is applied to create the `finance_sales_delta` table:

  * **Standardization:** Dates converted to `yyyy-MM-dd`.
  * **Currency Conversion:** Normalizing all financial metrics from USD to INR.
  * **Handling Nulls:** Imputing missing region values based on master data.
  * **Feature Engineering:** \* Calculated `Profit Margin`.
      * Categorized transactions into `Domestic` vs `International`.
  * **Optimization:** Data is stored in Delta format to enable **Time Travel** and **Change Data Feed (CDF)**.
    
<img width="834" height="463" alt="image" src="https://github.com/user-attachments/assets/c00aa5e6-8958-4016-9088-f068d5f2b0cf" />

-----

## 📈 Pipeline Metrics & Observability

We track the health of the pipeline through a dedicated monitoring view in Amazon Athena:

  * **Total Records Processed:** High-level volume tracking.
  * **Rejected Record Count:** Identifying source system inconsistencies.
  * **Lineage Tracking:** Mapping every Delta row back to its source S3 file and Glue Run ID.

-----

## 🚀 Future Enhancements

  * [ ] **Gold Layer:** Implement highly aggregated tables optimized for PowerBI/Tableau.
  * [ ] **CI/CD:** Automate deployment using GitHub Actions and Terraform.
  * [ ] **Alerting:** Integrate Amazon SNS to send email alerts on job failures.
  * [ ] **Data Governance:** Implement Unity Catalog for centralized access control.

-----

## 🎓 Key Learnings

  * Building robust **Schema-on-Read** validation logic to prevent pipeline breakage.
  * Managing state and metadata in a distributed environment using **DynamoDB**.
  * Leveraging **Delta Lake** features like schema enforcement to ensure data reliability for downstream business users.

-----

*Developed as part of the Atliq Data Engineering Internship.*

---

<details>

---



---


  
</details>





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
Step 1: create buckete atliq-de-mus
s3://atliq-de-mus/ (S3 Bucket)

📁 raw/ 
 📁 finance/
 📁 sales/
📁 rejected/ 
📁 bronze/ (Created automatically by the Glue Job!)

<img width="1360" height="544" alt="image" src="https://github.com/user-attachments/assets/0abcf260-836d-4cf2-8fd4-7456e44361a2" />



Step 1: a) Visual Configure the AWS Glue Job

<img width="1363" height="586" alt="image" src="https://github.com/user-attachments/assets/3f7e7f32-a6f8-4e2b-afc1-501c2d5ef315" />
<img width="1355" height="538" alt="image" src="https://github.com/user-attachments/assets/3e3cf60e-6887-4ec0-a34c-69b75c5ee1f4" />
<img width="1350" height="551" alt="image" src="https://github.com/user-attachments/assets/688a4798-a691-4de4-a20b-1c61e8a3fa9b" />
<img width="960" height="459" alt="image" src="https://github.com/user-attachments/assets/269c8996-91ce-4d05-91f4-873f53d088d0" />

- Transformation - everything is in string by default 
<img width="1347" height="424" alt="image" src="https://github.com/user-attachments/assets/373decba-7dab-4358-b4da-c8ca4358ca3a" />
- Spark will successfully parse "09-01-2025", but it will look at "Sep 01, 2025", fail to understand it, and turn it into null - coalesce
<img width="1350" height="420" alt="image" src="https://github.com/user-attachments/assets/4262182c-d123-4570-b874-2a2bc4e5a972" />
<img width="1351" height="404" alt="image" src="https://github.com/user-attachments/assets/301b248a-7a57-4d21-8035-74e1b5700891" />

<img width="774" height="454" alt="image" src="https://github.com/user-attachments/assets/e93feec6-cf65-40aa-8470-b5eac819089a" />
<img width="1327" height="438" alt="image" src="https://github.com/user-attachments/assets/ddb6827f-a632-416d-9276-9115e4081050" />
<img width="1137" height="459" alt="image" src="https://github.com/user-attachments/assets/0be0e4c8-482d-429a-9776-8b0d721a0a5b" />
<img width="1163" height="369" alt="image" src="https://github.com/user-attachments/assets/9321665c-2f78-4f91-bd73-dda4d6c5dd14" />
<img width="695" height="332" alt="image" src="https://github.com/user-attachments/assets/443eb8aa-eb9d-4886-ac19-d6545302ef14" />
<img width="1361" height="476" alt="image" src="https://github.com/user-attachments/assets/1e7f5c5c-d7df-4e3c-acf3-cae72d1613e5" />

- manual job run
- via lambda job run
- b) script job run
- 
<img width="796" height="464" alt="image" src="https://github.com/user-attachments/assets/0f88c3df-a9e3-4b2b-9e6a-1d62609f4a53" />

<img width="903" height="423" alt="image" src="https://github.com/user-attachments/assets/5f229973-9881-483f-84e3-443548016854" />


- Configure an AWS Lambda trigger to execute the Glue Job whenever a new file lands in any raw/ subfolder.






---


  
</details>




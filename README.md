
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

- IAM
<img width="1353" height="427" alt="image" src="https://github.com/user-attachments/assets/5a57f0b3-acfc-490b-924b-cc534728b0b0" />
<img width="1356" height="391" alt="image" src="https://github.com/user-attachments/assets/477729c5-0157-4892-8283-02c9439f1be6" />

 
- Configure an AWS Lambda trigger to execute the Glue Job whenever a new file lands in any raw/ subfolder.
<img width="1338" height="468" alt="image" src="https://github.com/user-attachments/assets/49a615fb-1d50-4bba-a32b-e7309b9d0f76" />

Lambda successfully told Glue to start the job
<img width="1359" height="593" alt="image" src="https://github.com/user-attachments/assets/b35bdb26-439b-4312-a52c-793c2f3ae5bb" />
<img width="1357" height="554" alt="image" src="https://github.com/user-attachments/assets/6831cc5d-a24d-44db-b14c-e76511e6493c" />

<img width="1357" height="557" alt="image" src="https://github.com/user-attachments/assets/983bc830-5d39-42b5-ae5e-86ca64b9c2fa" />
<img width="1362" height="579" alt="image" src="https://github.com/user-attachments/assets/08d4efad-82e2-40f4-92dd-8f49aaf9b7e6" />
<img width="1366" height="563" alt="image" src="https://github.com/user-attachments/assets/86a06811-a366-4d16-a9eb-98b6102453a8" />
- output logs - CloudWatch
<img width="1360" height="541" alt="image" src="https://github.com/user-attachments/assets/c21fb904-e385-43e2-a2bb-b41737ebcf64" />
<img width="1357" height="481" alt="image" src="https://github.com/user-attachments/assets/559e9d8d-bf0c-46e3-802a-c6a35d949d3d" />

- as soon as upload new sales file - auto AWS Lambda trigger - "All object create events", the very second that file finished uploading to your S3 folder, a "notification" was sent to your Lambda, which should have already started your Glue job.
<img width="1359" height="465" alt="image" src="https://github.com/user-attachments/assets/bb36e83a-f35e-4638-b96c-4de8e6a35d58" />
<img width="1343" height="519" alt="image" src="https://github.com/user-attachments/assets/942f0f02-40d8-4b8c-a2cf-64269939ae7b" />





---
- Based on the CloudWatch logs you've shared, your pipeline successfully caught the "bad" data for both Finance and Sales.

### **Data Validation Summary**

| Dataset | Total Rows | Valid (Bronze) | **Invalid (Rejected)** |
| :--- | :--- | :--- | :--- |
| **Finance** | 508 | 428 | **80** |
| **Sales** | 505 | 391 | **114** |
| **Total** | **1,013** | **819** | **194** |

---
<img width="1355" height="490" alt="image" src="https://github.com/user-attachments/assets/8a7ab8f6-96fd-4c24-a121-263d28fcaf68" />
<img width="1360" height="551" alt="image" src="https://github.com/user-attachments/assets/2f49f8a4-eed5-4453-9a59-0d0086abead7" />

<img width="1358" height="478" alt="image" src="https://github.com/user-attachments/assets/01d82f6a-448e-4570-b21b-b869905627b7" />

----
# Bronze_to_Silver - Transformation in Databricks

<img width="1239" height="633" alt="image" src="https://github.com/user-attachments/assets/389a94bb-680c-450e-a875-62ca708b5df1" />
<img width="864" height="625" alt="image" src="https://github.com/user-attachments/assets/b1854b22-6d23-46a7-b8a9-89e8176c65dc" />
<img width="1200" height="580" alt="image" src="https://github.com/user-attachments/assets/8d5482e0-d7b7-44f3-920a-26537c8eef6b" />
<img width="1236" height="631" alt="image" src="https://github.com/user-attachments/assets/051a3118-c89a-4f4e-84a5-04d8c45a8cc4" />






---


  
</details>




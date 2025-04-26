# DBT_Stock

Cloud run codes and architecture diagram in ELT folders 

DBT sql files inside model folder

📊 ELT Data Pipeline System (GCP + dbt + BI Tools)
Overview
This project demonstrates a modern ELT (Extract, Load, Transform) pipeline built entirely on Google Cloud Platform (GCP).
It automates the process of fetching external CSV data on a weekly schedule, storing it securely, loading it into BigQuery, and transforming it using dbt for final analysis in BI tools like Power BI and Looker.

The system is highly modular, scalable, and monitors errors automatically using Cloud Functions notifications.

🛠️ Architecture Diagram

📋 Workflow Description
1. External CSV Source (Weekly Scheduled Ingestion)

   Data is fetched weekly from an external CSV source.

   Triggered by a scheduled process (could be a Cloud Scheduler or any orchestrator).

3. Cloud Run: Ingest CSV

   A Cloud Run service is responsible for ingesting the CSV.

   It processes the incoming CSV file and stores it into Google Cloud Storage (GCS).

5. Google Cloud Storage (GCS)

   Acts as intermediate storage.

   Once a file is uploaded, a GCS Event Trigger automatically fires to continue the pipeline.

7. Cloud Run: Load to BigQuery

   A second Cloud Run service listens to GCS triggers.

   It loads the newly uploaded CSV directly into a BigQuery raw dataset.

   This keeps ingestion and loading operations clean and separated.

9. BigQuery (Raw Data Layer)

   Holds unprocessed/raw data.

   Acts as the single source of truth before transformations.

11. DBT (Data Build Tool)

    dbt models run on the raw data.

    Responsible for cleansing, validations, and transformations.

    Only validated and cleaned data is moved further.

13. BigQuery (Transformed Data Layer)

    Transformed datasets are stored here.

    This serves as the final data warehouse layer optimized for analytics.

15. BI Tools (Power BI & Looker)

    Power BI and Looker connect to the transformed BigQuery tables.

    Business users and analysts can build dashboards and generate insights.

17. Error Handling (Cloud Functions)

    In case of failures at any stage, error alerts are triggered using Cloud Functions.

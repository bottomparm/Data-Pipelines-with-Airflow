# Data Pipelines with Apache Airflow

## Context

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The task at hand is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Data quality plays a big part when analyses are executed on top of the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in AWS S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Tech Stack

- Airflow

## Sparkify Airflow DAG

<img src="https://video.udacity-data.com/topher/2019/January/5c48a861_example-dag/example-dag.png" width="900" height="250">

The DAG diagram above illustrates the DAG's workflow. The blocks represent an operator or task. The arrows represent dependencies.

## Run

In order to run this project you will need the following:

- An AWS Redshift Cluster with an "Available" status that is publically accessible.
- The appropriate Apache Airflow Admin Connections defined in your Airflow instance.
  - redshift
  - aws_credentials

Once these conditions are satisfied the project DAG can be turned on, triggered and monitored from the Airflow webserver.

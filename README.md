# Fraud Pattern Detection in Digital Payments

> Built by two data science graduates, Mahimna Darji & Hema Manasi Potnuru. This project simulates a fraud detection system for digital payments using Kafka, Spark, machine learning, graph analysis, and a React dashboard.

---

## Project Overview

Digital payment platforms process large volumes of transactions every second. Fraud detection systems need to identify suspicious activity quickly while keeping enough context for investigation.

We built this to:

* Stream payment transactions through Kafka
* Process transactions using Spark Structured Streaming
* Detect suspicious behavior using anomaly detection and fraud scoring
* Identify network-based fraud patterns such as rings, bursts, and risky clusters
* Present alerts, graph signals, and timeline activity through a React dashboard

---

## Authors

* **Mahimna Darji**
* **Hema Manasi Potnuru**

---

## Tech Stack

| Layer              | Tools Used                            |
| ------------------ | ------------------------------------- |
| Data Source        | Synthetic transaction dataset         |
| Streaming          | Apache Kafka                          |
| Stream Processing  | PySpark Structured Streaming          |
| Machine Learning   | Isolation Forest, XGBoost, LightGBM   |
| Graph Analysis     | NetworkX                              |
| Backend            | Python, FastAPI                       |
| Dashboard          | React, TypeScript, Tailwind, Recharts |
| Storage            | Parquet, CSV, model artifacts         |
| Development        | VS Code, Git, local environment       |

---

## Features Built

### Transaction Ingestion

* Read transactions from the prepared dataset
* Streamed records into Kafka topic using a Python producer
* Preserved transaction fields during ingestion
* Added controlled streaming rate for simulation

### Data Validation

* Added schema validation before sending records to Kafka
* Handled missing values and invalid records
* Standardized timestamp fields
* Logged skipped records without stopping the pipeline

### Streaming Feature Engineering

* Consumed Kafka transactions using Spark Structured Streaming
* Created rolling transaction frequency features
* Calculated user-level and merchant-level aggregates
* Enriched transactions with historical profile statistics

### Fraud Scoring

* Trained anomaly detection model using Isolation Forest
* Added supervised fraud scoring using XGBoost or LightGBM
* Generated fraud probability and anomaly score
* Assigned risk levels based on scoring thresholds

### Graph-Based Fraud Detection

* Built transaction entity graphs using users, accounts, and merchants
* Calculated PageRank-based node importance
* Detected communities and connected components
* Identified triangle patterns for circular activity
* Calculated propagated risk across connected nodes

### Investigation Dashboard

* Built a React dashboard for fraud monitoring
* Added Overview, Alerts, Graph Signals, and Timeline views
* Displayed fraud KPIs, alert tables, graph metrics, and pipeline activity
* Designed the interface as a compact fraud operations console

---

## Sample Dashboards

| Overview View                    | Alerts View                         |
| -------------------------------- | ----------------------------------- |
| KPI summary and alert patterns   | Alert queue and risk matrix         |
| Severity mix and alert reasons   | Transaction-level investigation     |

| Graph Signals View               | Timeline View                       |
| -------------------------------- | ----------------------------------- |
| Entity risk and cluster signals  | Streaming events and latency        |
| Propagated risk and graph metrics| Processing efficiency indicators    |

---

### Live Dashboard Link



---

## Insights Discovered

* Fraud activity often appears in bursts instead of spreading evenly over time
* High-risk users and merchants tend to form connected clusters
* Some transactions appear safe alone but become risky through network exposure
* Transaction velocity helps detect sudden abnormal activity
* Graph-based risk propagation adds useful context beyond transaction-level scoring

---

## Additional Analysis & Insights from Backend

### Network Risk Score

We created a graph-based risk score:

```text
Network Risk Score = Base Transaction Risk + Risk Influence from Connected Entities

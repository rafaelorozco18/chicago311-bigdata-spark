# Chicago 311 Service Requests — Big Data Analysis with PySpark

> **Academic Project · Universidad Externado de Colombia · Big Data 2026-I**  
> Allison M. Loango Rayo · Angie K. Loango Rayo · Rafael J. Orozco Luquez

---

## Overview

End-to-end distributed data engineering and machine learning pipeline built to analyze the City of Chicago's 311 Service Request system — one of the largest civic open datasets available publicly.

The project was contracted as a consulting simulation for the **City of Chicago — Department of Assets, Information and Services**, with the goal of identifying demand patterns, operational bottlenecks, and predicting late-resolution tickets across the city's 50 wards.

---

## Dataset

| Attribute | Value |
|---|---|
| Source | [Chicago Data Portal](https://data.cityofchicago.org/Service-Requests/311-Service-Requests/v6vf-nfxy) |
| Size | 4.9 GB (decompressed) |
| Records | 13.78 million |
| Columns | 39 |
| Coverage | December 2018 — April 2026 |

---

## Infrastructure

```
Cluster:  1 master node + 2 worker nodes (VirtualBox)
Hadoop:   3.4.1  —  HDFS with replication factor 2, 39 blocks × 128 MB
Spark:    3.5.7  —  deployed over YARN
```

**SparkSession configuration:**
```python
spark.executor.memory         = 3g
spark.executor.cores          = 2
spark.driver.memory           = 2g
spark.sql.shuffle.partitions  = 100
spark.sql.autoBroadcastJoinThreshold = 20 MB
spark.sql.adaptive.enabled    = true   # AQE active
```

**Execution summary:** `application_1779304263670_0002` · 793 tasks · 0 failures · ~59 min task time

---

## Tech Stack

`Python` · `PySpark` · `Apache Spark 3.5` · `Hadoop HDFS` · `YARN` · `MLlib` · `Jupyter Notebook` · `Matplotlib`

---

## Methodology

### 1. Data Ingestion
Dataset loaded into HDFS via `hdfs dfs -put` with replication factor 2. Block integrity verified with `hdfs fsck` — Status: **HEALTHY**, 39/39 blocks minimally replicated.

### 2. EDA (Distributed)
- Schema inspection, null analysis, and value distributions across all 39 columns
- Pearson correlation matrix — detected strong spatial redundancy: WARD↔LATITUDE (0.76), WARD↔POLICE_DISTRICT (0.67)
- Time series 2019–2026 with COVID-19 annotation and seasonal pattern identification
- Heatmap: request volume by hour × day of week

### 3. Data Cleaning
- Filtered `DUPLICATE = false`
- Parsed timestamps (`CREATED_DATE`, `CLOSED_DATE`)
- Derived features: `ANIO`, `MES`, `TIEMPO_RESPUESTA_HORAS`
- Excluded negative response times and outliers > 1 year
- **Result: 13,074,977 clean records** (5.13% reduction from original)

### 4. Complex Transformations

| # | Transformation | Technique | Key Metric |
|---|---|---|---|
| T1 | Broadcast join + multi-aggregation by SR_TYPE × WARD | `BroadcastHashJoin` (AQE-confirmed) | Stage 66: ~24s, 60.4 MiB shuffle write |
| T2 | Monthly ranking of top-5 services | `Window.partitionBy(ANIO, MES)` with RANK + DENSE_RANK | Stage 69: ~24s, disk spill ~80 MB |
| T3 | Bottleneck identification by ward | Lazy chain of 6 chained operations | Stage 60: ~17s |

### 5. Machine Learning (Bonus)
**Target:** Binary classification — will a request exceed the P75 response time for its own service type?

**Pipeline:**
```
StringIndexer → OneHotEncoder → VectorAssembler → RandomForestClassifier
(SR_TYPE, CATEGORIA, WARD, ORIGIN, CREATED_DAY_OF_WEEK)
80 trees · maxDepth=10 · maxBins=128
Training sample: 5% of dataset (~374K rows · 17.6% positive class)
```

---

## Results

### Model Performance
| Metric | Value |
|---|---|
| **AUC-ROC** | **0.8061** |
| Accuracy | 0.8246 |
| F1 (weighted) | 0.7453 |
| Precision (weighted) | 0.6800 |
| Recall (weighted) | 0.8246 |

### Feature Importance
| Feature | Importance |
|---|---|
| SR_TYPE | 42.5% |
| WARD | 25.6% |
| ORIGIN | 21.1% |
| CATEGORIA | 10.0% |
| Temporal (HOUR, DOW, MES) | ~1% each |

### Key Findings
- **Top 5 service types account for >63% of total volume** — classic long-tail distribution
- **311 INFORMATION ONLY CALL** alone represents 35.7% (4.9M requests)
- **97.1% overall closure rate**, but with structural non-random variance across wards
- **10 wards account for 26–31% of late responses** — Ward 34 leads chronically
- Clear **seasonal pattern**: peaks April–September, drops in winter months
- **COVID-19 visible** as a marked demand drop in 2020 with progressive recovery through 2023

### Operational Recommendations
1. Re-balance field crews using late-response rate by ward
2. Pilot the RF model score (AUC 0.81) to flag tickets with P(late) > 0.70 before SLA breach
3. Reinforce mobile/web channels (currently at 7.6% vs. 57.1% for phone)
4. Normalize columns with >50% null values to NULL for cleaner future analyses

---

## Project Structure

```
chicago311-bigdata-spark/
├── README.md
├── chicago311_bigdata.ipynb        # Main pipeline: ingestion, EDA, transformations, ML
├── EDA_profundo_chicago311.ipynb   # Deep exploratory analysis
├── reports/
│   ├── Informe_Consultoria.pdf     # Executive consulting report (1 page)
│   └── Anexos_Capturas_Importantes.pdf  # 29 annotated screenshots (HDFS, YARN, Spark UI)
└── requirements.txt
```

---

## How to Run

### Prerequisites
- Apache Hadoop 3.x (HDFS + YARN)
- Apache Spark 3.5.x
- Python 3.10+
- Jupyter Notebook

### Steps
```bash
# 1. Start the cluster
start-dfs.sh && start-yarn.sh && start-history-server.sh

# 2. Download dataset
pip install gdown
gdown 1bx70ng1IJxH-Srpyp1ebaLP5_aUcBi_v

# 3. Load to HDFS
hdfs dfs -mkdir -p /user/hadoop/chicago311
hdfs dfs -D dfs.replication=2 -put 311_Service_Requests.csv /user/hadoop/chicago311/

# 4. Verify integrity
hdfs fsck /user/hadoop/chicago311/311_Service_Requests.csv -files -blocks -locations

# 5. Remove local copy (HDFS-only workflow)
rm 311_Service_Requests.csv

# 6. Launch notebook
jupyter notebook
```

---

## References

- Apache Software Foundation. (2024). *Apache Spark 3.5 Documentation — SQL Performance Tuning.* https://spark.apache.org/docs/latest/sql-performance-tuning.html
- City of Chicago. (2026). *311 Service Requests Dataset.* Chicago Data Portal. https://data.cityofchicago.org/Service-Requests/311-Service-Requests/v6vf-nfxy
- Zaharia, M., et al. (2016). Apache Spark: A unified engine for big data processing. *Communications of the ACM, 59*(11), 56–65. https://doi.org/10.1145/2934664

---

*Universidad Externado de Colombia · Departamento de Matemáticas — Programa de Ciencia de Datos · 2026*


# E-Commerce Big Data Analytics Project

## Project Overview
This project implements a **polyglot Big Data Analytics architecture** for an e-commerce platform.  
It integrates **MongoDB**, **Apache HBase**, and **Apache Spark** to store, process, and analyze large-scale transactional and session-level data.

The goal of the project is to demonstrate how multiple Big Data technologies can be combined to extract meaningful business insights such as customer behavior, conversion performance, revenue trends, and Customer Lifetime Value (CLV).

The project was developed as part of the **Big Data Analytics final examination** and focuses on **batch-oriented analytics** using synthetic e-commerce datasets.

---

## Technologies Used
- MongoDB – Document-based storage for products and transactions  
- Apache HBase – Wide-column storage for high-volume session and behavioral data  
- Apache Spark – Distributed batch processing and analytics  
- Python – Data ingestion, analysis, and visualization  
- Docker & Docker Compose – Service orchestration and reproducibility  
- Matplotlib / Pandas – Visualization and data manipulation  

---

## Project Architecture
The system follows a layered architecture:

1. Data Generation Layer – Synthetic e-commerce data generation  
2. Data Storage Layer – MongoDB (transactions) and HBase (sessions)  
3. Data Processing Layer – Apache Spark batch analytics  
4. Visualization & Insights Layer – Charts and summary tables  

---

## Folder Structure
E_Commerce_Final_Project/  
├── .venv/  
├── analysis/  
├── data_raw/  
├── outputs/  
│   ├── analysis_plots/  
│   ├── analysis_tables/  
│   ├── batch_sessions/  
│   ├── final_integrated_outputs/  
│   ├── hbase_exports/  
│   └── spark_sql_analytics/  
├── Python_script/  
├── docker-compose.yml  
├── requirements.txt  
├── Final_project_report_101032.pdf  
└── README.md  

---

## Datasets
Synthetic datasets are stored in the `data_raw/` directory:
- users.json  
- products.json  
- transactions.json  
- sessions_0.json  
- categories.json  

---

## Environment Setup

### Install Python Dependencies
pip install -r requirements.txt

### Start Services Using Docker
docker compose up -d

---

## Data Ingestion
python analysis/import_sessions_to_hbase.py

---

## Spark Batch Analytics
spark-submit Python_script/spark_sql_analytics.py

Outputs are saved under:
outputs/spark_sql_analytics/

---

## Analytical Scripts
Located in the `analysis/` directory:
- clv_analysis.py  
- hbase_conversion_funnel.py  
- hbase_conversion_referrer_analysis.py  
- order_status_plot.py  
- visualizations.py  

Run using:
python analysis/script_name.py


---

## Outputs and Results
Plots: outputs/analysis_plots/  
Tables: outputs/analysis_tables/  

---

## Key Business Insights
- High drop-off during checkout  
- Revenue concentrated among few customers  
- Completed orders dominate revenue  
- Email, affiliate, and search channels perform best  

---

## Scalability Considerations
- MongoDB: flexible schema and sharding  
- HBase: horizontal scalability  
- Spark: distributed analytics  
- Docker: reproducibility  

---

## Limitations
- Local hardware constraints  
- Batch-only analytics  
- Synthetic data  

---

## Future Enhancements
- Real-time streaming analytics  
- Predictive modeling  
- Cloud deployment  

---

## Author
MPANO Rudakenga Rongin  
Student ID: 101032  

---

## License
Academic and educational use only.

---


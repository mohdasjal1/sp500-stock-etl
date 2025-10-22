# 🧩 S&P 500 Stock Data Pipeline  

An **end-to-end data engineering pipeline** that automates the extraction, transformation, and loading (ETL) of S&P 500 company and stock data — orchestrated with **Apache Airflow**, stored in **Amazon S3**, and loaded into **Snowflake** for analysis.  

---

## 🚀 Project Overview  

This pipeline performs the following steps automatically:  
1. **Extracts** the latest list of S&P 500 companies from **Wikipedia**.  
2. **Fetches** their most recent stock prices using the **Yahoo Finance API**.  
3. **Stores** the raw data in **Amazon S3 (Data Lake)**.  
4. **Transforms and Loads** the cleaned dataset into **Snowflake (Data Warehouse)**.  
5. **Orchestrates** all tasks seamlessly using **Apache Airflow**.  

The result is a fully automated workflow that keeps both the S&P 500 company data and stock prices centralized and ready for analysis.  

---

## 🧱 Architecture  

**Tools & Technologies Used**  
- **Apache Airflow** – Workflow orchestration  
- **Python** – Data extraction & transformation  
- **Amazon S3** – Data lake for raw data storage  
- **Snowflake** – Cloud data warehouse for transformed data  
- **Wikipedia & Yahoo Finance** – Data sources  

**High-Level Flow:**  
```
Wikipedia + Yahoo Finance → Airflow → Amazon S3 (Raw Data)
                                   ↓
                              Snowflake (Transformed Data)
```

---

## 💡 Key Features  

- Automated daily data ingestion  
- Separate **data lake** (S3) and **data warehouse** (Snowflake) layers  
- Modular and scalable design  
- Uses real-world data sources (Wikipedia, Yahoo Finance)  
- Easy to schedule and monitor via Airflow UI  

---

## 🧩 Project Structure  

```
├── dags/                  # Airflow DAGs
├── scripts/               # Python scripts for extraction & transformation
├── requirements.txt       # Dependencies
├── README.md              # Project documentation
```

---

## ⚙️ How It Works  

1. Airflow triggers the DAG on schedule.  
2. A Python script extracts the list of S&P 500 companies from Wikipedia.  
3. Another script fetches stock price data via Yahoo Finance.  
4. Data is saved directly to Amazon S3.  
5. Airflow then triggers a load job that moves cleaned data from S3 into Snowflake.  

---

## 🌐 Use Cases  

- Building a **data pipeline portfolio project**  
- Demonstrating **ETL and orchestration** concepts  
- Practicing **Snowflake, S3, and Airflow integration**  
- Serving as a foundation for **real-time stock data analytics**  

---

## 📈 Future Improvements  

- Add **data validation and quality checks**  
- Integrate with **dbt for transformations**  
- Visualize data using **Tableau** or **Power BI**  
- Extend to other stock indices (e.g., NASDAQ 100)  

---

## 🧠 Learning Outcomes  

Through this project, you’ll gain hands-on experience with:  
- Building and scheduling pipelines in Airflow  
- Using AWS S3 and Snowflake for data storage and warehousing  
- Automating ETL processes  
- Handling real-world data sources programmatically  

---

## 🏷️ Tags  

`#DataEngineering` `#ETL` `#Airflow` `#Snowflake` `#S3` `#Python` `#DataPipeline`  

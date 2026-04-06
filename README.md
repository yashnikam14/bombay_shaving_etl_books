#  ETL Pipeline with Web Scraping

##  Overview

This project is a simple ETL (Extract, Transform, Load) pipeline.

* **Extract**: Scrapes book data from a website
* **Transform**: Cleans and formats the data
* **Load**: Stores the data in a MySQL database

---

##  What I Built

* Scraped book data using Playwright
* Cleaned data using Pandas
* Stored data in MySQL
* Created an Airflow DAG to automate the pipeline

---

##  How to Run

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Install Playwright browsers

```bash
playwright install
```

### 3. Setup MySQL

Create database:

```sql
CREATE DATABASE bombay_shavings;
```

Update DB config in `load/book_load.py`

---

### 4. Run pipeline

```bash
python main.py
```

---

##  Airflow (Optional)

The pipeline is also implemented using Airflow with 3 tasks:

```text
scrape → transform → load
```

Due to Windows limitations, Airflow may require Docker/Linux to run.

---

##  Data Collected

* Title
* Price
* Rating
* Availability
* Category
* Description

---

##  Challenges Faced

* Pagination issue → fixed navigation logic
* Relative URL issue → used proper URL handling
* Async errors → fixed with correct `await` usage
* Airflow on Windows → suggested Docker

---

##  Decisions

* Used Playwright for reliable scraping
* Used Pandas for easy data cleaning
* Used MySQL for structured storage
* Added logging and error handling

---

##  Output

* ~1000 records stored in database
* Logs generated for tracking

---

##  Summary

This project demonstrates a complete ETL pipeline with scraping, data cleaning, database storage, and workflow automation.

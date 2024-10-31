# Transaction Data Pipeline

## Overview
This project implements a data pipeline that processes transaction data from an API and loads it into a PostgreSQL database. The pipeline handles data extraction, cleaning, enrichment, and database loading operations.

## Key Features
- API data extraction with error handling
- Data cleaning and validation
- Transaction amount categorization
- Bulk database operations
- Error recovery mechanisms


## Approach Explained

This setup is built to handle data with care and prevent errors. Each part of the process extracting, cleaning, and loading data is split into separate functions, which makes the code easy to manage and test. The data cleaning steps are detailed: fixing missing information, standardizing dates, removing duplicates, and filtering out negative transactions. The enrichment stage adds extra value by grouping transactions and calculating totals for each customer, aiming to meet common business needs.

Key improvements include using pandas for quick data handling, grouping database updates to minimize processing time, and protecting data accuracy with transaction control and rollback options. Some compromises were made, like using synchronous processing instead of asynchronous for simplicity, and holding all data in memory rather than streaming it. While this may not work well for extremely large datasets, it balances effectiveness, stability, and ease of development. The use of print statements and error logs helps with tracking issues and keeping the system running smoothly..


## Theory Answers

1. To make this solution work with 10 or even 100 times more data, we would need to improve its efficiency and reduce wait times. One way to do this is by using parallel processing tools like multiprocessing which would let us handle different parts of the data at the same time, making the process faster. Another helpful change would be to use asynchronous processing, which allows tasks that don’t depend on each other to run at the same time, reducing waiting time. Switching to stream processing with tools like Kafka or Spark would also help, as it lets us handle data in smaller, real time chunks rather than all at once in memory. Also using a distributed database, like a NoSQL or cloud database, would make it easier to store and access large amounts of data quickly.


2.  Adding a timestamp or version to each transaction would help with tracking the latest changes. To handle backdated transactions, we could adjust the loading process so it catches updates even for older records. Running a data reconciliation process, which regularly checks and updates records between the source data and the database. This can help with keeping the data accurate.

  
3. If the company is using this data for the first time in a format that works for analytics or automation, the most important thing to create would be a clear transaction history for each customer. This would help the company understand customer behaviour, find out which customers are the most frequent or valuable, and detect issues like frequent declines or unusual patterns. The company could use the data for targeted marketing, improving customer support, and making better business decisions based on customer trends.


## Project Structure

- `main.py` - Main application entry point  
- `sqlscripts.sql` - SQL scripts for database operations


## Dependencies

### Required Python packages:
- `requests` - For API calls
- `pandas` - For data manipulation and analysis
- `psycopg2` - For PostgreSQL database operations
- `python-dotenv` - For environment variable management

Install dependencies using:
```bash

pip install requests pandas psycopg2-binary python-dotenv




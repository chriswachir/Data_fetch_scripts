# Data Transfer Scripts

This repository contains two Python scripts designed to facilitate data transfer between various data sources, including Redshift, MySQL, and S3.

## 1. `redshift-redshift.py`

### Overview
This script connects to a Redshift database, deletes existing data from a specified target table for a specific date, and then inserts new data based on a date range. Notifications are sent via email and Slack if any errors occur during the process.

### Features
- **Connects to Redshift**: Establishes a connection to the specified Redshift database using configuration details.
- **Error Notification**: Sends email and Slack alerts if an error occurs during the data insertion process.

### Requirements
- Python 3.x
- Libraries: `psycopg2`, `smtplib`, `email`, `requests`, `datetime`, `configparser`

### Configuration
- **Email Configuration**: Update the `r_emailConfig.ini` file in the `/config` directory with the SMTP server details.
- **Redshift Configuration**: Update the `r_validation.ini` file in the `/config` directory with the connection details for Redshift.

Here’s a straightforward README focusing solely on what the script does:

---

---

## 2. `mysql-s3-redshift.py`

### Overview
This script connects to a MySQL database, retrieves data for the last day, uploads it to an S3 bucket as a CSV file, and then loads that CSV into a specified Redshift table. It also sends error notifications via email and Slack.

### Features
- Fetches data from a MySQL database for the last day.
- Saves the fetched data as a CSV file and uploads it to S3.
- Uses the Redshift COPY command to load the data from the S3 bucket into a Redshift table.
- Sends notifications in case of failures.

### Requirements
- Python 3.x
- Libraries: `pymysql`, `psycopg2`, `boto3`, `csv`, `smtplib`, `email`, `requests`, `datetime`, `configparser`

### Configuration
- **AWS S3 Configuration**: Update the `r_validation.ini` file in the `/config` directory with your AWS S3 credentials and bucket information.
- **Email Configuration**: Similar to the first script, update the `r_emailConfig.ini` file in the `/config` directory.


---

## Notes
- Make sure to install the required libraries before running the scripts.
- Ensure that the configuration files are correctly set up to avoid connection issues.

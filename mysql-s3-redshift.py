import pymysql
import psycopg2
import datetime
from datetime import timedelta
from configparser import ConfigParser
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import boto3
import os
import csv
import requests

# set the target table
target = schema.table

# Function to read server configuration
def server_config(filename, section):
    parser = ConfigParser()
    parser.read(filename)
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        print(f'Warning: Section {section} not found in the {filename} file')
        return None  # Return None if section is not found
    return config


# Function to setup email server
def setup_email():
    email_config = server_config('/config/r_emailConfig.ini', 'email_config')
    smtp_host = email_config['smtp_host']
    smtp_port = int(email_config['smtp_port'])
    smtp_username = email_config['smtp_username']
    smtp_password = email_config['smtp_password']
    fromaddr = email_config['sender_email']
    toaddr = 'christopher.wachira@cellulant.io'
    return smtp_host, smtp_port, smtp_username, smtp_password, fromaddr, toaddr


# Function to send notifications to Slack
def send_slack_notification(message):
    slack_config = server_config('/config/r_emailConfig.ini', 'email_config')
    slack_webhook_url = slack_config['slack_webhook_url']
    payload = {'text': message}
    requests.post(slack_webhook_url, json=payload)


# Function to send notifications
def send_notification(error_message):
    smtp_host, smtp_port, smtp_username, smtp_password, fromaddr, toaddr = setup_email()
    today = str(datetime.datetime.now())
    subject = "Data Lake Data Fetch Failure Alert"
    msg = MIMEMultipart("alternative")
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = f"{subject} {today}"

    body = (
        f"Data fetch failed. Error: {error_message}\n"
        f"Source: {source}\n"
        f"Timestamp: {today}\n"
    )
    msg.attach(MIMEText(body, 'plain'))

    with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
        server.login(smtp_username, smtp_password)
        server.sendmail(fromaddr, toaddr, msg.as_string())
    send_slack_notification(body)  # Send notification to Slack


# Function to fetch and insert data
def fetch_insert_lake_data(**kwargs):
    mysql_conn = None
    redshift_conn = None

    try:
        # Get S3 details
        s3_details = server_config('/config/r_validation.ini', 'aws_s3')
        aws_access_key_id = s3_details['aws_access_key_id']
        aws_secret_access_key = s3_details['aws_secret_access_key']
        s3_bucket = s3_details['bucket']

        # Create an S3 client using the AWS credentials directly
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        mysql_details = server_config('/config/r_validation.ini', 'table1')
        if mysql_details is None:
            raise Exception("MySQL configuration not found.")
        mysql_host = mysql_details["host"]
        mysql_dbname = mysql_details["database"]
        mysql_user = mysql_details["user"]
        mysql_password = mysql_details["password"]
        mysql_port = int(mysql_details["port"])

        redshift_details = server_config('/config/r_validation.ini', 'redshift')
        redshift_host = redshift_details["host"]
        redshift_dbname = redshift_details["database"]
        redshift_user = redshift_details["user"]
        redshift_password = redshift_details["password"]
        redshift_port = int(redshift_details["port"])

        mysql_conn = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_dbname,
            port=mysql_port
        )
        redshift_conn = psycopg2.connect(
            dbname=redshift_dbname,
            user=redshift_user,
            password=redshift_password,
            host=redshift_host,
            port=redshift_port
        )

        mysql_cur = mysql_conn.cursor()
        redshift_cur = redshift_conn.cursor()

        today = datetime.datetime.now()
        yesterday = today - timedelta(days=1)
        today_str = today.strftime("%Y-%m-%d")

        # Example SQL query
        q_fetch_data = """
            SELECT 
                COLUMN1,COLUMN2,COLUMN3,....
            FROM DB.TABLE
            WHERE DATECREATED >= %s AND DATECREATED < %s
        """
        
        mysql_cur.execute(q_fetch_data, (yesterday.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")))
        results = mysql_cur.fetchall()
        print(f"Fetched {len(results)} rows.")

        csv_file_path = f"{source}.csv"

      # Upload to s3

        with open(csv_file_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["pull_date", "source"] + [desc[0] for desc in mysql_cur.description])
            for data in results:
                data_values = (
                    today_str, 
                    source, 
                    *[int(d) if isinstance(d, float) and d.is_integer() else (int(d) if isinstance(d, (int, float)) else 'null' if d is None else d) for d in data]
                )
                writer.writerow(data_values)

        # Check if the file exists before uploading
        if os.path.exists(csv_file_path):
            print(f"File exists: {csv_file_path}")
        else:
            raise FileNotFoundError(f"File not found: {csv_file_path}")

        # Upload CSV to S3
        print(f"Uploading file to S3: {csv_file_path}")
        s3_client.upload_file(csv_file_path, s3_bucket, f"{source}/{os.path.basename(csv_file_path)}")
        print("Upload successful.")

        # Use COPY command to load data into Redshift
        copy_query = f"""
        COPY {target} 
        FROM 's3://{s3_bucket}/{source}/{os.path.basename(csv_file_path)}'
        CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
        DELIMITER ','  
        CSV
        IGNOREHEADER 1
        NULL AS 'null'
        DATEFORMAT 'auto'
        TIMEFORMAT 'auto'
        IGNOREBLANKLINES
        ACCEPTINVCHARS
        EMPTYASNULL
        """
        redshift_cur.execute(copy_query)
        redshift_conn.commit()
        print("Data loaded into Redshift successfully.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        send_notification(str(e))  # Send email and Slack notification

    finally:
        # Clean up database connections
        if mysql_cur:
            mysql_cur.close()
        if mysql_conn:
            mysql_conn.close()
        if redshift_cur:
            redshift_cur.close()
        if redshift_conn:
            redshift_conn.close()

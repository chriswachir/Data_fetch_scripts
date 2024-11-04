import datetime
from datetime import timedelta
import psycopg2 as pg
from configparser import ConfigParser
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests

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
        raise Exception(f'Section {section} not found in the {filename} file')
    return config

# Email server login parameters
email_config = server_config('/config/r_emailConfig.ini', 'email_config')
smtp_host = email_config['smtp_host']
smtp_port = int(email_config['smtp_port'])
smtp_username = email_config['smtp_username']
smtp_password = email_config['smtp_password']
fromaddr = email_config['sender_email']
toaddr = email_config['reciever_email']
slack_webhook_url = email_config['slack_webhook_url']

# Define target table
target = 'schema.table'

# Function to insert data into Redshift
def insert_lake_data():
    config = server_config('/config/r_validation.ini', 'redshift')

    try:
        # Connect to the Redshift database
        with pg.connect(**config) as conn:
            with conn.cursor() as cur:
                today = datetime.datetime.now()
                yesterday = today - timedelta(days=1)
                today_str = today.strftime("%Y-%m-%d")

                # Define the data start and end points
                data_start_point = yesterday.strftime("%Y-%m-%d %H:%M:%S")
                data_end_point = today.strftime("%Y-%m-%d %H:%M:%S")

                # Direct INSERT INTO SELECT query to load data from source into target
                insert_query = f"""
                INSERT INTO {target} (
                    column1, column2, ....
                )
                SELECT
                  columns
                FROM schema.table 
                WHERE datecreated >= '{data_start_point}'
                AND dateCreated < '{data_end_point}';
                """

                # Execute the direct insert query
                cur.execute(insert_query)
                conn.commit()
                print(f"Data inserted successfully from {data_start_point} to {data_end_point}")

    except Exception as e:
        send_notification(f"Failed to insert data: {str(e)}", slack_webhook_url)
        raise

# Function to send email and Slack alerts
def send_notification(message, webhook_url):
    try:
        # Send email alert
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        msg['To'] = toaddr
        msg['Subject'] = "Data Lake Insert Error"
        body = message
        msg.attach(MIMEText(body, 'plain'))
        
        # Establish SMTP connection and send email
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
        
        # Send Slack alert
        slack_data = {'text': message}
        requests.post(webhook_url, json=slack_data)

    except Exception as e:
        print(f"Failed to send notification: {str(e)}")

if __name__ == "__main__":
    insert_lake_data()

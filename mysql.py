#gets for every table items created a week ago max looking for page_modified, page_created, last_login, pgvw_datetime.

import pymysql
import boto3
import csv
import pandas as pd
from datetime import datetime, timedelta
import json

#connect to mysql
db = pymysql.connect(
    host='prime-data-analysis-1.cluster-cfbdgckvdfcz.eu-west-2.rds.amazonaws.com',
    user='admin',
    password='jXO3kKAqpuqxAwYyo4b5',
    db='mindtoolsManagement'
)
cursor = db.cursor()

table_names = ['page_views_corp_archive', 'page_views', 'corporate_members']
one_week_ago = (datetime.now() - timedelta(weeks=1)).strftime('%Y-%m-%d')

for table_name in table_names:
    cursor.execute(f"DESCRIBE {table_name}")
    schema = cursor.fetchall()
    
        # Check if pgvw_datetime column exists
    if any('pgvw_datetime' in col for col in schema):
        where_clause = f"pgvw_datetime >= '{one_week_ago}'"

    # Check if last_login column exists
    elif any('last_login' in col for col in schema):
        where_clause = f"last_login >= '{one_week_ago}'"

    # Check if page_modified and page_added columns exist
    elif all(('page_modified' in col for col in schema), ('page_added' in col for col in schema)):
        where_clause = f"page_modified >= '{one_week_ago}' AND page_added >= '{one_week_ago}'"

    # If none of the columns exist, raise an error
    else:
        raise ValueError("None of the specified columns exist in the table")

    cursor.execute(f"SELECT * FROM {table_name} WHERE {where_clause}")

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    # Fetch data and save as CSV file
    path = f"{table_name}.csv"
    with open(path, "w", encoding="utf-8", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(columns)
        for row in rows:
            csvwriter.writerow(row)

    # Upload file to S3 bucket
    s3 = boto3.resource('s3')
    bucket_name = '322104163088-mysql-ingestion-bucket'

    # getting year, month, day to name folders
    yesterday = datetime.now() - timedelta(days=1)
    year = datetime.strftime(yesterday, '%Y')
    month = datetime.strftime(yesterday, '%m')
    day = datetime.strftime(yesterday, '%d')

    # getting folder name
    file_name = path.split('.')[0]

    with open(path, 'r') as csvfile:
        csv_data = csv.reader(csvfile)
        csv_file = '\n'.join([','.join(row) for row in csv_data]).encode('utf-8')

    # put file into s3 bucket
    s3.Object(bucket_name, f'{file_name}/{year}/{month}/{day}/data.csv').put(Body=csv_file)

    print(f"Data from {table_name} has been successfully processed and uploaded to S3.")

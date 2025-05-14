import boto3
import pymysql
import os
from botocore.exceptions import ClientError


NAMESPACE = "CustomRDS"
DIMENSIONS = [{'Name': 'DBInstanceIdentifier', 'Value': 'my-db'}]

TOTAL_MEMORY_BYTES = 1 * 1024 * 1024 * 1024
TOTAL_STORAGE_BYTES = 20 * 1024 * 1024 * 1024

def get_ssm_parameter(name):
    ssm = boto3.client('ssm')
    try:
        response = ssm.get_parameter(Name=name, WithDecryption=True)
        return response['Parameter']['Value']
    except ClientError as e:
        print(f"Error retrieving {name}: {e}")
        raise

def lambda_handler(event, context):
    # the name of your ssm parameter store may vary
    host = get_ssm_parameter('/testv2/db-url')
    user = get_ssm_parameter('/testv2/db-username')
    password = get_ssm_parameter('/testv2/db-password')
    dbname = get_ssm_parameter('/testv2/db-name')

    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            port=3306,
            connect_timeout=10
        )
        with connection.cursor() as cursor:
            # Free storage (approximate via Data_length + Index_length)
            cursor.execute("""
                SELECT table_schema,
                       SUM(data_length + index_length) AS used_bytes
                FROM information_schema.tables
                GROUP BY table_schema;
            """)
          
            rows = cursor.fetchall()
            used_storage_bytes = sum(row[1] for row in rows if row[1])

            free_storage_bytes = TOTAL_STORAGE_BYTES - used_storage_bytes
            free_storage_percent = (free_storage_bytes / TOTAL_STORAGE_BYTES) * 100

            # Approximate free memory (using InnoDB buffer pool for example)
            cursor.execute("SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_pages_free';")
            free_pages = int(cursor.fetchone()[1])

            cursor.execute("SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_pages_total';")
            total_pages = int(cursor.fetchone()[1])

            free_memory_percent = (free_pages / total_pages) * 100 if total_pages > 0 else 0

            print("Free Memory Percent: " + str(round(free_memory_percent, 2)) + "%")
            print("Free Storage Percent: " + str(round(free_storage_percent, 2)) + "%")
            
          # Push to CloudWatch
            cloudwatch = boto3.client('cloudwatch')
            cloudwatch.put_metric_data(
                Namespace=NAMESPACE,
                MetricData=[
                    {
                        'MetricName': 'FreeableMemoryPercent',
                        'Dimensions': DIMENSIONS,
                        'Unit': 'Percent',
                        'Value': round(free_memory_percent, 2)
                    },
                    {
                        'MetricName': 'FreeableStoragePercent',
                        'Dimensions': DIMENSIONS,
                        'Unit': 'Percent',
                        'Value': round(free_storage_percent, 2)
                    }
                ]
            )
            print("Metrics successfully pushed.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'connection' in locals():
            connection.close()

import requests
import boto3
import os  # add import
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook


def get_public_ip():
    response = requests.get('https://api.ipify.org?format=json', timeout=10)
    response.raise_for_status()
    ip = response.json()['ip']
    return ip

def update_route53_dns(**context):
    ip = context['ti'].xcom_pull(task_ids='get_public_ip')
    hosted_zone_id = 'Z072222327445DTYH7COP'
    record_name = 'gateway.dragon-den.com.'
    record_type = 'A'
    ttl = 300

    ip_file = '/tmp/current_public_ip.txt'
    previous_ip = None

    # Get AWS credentials from Airflow connection
    conn = BaseHook.get_connection('AWS-Route53')
    aws_access_key_id = conn.login
    aws_secret_access_key = conn.password
    region_name = conn.extra_dejson.get('region_name', 'us-east-1')

    client = boto3.client(
        'route53',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    # Get current record from AWS
    response = client.list_resource_record_sets(
        HostedZoneId=hosted_zone_id,
        StartRecordName=record_name,
        StartRecordType=record_type,
        MaxItems='1'
    )
    record_sets = response.get('ResourceRecordSets', [])
    current_ip = None
  

    # If local file doesn't exist, create it with current_ip from AWS
    if not os.path.exists(ip_file):
        if record_sets and record_sets[0]['Name'] == record_name and record_sets[0]['Type'] == record_type:
            resource_records = record_sets[0].get('ResourceRecords', [])
            if resource_records:
                current_ip = resource_records[0]['Value']
        print(f"Generating file ({ip_file}) with IP ({current_ip})")
        with open(ip_file, 'w') as f:
            f.write(current_ip if current_ip else '')
        previous_ip = current_ip
    else:
        print(f"File exists ({ip_file}) - retrieving stored IP")
        with open(ip_file, 'r') as f:
            previous_ip = f.read().strip()

    if ip == previous_ip:
        print(f"No update needed. Current IP ({ip}) matches stored IP ({previous_ip}).")
        return

    # Only update if IP has changed in Route53
    if ip != current_ip:
        client.change_resource_record_sets(
            HostedZoneId=hosted_zone_id,
            ChangeBatch={
                'Comment': 'Update VPN IP from Airflow DAG',
                'Changes': [
                    {
                        'Action': 'UPSERT',
                        'ResourceRecordSet': {
                            'Name': record_name,
                            'Type': record_type,
                            'TTL': ttl,
                            'ResourceRecords': [{'Value': ip}]
                        }
                    }
                ]
            }
        )
        print(f"Route53 record updated to IP {ip}.")
    else:
        print(f"No Route53 update needed. Current record IP ({current_ip}) matches public IP ({ip}).")

    # Store the new IP locally
    with open(ip_file, 'w') as f:
        f.write(ip)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='external_vpn_update',
    default_args=default_args,
    description='Update Route53 DNS with current public IP',
    schedule='*/5 * * * *',  # changed to every 30 minutes
    start_date=datetime(2025, 10, 7),
    catchup=False,
    tags=['vpn', 'route53', 'network'],
) as dag:
    get_public_ip_task = PythonOperator(
        task_id='get_public_ip',
        python_callable=get_public_ip,
    )

    update_route53_dns_task = PythonOperator(
        task_id='update_route53_dns',
        python_callable=update_route53_dns,
    )

    get_public_ip_task >> update_route53_dns_task
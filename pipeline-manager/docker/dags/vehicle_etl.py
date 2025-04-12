
# Generated file - Do not edit manually
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
import datetime

# Define default arguments
default_args = {
    'owner': 'Data Engineering',
    'start_date': datetime.datetime.fromisoformat('2025-04-08'),
    'retries': 0,
    'retry_delay': datetime.timedelta(seconds=300)
}

# Create the DAG
dag = DAG(
    'vehicle_etl',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False
)

# Define tasks

start = DummyOperator(
    task_id='start',
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""docker exec spark-runner spark-submit --class com.insite.etl.common.ETLRunner --master local[*] --conf spark.sql.catalogImplementation=hive --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 /app/jars/vehicle-etl.jar com.insite.etl.vehicle.VehicleETL partition-columns=date,hour base-path=/tmp/vehicle-data target-table=vehicle_datalake.processed_vehicles version=v001""",
    dag=dag
)

finish = DummyOperator(
    task_id='finish',
    dag=dag
)

# Set task dependencies
start >> transform_data
transform_data >> finish

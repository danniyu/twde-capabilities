from airflow import utils
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import date, timedelta

today = datetime.date.today().strftime("%Y%m%d")
source = '/Users/Thoughtworks/workspace/DataEng/Sample_Data/sample_citybikes.csv'
target = '/Users/Thoughtworks/workspace/DataEng/twde-capabilities/data/raw/bikeshare'
config_jar = 'config-1.3.2.jar'
app_jar = 'target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar'

default_args = {
    'owner': 'danni',
    'depends_on_past': False,
    'start_date': today,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

command = ('spark_submit --jars ' + config_jar
    + '--class com.thoughtworks.ca.de.batch.ingest.DailyDriver ' + app_jar
    + ' ' + source + ' ' + target)

dag = DAG('sparkIngest', default_args=default_args, schedule_interval=timedelta(minutes=60))

ingest_citibikes = BashOperator(
    task_id='inject-citibikes',
    bash_command=command,
    dag=dag)
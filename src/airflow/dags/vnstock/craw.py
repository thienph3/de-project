import airflow

import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago



default_args = {
    'owner': 'abc'
    }

dag_bash = DAG(
	dag_id='crawling_vnstock',
	default_args=args,
	#schedule_interval='0 0 * * *',
	schedule_interval='@once',
	start_date=days_ago(1),
	dagrun_timeout=timedelta(minutes=60),
	description = 'Creating text file by executing the ShellCommand',
)

task_2 = DummyOperator(task_id='testingdag', dag=dag_bash)
 

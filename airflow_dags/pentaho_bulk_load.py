# -*- coding: utf-8 -*-
"""Example usage"""

from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow_pentaho.operators.kettle import KitchenOperator

import airflow.macros

#from airflow_pentaho.operators.kettle import PanOperator
#from airflow_pentaho.operators.carte import CarteJobOperator
#from airflow_pentaho.operators.carte import CarteTransOperator

DAG_NAME = 'pentaho_bulk_load'
DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['a.dolnik@mellivorasoft.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
    'email_on_retry': False
}

default_db_host = Variable.get("DEFAULT_DB_HOST")

with DAG(dag_id=DAG_NAME,
         default_args=DEFAULT_ARGS,
         dagrun_timeout=timedelta(hours=2),
         schedule_interval='30 0 * * *') as dag:

    # [START check default_db_host]
    run_default_db_host = BashOperator(
        task_id='check_default_db_host',
        bash_command="echo 'default_db_host={{ params.def_db_host}}' '{{ ds }}'",
        params={"def_db_host":default_db_host}
    )
    # [END check default_db_host]


    job1 = KitchenOperator(
        #pdi_conn_id='pdi_default',
        dag=dag,
        task_id='pentaho_job',
        #queue="pdi",
        directory='/opt/airflow/pentaho_scripts',
        job='main_test',
        xcom_push=True,
        file='/opt/airflow/pentaho_scripts/main.kjb',
        params={"date": "'{{ds}}'",
            'empty_param':'', 
            'DB_HOST':default_db_host, 
            'DB_NAME' : Variable.get("DEFAULT_DB_NAME"),
            'DB_PWD' :  Variable.get("DEFAULT_DB_PWD"),
            'DB_USER' :  Variable.get("DEFAULT_DB_USER"),
            'default_path' : Variable.get("DEFAULT_PMPLAN_MEDIA_PATH")})
        
    run_default_db_host >> job1

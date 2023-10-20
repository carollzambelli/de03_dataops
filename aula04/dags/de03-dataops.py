from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, date, timedelta
import pandas as pd
import sys

sys.path.append('/opt/airflow/dags/scripts')
from pipeline import ingestion, preparation_work
from utils import sw_work_to_dw
from config import configs_work, configs_dw, id_file


def _t0(ti):
    id = pd.read_csv(id_file)['id'].max()
    ti.xcom_push(key='last_id', value=int(id))

def _t1(ti):
    payload = ingestion(ti.xcom_pull(key='last_id', task_ids='t0'))
    ti.xcom_push(key='payload', value=payload)

def _t2(ti):
    preparation_work(ti.xcom_pull(key='payload', task_ids='t1'))

def _t3(ti):
    sw_work_to_dw(configs_work, configs_dw)
    last_id = ti.xcom_pull(key='last_id', task_ids='t0')+1
    pd.DataFrame([last_id, datetime.now()]).T.to_csv(
        id_file, index=False, mode='a', header=False)
    

with DAG(
    "de03-dataops",
    start_date=datetime(2023, 10, 10), 
    schedule_interval=timedelta(minutes=2),
    catchup=False) as dag:

    t0= PythonOperator(
        task_id='t0',
        python_callable=_t0,
        email_on_failure = True,
        email = 'carolina.kamada@faculdadeimpacta.com.br'
    )

    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )

    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )

    t3 = PythonOperator(
        task_id='t3',
        python_callable=_t3
    )


t0 >> t1 >> t2 >> t3
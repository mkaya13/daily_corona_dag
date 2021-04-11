from datetime import datetime
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator # 1

from airflow.providers.http.sensors.http import HttpSensor # 2

from airflow.providers.http.operators.http import SimpleHttpOperator # 3

from airflow.operators.python import PythonOperator # 4

from airflow.sensors.filesystem import FileSensor

from airflow.operators.bash import BashOperator # 6

import json
from pandas import json_normalize

default_args = {

'start_date': datetime(2021,1,1)

}

def _processing_table(ti):
    datas = ti.xcom_pull(task_ids = ['extracting_data'])
    if not len(datas):
        raise ValueError('data is empty')
    data = datas[0]['Global']

    processed_data = json_normalize({
        'newconfirmed': data['NewConfirmed'],
        'totalconfirmed': data['TotalConfirmed'],
        'newdeaths': data['NewDeaths'],
        'totaldeaths': data['TotalDeaths'],
        'newrecovered': data['NewRecovered'],
        'totalrecovered': data['TotalRecovered'],
        'date': data['Date']
    })
    processed_data.to_csv('/tmp/corona.csv', index = None, header = False)



with DAG('daily_corona_dag', default_args=default_args,schedule_interval='@daily',catchup=False) as dag:


    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id = 'corona_api',
        endpoint = 'summary'
    )

    extracting_data = SimpleHttpOperator(
        task_id = 'extracting_data',
        http_conn_id = 'corona_api',
        endpoint= 'summary',
        method ='GET',
        response_filter= lambda response: json.loads(response.text),
        log_response = True

    )

    create_table = SqliteOperator(
        task_id='create_table',
        sqlite_conn_id = 'db_sqlite',
        sql = '''
            CREATE TABLE IF NOT EXISTS data(
                newconfirmed INTEGER NOT NULL,
                totalconfirmed INTEGER NOT NULL,
                newdeaths INTEGER NOT NULL,
                totaldeaths INTEGER NOT NULL,
                newrecovered INTEGER NOT NULL,
                totalrecovered INTEGER NOT NULL,
                date TIMESTAMP NOT NULL
            );
            '''
    )

    processing_table = PythonOperator(
        task_id ='processing_table',
        python_callable = _processing_table
    )

    check_if_data_arrive = FileSensor(
        task_id = 'check_if_data_arrive',
        fs_conn_id = 'fs_default',
        filepath = 'corona.csv',
        poke_interval=10
    )

    storing_data = BashOperator(
        task_id = 'storing_data',
        bash_command = 'echo -e ".separator ","\n.import /tmp/corona.csv data" | sqlite3 /home/airflow/airflow/airflow.db'
    )

    is_api_available >> extracting_data >> create_table >> processing_table >> check_if_data_arrive >>  storing_data

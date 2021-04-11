from datetime import datetime
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator # 1

from airflow.providers.http.sensors.http import HttpSensor # 2

from airflow.providers.http.operators.http import SimpleHttpOperator # 3

from airflow.operators.python import PythonOperator # 4

from airflow.sensors.filesystem import FileSensor # 5

from airflow.operators.python import BranchPythonOperator # 6

from airflow.operators.bash import BashOperator # 7

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
    processed_data.to_csv('/tmp/corona_total.csv', index = None, header = False)

def _processing_table_2(ti):
    datas = ti.xcom_pull(task_ids = ['extracting_data'])
    if not len(datas):
        raise ValueError('data is empty')
    counter = 0
    data = datas[0]
    while counter<len(data["Countries"]):
        if(counter==0):
        
            x = data["Countries"][counter]
            processed_data = json_normalize({
            'country': x["Country"],
            'totalconfirmed': x["TotalConfirmed"],
            'totaldeaths': x["TotalDeaths"],
            'newconfirmed': x["NewConfirmed"],
            'newdeaths': x["NewDeaths"],
            'newrecovered': x["NewRecovered"],
            'date': x["Date"]
            })
            counter+=1
        elif(counter>=1):
            x = data["Countries"][counter]
            processed_data_2 = json_normalize({
                'country': x["Country"],
                'totalconfirmed': x["TotalConfirmed"],
                'totaldeaths': x["TotalDeaths"],
                'newconfirmed': x["NewConfirmed"],
                'newdeaths': x["NewDeaths"],
                'newrecovered': x["NewRecovered"],
                'date': x["Date"]
            })


            processed_data = processed_data.append(processed_data_2, ignore_index = True)
            counter+=1
    


    processed_data.to_csv('/tmp/corona_by_country.csv', index = None, header = False)
    



with DAG('new_corona', default_args=default_args,schedule_interval='@daily',catchup=False) as dag:


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

    create_table_total = SqliteOperator(
        task_id='create_table_total',
        sqlite_conn_id = 'db_sqlite',
        sql = '''
            CREATE TABLE IF NOT EXISTS data_total(
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

    create_table_by_country = SqliteOperator(
        task_id ='create_table_by_country',
        sqlite_conn_id = 'db_sqlite',
        sql = '''
            CREATE TABLE IF NOT EXISTS data_by_country(
                country VARCHAR NOT NULL,
                totalconfirmed INTEGER NOT NULL,
                totaldeaths INTEGER NOT NULL,
                newconfirmed INTEGER NOT NULL,
                newdeaths INTEGER NOT NULL,
                newrecovered INTEGER NOT NULL,
                date TIMESTAMP NOT NULL

            );
            '''

    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=lambda: ['create_table_total', 'create_table_by_country']
        )

    processing_table_1 = PythonOperator(
        task_id ='processing_table_1',
        python_callable = _processing_table
    )

    processing_table_2 = PythonOperator(
        task_id ='processing_table_2',
        python_callable = _processing_table_2
    )

    check_if_data_arrive_1 = FileSensor(
        task_id = 'check_if_data_arrive_1',
        fs_conn_id = 'fs_default',
        filepath = 'corona_total.csv',
        poke_interval=10
    )

    check_if_data_arrive_2 = FileSensor(
        task_id = 'check_if_data_arrive_2',
        fs_conn_id = 'fs_default',
        filepath = 'corona_by_country.csv',
        poke_interval=10
    )

    storing_data_1 = BashOperator(
        task_id = 'storing_data_1',
        bash_command = 'echo -e ".separator ","\n.import /tmp/corona_total.csv data_total" | sqlite3 /home/airflow/airflow/airflow.db'
    )

    storing_data_2 = BashOperator(
        task_id = 'storing_data_2',
        bash_command = 'echo -e ".separator ","\n.import /tmp/corona_by_country.csv data_by_country" | sqlite3 /home/airflow/airflow/airflow.db'
    )

    is_api_available >> extracting_data >> branching
    branching >> create_table_total >> processing_table_1 >> check_if_data_arrive_1 >> storing_data_1
    branching >> create_table_by_country >> processing_table_2 >> check_if_data_arrive_2 >> storing_data_2

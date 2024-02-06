from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum
from airflow.macros import ds_add
import os
from os.path import join
import pandas as pd 
from datetime import datetime, timedelta

with DAG(
        "climate_data",
        start_date=pendulum.datetime(2023, 8, 22, tz="UTC"),
        schedule_interval='0 0 * * 1', # execute every monday
) as dag:

    task_1 = BashOperator(
        task_id = 'create_folder',
        bash_command = 'mkdir -p "/home/fernando/Documents/airflowalura/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    def extract_data(data_interval_end):

        city = 'Boston'
        key = 'G45UFSJZLMPXARA77KV84HL2T'

        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
                    f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        data = pd.read_csv(URL)
        print(data.head())

        file_path = f'/home/fernando/Documents/airflowalura/semana={data_interval_end}/'

        data.to_csv(file_path + 'row_data.csv')
        data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperatures.csv')
        data[['datetime', 'description', 'icon']].to_csv(file_path + 'conditions.csv')


    task_2 = PythonOperator(
        task_id = 'extract_data',
        python_callable=extract_data,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )


    task_1 >> task_2
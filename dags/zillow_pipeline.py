from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import requests
import json

from airflow import DAG
from airflow.operators.python import PythonOperator



def extract_data_func(**kwargs):
    # not very useful to use kwargs in this case, just to try out
    url = kwargs["url"]
    querystring = kwargs["querystring"]
    headers = kwargs["headers"]
    dt = kwargs["dt"]
    response = requests.get(url=url, params=querystring, headers=headers)
    data = response.json()

    file_name = f"zillow_data_{dt}"
    output_json = f"/home/ubuntu/{file_name}.json"
    file_str = file_name + ".csv"

    with open(output_json, "w") as f:
        json.dump(data, f, indent=4)
    output_list = [output_json, file_str]
    return output_list   


def get_headers():
    load_dotenv()
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    rapidapi_host = os.getenv("RAPIDAPI_HOST")

    headers = {
        "X-RapidAPI-Key": rapidapi_key,
        "X-RapidAPI-Host": rapidapi_host
    }
    return headers

aux = {
    "url":"https://zillow56.p.rapidapi.com/search",
    "querystring":{"location":"houston, tx"},
    "headers":get_headers(),
    "dt":datetime.now().strftime("%Y%m%d")
}

args = {
    "owner":"Sja",
    "retries":2,
    "retry_delay":timedelta(minutes=30),
    "depend_on_past":False
}

dag = DAG(
    dag_id="zillow_etl_pipeline",
    description="end-to-end ETL project",
    default_args=args,
    start_date=datetime(2023,9,3),
    schedule="@monthly",
    catchup=False
)

with dag:
    extract_data_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_func,
        op_kwargs=aux
    )
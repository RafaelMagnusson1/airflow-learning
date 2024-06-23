from datetime import datetime, timedelta
import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def query_twitter_api():

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)

    query = "data science"

    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

    url_raw = f"https://labdados.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

    response = requests.get(url_raw)

    json_response = response.json()

    with open(r"C:\Users\rafae\apache-airflow\json_twitter.json", "w", encoding="utf-8") as f:

        json.dump(json_response, f, ensure_ascii=False, indent=4)

with DAG(
    "dag_json_twitter",
    start_date=days_ago(1),
    schedule_interval="@daily"
) as dag:

    query_twitter_api = PythonOperator(
        task_id = "query_twitter_api",
        python_callable = query_twitter_api
    )



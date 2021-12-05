from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from twitter_plugin.operators.twitter_search_to_local_operator import TwitterSearchToLocalOperator

DAG_NAME = "test_twitter_search_tweets"
OWNER = "BaoCai"
# TODO: Change this, otherwise it will create a lot of new directories
DIR = "/Users/baocai/Kite/Python/tweet_search/data/"

args = {
    "owner": OWNER,
    "start_date": days_ago(1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "use_legacy_sql": False,
    "catchup": False,
}

with DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval="@once",
    dagrun_timeout=timedelta(minutes=10),
) as dag:

    start = DummyOperator(
        task_id="start",
    )
    end = DummyOperator(
        task_id="end"
    )
    twitter_search = TwitterSearchToLocalOperator(
        task_id=f"twitter_search",
        directory=DIR,
        # query="((#korona OR #covid) OR corona) lang:fi",
        # query="%28%28%5C%23korona%20OR%20%5C%23covid%29%20OR%20corona%29%20lang%3Afi",
        include=[
            "masennus",
            "masennus oireet",
            "masennustesti",
            "masennus testi",
            "synnytyksen jälkeinen masennus",
            "masennus hoito",
            "ahdistus",
            "keskivaikea masennus",
            "psykoottinen masennus",
            "lapsen masennus",
            "vakava masennus",
            "depression",
            "vaikea masennus",
            "nuorten masennus",
            "raskaus masennus",
            "väsymys",
            "lievä masennus",
            "masennuslääkkeet",
            "mielenterveys",
            "nuoren masennus",
            "masennus blogi",
            "itsemurha",
            "masennus keskustelu",
            "psykoosi",
            "masennuksen hoito",
            "masennus itsehoito",
            "krooninen masennus",
            "kaksisuuntainen mielialahäiriö",
            "depression",
            "depression test",
            "depression symptoms",
            "manic depression",
            "postpartum depression",
            "crippling depression",
            "clinical depression",
            "high functioning depression",
        ],
        language="fi",
        start_time="2021-12-01T00:00:00Z",
        end_time="2021-12-05T00:00:00Z",
        endpoint="recent",
        max_results=100,
    )

    start >> twitter_search >> end
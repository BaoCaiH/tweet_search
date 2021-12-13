from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from twitter_plugin.operators.twitter_search_to_local_operator import (
    TwitterSearchToLocalOperator,
)

from twitter_plugin.utilities.cron import parse_cron

DAG_NAME = "twitter_search_tweets_weekly"
OWNER = "BaoCai"
DIR = "/home/baocai/baocai/data"
# DIR = "/Users/baocai/Kite/Python/tweet_search/data/"
SCHEDULE_INTERVAL = parse_cron(minute=0, hour=0, day_of_week="tuesday")

args = {
    "owner": OWNER,
    "start_date": datetime(2021, 1, 1, 0, 0, 0),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "use_legacy_sql": False,
    "catchup": False,
}

with DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval=SCHEDULE_INTERVAL,
    dagrun_timeout=timedelta(minutes=10),
) as dag:

    start = DummyOperator(
        task_id="start",
    )
    end = DummyOperator(task_id="end")
    twitter_search = TwitterSearchToLocalOperator(
        task_id=f"twitter_search",
        directory=DIR,
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
        start_time=f"{{{{ prev_ds }}}}T00:00:00Z",
        end_time=f"{{{{ ds }}}}T00:00:00Z",
        endpoint="all",
        max_results=100,
    )

    start >> twitter_search >> end

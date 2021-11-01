"""This module contains the ad analytics to GCS operator."""
import logging
import json
import pathlib
from datetime import datetime as dt, timedelta as tdt
from airflow.models import BaseOperator, SkipMixin
from airflow.exceptions import AirflowSkipException
from twitter_plugin.hooks.twitter_hook import TwitterHook
from twitter_plugin.utilities.patterns import patterns


class TwitterSearchToLocalOperator(BaseOperator, SkipMixin):
    
    template_fields = (
        "directory",
        "query",
        "start_time",
        "end_time",
        "endpoint",
        "expansions",
        "tweet_fields",
        "user_fields",
        "max_results",
        "twitter_conn_id",
        "filename",
    )

    def __init__(
        self,
        directory: str,
        query: str,
        start_time: str,
        end_time: str = None,
        endpoint: str = "recent",
        expansions: str = "referenced_tweets.id",
        tweet_fields: str = "author_id,conversation_id,created_at,geo,id,lang,text",
        user_fields: str = "created_at,id,verified",
        max_results: int = 20,
        twitter_conn_id: str = "twitter",
        filename: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.directory = directory
        self.query = query
        self.endpoint = endpoint
        self.start_time = start_time
        self.end_time = end_time
        self.expansions = expansions
        self.tweet_fields = tweet_fields
        self.user_fields = user_fields
        self.max_results = max_results
        self.twitter_conn_id = twitter_conn_id
        self.filename = filename
        logging.info("Initialising done.")
    
    def __input_check(self):
        logging.info("Checking input quality..")
        try:
            start = dt.strptime(self.start_time, patterns["start_time"])
        except ValueError as e:
            raise ValueError(
                f"Time provided in wrong format: {self.start_time}\n"
                f"Must be in format: {patterns['start_time']}"
            ) from e
        if not self.end_time:
            self.end_time = dt.strftime(
                start + tdt(hours=12),
                patterns["end_time"]
            )
        else:
            try:
                dt.strptime(self.end_time, patterns["end_time"])
            except ValueError as e:
                raise ValueError(
                    f"Time provided in wrong format: {self.end_time}\n"
                    f"Must be in format: {patterns['end_time']}"
                ) from e
        logging.info("Checking done.")

    def execute(self, context: dict):
        self.__input_check()
        logging.info("Initiating hooks..")
        twitter_hook = TwitterHook(
            "GET", self.twitter_conn_id
        )

        logging.info("Searching for tweets..")
        pages = twitter_hook.search(
            self.query,
            self.start_time,
            self.end_time,
            self.endpoint,
            self.expansions,
            self.tweet_fields,
            self.user_fields,
            self.max_results
        )

        if not self.filename:
            pathlib.Path(
                self.directory
                + self.twitter_conn_id
                + f"/search_{self.endpoint}"
            ).mkdir(parents=True, exist_ok=True)
            self.filename = (
                self.twitter_conn_id
                + f"/search_{self.endpoint}/"
                + self.start_time
            )

        count_page = 0
        for page in pages:
            if not page:
                raise AirflowSkipException("No records")
            logging.info(f"Received {len(page)} tweets.")
            save_file = (
                self.directory
                + self.filename
                + f"0000{count_page}"[-4:]
                + ".json"
            )
            with open(save_file, "a") as file:
                for line in page:
                    file.write(json.dumps(line) + "\n")
                file.flush()
                logging.info(f"Wrote {len(page)} lines to {file.name}")
            count_page += 1
        logging.info("Done.")

"""This module contains the ad analytics to GCS operator."""
from configparser import Error
import logging
import json
import pathlib
from re import sub
from typing import List
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
        start_time: str,
        query: str = None,
        queries: List[str] = None,
        include: List[str] = None,
        exclude: List[str] = None,
        language: str = None,
        country: str = None,
        end_time: str = None,
        endpoint: str = "recent",
        expansions: str = "referenced_tweets.id",
        tweet_fields: str = "author_id,conversation_id,created_at,geo,id,lang,text",
        user_fields: str = "created_at,id,verified",
        max_results: int = 20,
        access_limit: int = 512,
        twitter_conn_id: str = "twitter",
        filename: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.directory = directory
        self.endpoint = endpoint
        self.start_time = start_time
        self.query = query
        self.queries = queries
        self.include = include
        self.exclude = exclude
        self.language = language
        self.country = country
        self.end_time = end_time
        self.expansions = expansions
        self.tweet_fields = tweet_fields
        self.user_fields = user_fields
        self.max_results = max_results
        self.access_limit = access_limit
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
        if not (self.query or self.queries or self.include or self.exclude):
            raise Error("At least one of 'query', 'include', or 'exclude' has to be provided.")
        if not (self.query or self.queries):
            logging.info("Query wasn't provided, building query from params..")
            self.__build_query()
        elif not self.queries:
            self.queries = [self.query]
        logging.info("Checking done.")
    
    def __build_query(self):
        query_sub_part = ""
        if self.exclude:
            query_sub_part += f" {' '.join(['-' + kw for kw in self.exclude])}"
        if self.language:
            query_sub_part += f" lang:{self.language}"
        if self.country:
            query_sub_part += f" place_country:{self.country}"
        total_chars = len("".join(self.include)) + len(query_sub_part)
        if total_chars < self.access_limit * 0.8:
            self.queries = [f"({' OR '.join(self.include)})" + query_sub_part]
        else:
            logging.info(
                f"Number of characters ({total_chars}) is reaching"
                f" or exceeding the access limit ({self.access_limit}).\n"
                f"Splitting include words into several queries"
            )
            include_limit = self.access_limit + 0.8 - len(query_sub_part)
            subsets = []
            curr = self.include[0]
            for word in self.include[1:]:
                if len(curr) + len(word) <= include_limit:
                    curr += " OR " + word
                else:
                    subsets.append(curr)
                    curr = word
            if curr:
                subsets.append(curr)
            self.queries = [
                f"({sub})" + query_sub_part
                for sub in subsets
            ]
        logging.info(f"{len(self.queries)} queries were built.")        

    def __output_manager(self, subset, pages):
        logging.info(f"Handling output of subset {subset}..")
        count_page = 0
        for page in pages:
            if not page:
                raise AirflowSkipException("No records")
            logging.info(f"Received {len(page)} tweets.")
            save_file = (
                self.directory
                + self.filename
                + f"_{subset}_"
                + f"0000{count_page}"[-4:]
                + ".json"
            )
            with open(save_file, "a") as file:
                for line in page:
                    file.write(json.dumps(line) + "\n")
                file.flush()
                logging.info(f"Wrote {len(page)} lines to {file.name}")
            count_page += 1

    def execute(self, context: dict):
        self.__input_check()
        logging.info("Initiating hooks..")
        twitter_hook = TwitterHook(
            "GET", self.twitter_conn_id
        )

        logging.info("Searching for tweets..")
        for sub, query in enumerate(self.queries):
            pages = twitter_hook.search(
                query,
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
                    + f"/{self.start_time}"
                ).mkdir(parents=True, exist_ok=True)
                self.filename = (
                    self.twitter_conn_id
                    + f"/search_{self.endpoint}"
                    + f"/{self.start_time}/"
                )

            self.__output_manager(sub, pages)
        logging.info("Done.")

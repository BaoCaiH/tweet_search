"""This module contains the Linkedin hook."""
import logging
import json
from hashlib import sha256
from datetime import datetime as dt
from typing import Dict, List, Any, Tuple
from airflow.providers.http.hooks.http import HttpHook
import requests


class TwitterHook(HttpHook):
    """Twitter Hook."""

    def __init__(
        self,
        method: str = "GET",
        http_conn_id: str = "twitter",
    ) -> None:
        self.connection = self.get_connection(http_conn_id)
        self.access_token = self.connection.password
        self.headers = {"Authorization": f"Bearer {self.access_token}"}
        super().__init__(method=method, http_conn_id=http_conn_id)

    @staticmethod
    def __next_page(params: Dict[str, Any], next_token: str = None) -> Dict[str, Any]:
        params.update({"next_token": next_token})
        return params

    @staticmethod
    def __parse_response(response: requests.Response) -> Tuple[List[Any], str]:
        response_json = response.json()
        data = response_json.get("data")
        if not data:
            return (None, None)
        full_tweets = response_json.get("includes")
        if full_tweets:
            full_tweets = full_tweets.get("tweets")
        next_token = response_json["meta"].get("next_token", None)
        response_parsed = []
        for tweet in data:
            tweet_data = {
                "id": tweet["id"],
                "created_at": tweet["created_at"],
                "type": "",
                "tweet_id": "",
                "conversation_id": tweet["conversation_id"],
                "author_id": tweet["author_id"],
                "text": tweet["text"]
            }
            if "referenced_tweets" in tweet:
                tweet_data["type"] = tweet["referenced_tweets"][0]["type"]
                tweet_data["tweet_id"] = tweet["referenced_tweets"][0]["id"]
                full_text = list(filter(
                    lambda x: x["id"] == tweet_data["tweet_id"],
                    full_tweets)
                )
                if full_text:
                    tweet_data["text"] = full_text[0]["text"]
            else:
                tweet_data["type"] = "tweet"
                tweet_data["tweet_id"] = tweet["id"]
                tweet_data["text"] = tweet["text"]
            response_parsed.append(tweet_data)
        return (response_parsed, next_token)

    def __paginate_response(self, url, params, headers):
        response = super().run(url, params, headers)
        (data, next_token) = self.__parse_response(response)
        yield data
        while next_token:
            params = self.__next_page(params.copy(), next_token)
            logging.info(f"after: {params}")
            response = super().run(url, params, headers)
            (data, next_token) = self.__parse_response(response)
            yield data

    def search(
        self,
        query: str,
        start_time: str,
        end_time: str,
        endpoint: str,
        expansions: str,
        tweet_fields: str,
        user_fields: str,
        max_results: int,
    ):
        """Call the search endpoint."""
        if endpoint == "all":
            url = "2/tweets/search/all"
        else:
            url = "2/tweets/search/recent"
        params = {
            "query": query,
            "start_time": start_time,
            "end_time": end_time,
            "expansions": expansions,
            "tweet.fields": tweet_fields,
            "user.fields": user_fields,
            "max_results": max_results,
        }
        return self.__paginate_response(url, params, self.headers)

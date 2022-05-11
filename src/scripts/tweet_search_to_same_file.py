import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime as dt
from datetime import timedelta as tdt
from typing import Any, Dict, List, Tuple

import requests
from dotenv import load_dotenv


# Functions
def parse_response(response: requests.Response) -> Tuple[List[Any], str]:
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
            "text": tweet["text"],
        }
        if "referenced_tweets" in tweet:
            tweet_data["type"] = tweet["referenced_tweets"][0]["type"]
            tweet_data["tweet_id"] = tweet["referenced_tweets"][0]["id"]
            try:
                full_text = list(filter(lambda x: x["id"] == tweet_data["tweet_id"], full_tweets))
                if full_text:
                    tweet_data["text"] = full_text[0]["text"]
            except TypeError:
                logging.info("No full text version found. Continue.")
        else:
            tweet_data["type"] = "tweet"
            tweet_data["tweet_id"] = tweet["id"]
            tweet_data["text"] = tweet["text"]
        response_parsed.append(tweet_data)
    return (response_parsed, next_token)


def next_page(params: Dict[str, Any], next_token: str = None) -> Dict[str, Any]:
    params.update({"next_token": next_token})
    return params


def paginate_response(url, params, headers):
    response = requests.request("GET", url, params=params, headers=headers)
    (data, next_token) = parse_response(response)
    yield data
    while next_token:
        time.sleep(10)
        params = next_page(params.copy(), next_token)
        logging.info(f"after: {params}")
        response = requests.request("GET", url, params=params, headers=headers)
        (data, next_token) = parse_response(response)
        yield data


def output_manager(pages, append_mode=True):
    count_page = 0
    for page in pages:
        if not page:
            logging.info("No records")
            break
        logging.info(f"Received {len(page)} tweets.")
        if append_mode:
            save_file = DIR + FILE_NAME + ".json"
        else:
            save_file = DIR + FILE_NAME + f"0000{count_page}"[-4:] + ".json"
        with open(save_file, "a") as file:
            for line in page:
                file.write(json.dumps(line) + "\n")
            file.flush()
            logging.info(f"Wrote {len(page)} lines to {file.name}")
        count_page += 1


if __name__ == "__main__":
    # Setup
    ## Log to console
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()],
    )
    ## Change dir to current file so relative paths work
    os.chdir(sys.path[0])
    # Defining arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--start-date",
        required=True,
        type=str,
        help="Start date of the range you want to search",
        dest="start_date"
    )
    parser.add_argument(
        "-e",
        "--end-date",
        required=True,
        type=str,
        help="End date of the range you want to search",
        dest="end_date"
    )
    parser.add_argument(
        "-kw",
        "--keywords",
        type=str,
        help="Comma separated keywords",
        dest="keywords"
    )
    parser.add_argument(
        "-kwf",
        "--keywords_file",
        type=str,
        help="Newline separated keywords file relative path",
        dest="keywords_file"
    )
    parser.add_argument(
        "-d",
        "--data-dir",
        required=True,
        type=str,
        help="Directory to save data, defaults to script directory",
        dest="data_dir"
    )
    parser.add_argument(
        "-f",
        "--filename",
        default="tweet_search_data",
        type=str,
        help="File name",
        dest="filename"
    )
    parser.add_argument(
        "-l",
        "--lang",
        type=str,
        help="Twitter user language setting.",
        dest="lang"
    )
    parser.add_argument(
        "-g",
        "--geo",
        type=str,
        help="Twitter use geo location setting.",
        dest="geo"
    )
    parser.add_argument(
        "--tweet-fields",
        type=str,
        default="author_id,conversation_id,created_at,geo,id,lang,text",
        help="Fields to get from the tweet metadata, comma separated.",
        dest="tweet_fields"
    )
    parser.add_argument(
        "--user_fields",
        type=str,
        default="created_at,id,verified",
        help="Fields to get from the user metadata, comma separated.",
        dest="user_fields"
    )
    parser.add_argument(
        "--expansions",
        type=str,
        default="referenced_tweets.id",
        help="Tweet expansions, comma separated.",
        dest="expansions"
    )
    args = parser.parse_args()
    load_dotenv()

    DIR = args.data_dir
    DIR_BACKUP = f"{DIR}backup/"
    FILE_NAME = args.filename
    start_date = dt.strptime(args.start_date, "%Y-%m-%d")
    end_date = dt.strptime(args.end_date, "%Y-%m-%d")
    if end_date < start_date:
        raise Exception("Start date cannot be more recent compares to end date.")
    dates = [start_date.strftime("%Y-%m-%d")]
    temp_date = start_date
    while temp_date < end_date:
        temp_date += tdt(7)
        dates.append(temp_date.strftime("%Y-%m-%d"))
    if args.keywords:
        keywords = args.keywords.split(",")
    elif args.keywords_file:
        with open(args.keywords_file) as f:
            lines = f.read().splitlines()
        keywords = lines
    else:
        raise Exception("At least -kw/--keywords or -kwf/--keywords_file must be passed.")

    URL = "https://api.twitter.com/2/tweets/search/all"
    HEADERS = {
        "Authorization": f"Bearer {os.getenv('TOKEN')}"
    }

    # Running queries
    query_template = "({})"
    if args.lang:
        query_template += f" lang:{args.lang}"
    if args.geo:
        query_template += f" place_country:{args.geo}"

    queries = [query_template.format(" OR ".join(keywords[i: i + 10])) for i in range(0, len(keywords), 10)]

    for i in range(len(dates) - 1):
        date_range = dates[i : i + 2]
        PARAMS = {
            "start_time": f"{date_range[0]}T00:00:00Z",
            "end_time": f"{date_range[1]}T00:00:00Z",
            "expansions": args.expansions,
            "tweet.fields": args.tweet_fields,
            "user.fields": args.user_fields,
            "max_results": 100,
        }
        for query in queries:
            params = PARAMS.copy()
            params.update({"query": query})
            logging.info(f"Query the query: {params}")
            pages = paginate_response(URL, params, HEADERS)
            output_manager(pages)

        # Backup with bash
        file_to_backup = DIR + FILE_NAME + ".json"
        file_backup = f"{DIR_BACKUP}{FILE_NAME}_{date_range[1]}.json"
        cmd_backup = f"cp {file_to_backup} {file_backup}"
        process_backup = subprocess.Popen(cmd_backup.split(), stdout=subprocess.PIPE)
        output, error = process_backup.communicate()
        # Deduplicate with bash
        cmd_sort = f"sort {file_backup}"
        cmd_deduplicate = f"uniq > {file_to_backup}"
        os.system(f"{cmd_sort} | {cmd_deduplicate}")

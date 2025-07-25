import datetime
from io import StringIO
import requests
import pandas as pd
from bs4 import BeautifulSoup
from save_to_s3 import save_raw_data_to_s3, save_clean_data_to_s3
from zoneinfo import ZoneInfo
from config import BUCKET_NAME

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_data_from_ibex() -> str:
    logger.info("Started fetching ibex data")
    url = 'https://ibex.bg/dam-history.php'
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        ),
        "Referer": "https://ibex.bg/dam-history.php",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: HTTP {response.status_code}")

    return response.text


def parse_data(html_str: str) -> pd.DataFrame:
    logger.info("Started parsing ibex data")
    soup = BeautifulSoup(html_str, "html.parser")
    cleaned_table = str(soup).replace(",", ".")
    ibex_df = pd.read_html(StringIO(cleaned_table))[0]
    ibex_df["Date"] = pd.to_datetime(ibex_df["Date"], format="%Y-%m-%d")

    return ibex_df


def main():
    now = datetime.datetime.now(ZoneInfo("Europe/Sofia"))
    raw_data = get_data_from_ibex()
    save_raw_data_to_s3(raw_data, now, BUCKET_NAME)
    parsed_data = parse_data(raw_data)
    save_clean_data_to_s3(parsed_data, now, BUCKET_NAME)


if __name__ == "__main__":
    main()

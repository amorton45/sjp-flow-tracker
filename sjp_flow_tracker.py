#!/usr/bin/env python3
"""
St. James's Place – daily price scraper & quarterly flow estimator
Author: Drew (ChatGPT scaffold)
"""

import datetime as dt
import io
import json
import re
import sys
from pathlib import Path

import pdfplumber
import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup

# ---------- config ----------
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
FUNDCODE_FILE = "fund_codes.yaml"

# Hidden XHR endpoint called by https://www.sjp.co.uk/individuals/fund-prices
PRICE_ENDPOINT = (
    "https://services.sjp.co.uk/fund-prices-api/prices?fundType={fund_type}"
)

FACTSHEET_PDF = (
    "https://fundfactsheets.sjp.co.uk/Latest/{code}_Factsheet.pdf"
)  # works for all mirrors
# ----------------------------


def load_fund_codes():
    with open(FUNDCODE_FILE, "r") as f:
        return yaml.safe_load(f)


def get_daily_prices(fund_type: str) -> pd.DataFrame:
    """Return dataframe: date, fundCode, price_pence"""
    url = PRICE_ENDPOINT.format(fund_type=fund_type)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    j = r.json()

    records = []
    for fund in j["funds"]:
        records.append(
            {
                "date": pd.to_datetime(fund["priceDate"]).date(),
                "code": fund["fundCode"],
                "price_pence": float(fund["price"]) * 100,
            }
        )
    return pd.DataFrame(records)


def get_month_end_size(code: str, as_of: dt.date) -> float:
    """
    Download factsheet PDF for fund code and pull the Fund size (£m/£bn).
    as_of must be the same month-end that appears on the factsheet.
    Returns size in GBP millions.
    """
    url = FACTSHEET_PDF.format(code=code)
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    with pdfplumber.open(io.BytesIO(r.content)) as pdf:
        text = "\n".join(page.extract_text() for page in pdf.pages)

    # 1) ensure the PDF date matches the month we expect
    if as_of.strftime("%d %B %Y") not in text:
        print(f"Warning: {code} factsheet not yet updated for {as_of}", file=sys.stderr)

    # 2) pull Fund size line – captures “Fund size £7.3bn” or “Fund size £845m”
    m = re.search(r"Fund size\s*£\s*([\d.,]+)\s*([mb]n)", text, flags=re.I)
    if not m:
        raise ValueError(f"Could not find Fund size for {code}")

    num, unit = m.groups()
    value = float(num.replace(",", ""))
    return value * (1_000 if unit.lower().startswith("b") else 1)  # → GBP m


def backsolve_flows(month_end: dt.date, codes: list[str], price_df: pd.DataFrame):
    """Return DataFrame of implied quarterly flows for each fund."""
    month_start = (month_end - pd.offsets.QuarterBegin(startingMonth=1)).date()

    flows = []
    for code in codes:
        try:
            size_start = get_month_end_size(code, month_start)
            size_end = get_month_end_size(code, month_end)
        except Exception as e:
            print(f"{code}: {e}", file=sys.stderr)
            continue

        # prices
        p_start = (
            price_df.loc[(price_df["code"] == code) & (price_df["date"] == month_start)]
            .iloc[0]["price_pence"]
        )
        p_end = (
            price_df.loc[(price_df["code"] == code) & (price_df["date"] == month_end)]
            .iloc[0]["price_pence"]
        )

        units_start = size_start * 1e6 / p_start  # GBP→pence
        aum_no_flow = units_start * p_end / 1e6  # back to £m
        flow = size_end - aum_no_flow

        flows.append(
            {
                "quarter_end": month_end,
                "code": code,
                "flow_gbp_m": round(flow, 2),
            }
        )

    return pd.DataFrame(flows)


def main():
    today = dt.date.today()
    codes = load_fund_codes()

    # -------- 1) grab today's prices & append to CSV --------
    prices_life = get_daily_prices("life")
    prices_pension = get_daily_prices("pension")
    prices_df = pd.concat([prices_life, prices_pension], ignore_index=True)

    price_file = DATA_DIR / "prices.csv"
    if price_file.exists():
        existing = pd.read_csv(price_file, parse_dates=["date"])
        prices_df = pd.concat([existing, prices_df]).drop_duplicates(
            subset=["date", "code"]
        )

    prices_df.to_csv(price_file, index=False)

    # -------- 2) on the 10th business day of Jan/Apr/Jul/Oct estimate flows --------
    first_biz = pd.Timestamp(today.replace(day=1)).tz_localize(None)
    biz_day = (
        pd.bdate_range(first_biz, periods=20, freq="C")
        .to_series()
        .reset_index(drop=True)
    )
    if (today in biz_day[:10].dt.date.values) and (today.month in [1, 4, 7, 10]):
        q_end = (today - pd.offsets.MonthBegin()).date()  # last day of prior month
        q_flows = backsolve_flows(
            q_end, list(codes["life"].keys()) + list(codes["pension"].keys()), prices_df
        )

        flow_file = DATA_DIR / "quarterly_flows.csv"
        if flow_file.exists():
            existing = pd.read_csv(flow_file, parse_dates=["quarter_end"])
            q_flows = pd.concat([existing, q_flows]).drop_duplicates(
                subset=["quarter_end", "code"]
            )

        q_flows.to_csv(flow_file, index=False)
        print(f"Flows up to {q_end} updated.")


if __name__ == "__main__":
    main()

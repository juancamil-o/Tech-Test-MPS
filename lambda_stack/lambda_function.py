# app.py
import os, json, csv, time
from datetime import date, timedelta
from urllib import request, parse
import boto3

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["BUCKET_NAME"]
OUT_DIR = os.getenv("OUT_DIR", "/tmp/output.csv")
BASE_URL = "https://www.datos.gov.co/resource/jbjy-vk9h.json"

PAGE_SIZE = int(os.getenv("PAGE_SIZE", "3000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "0.8"))

def calculate_days_back(days_back: int = 1) -> str:
    return (date.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")

def query_fechadefirma(day: str, limit: int, offset: int) -> dict:
    return {
        "$select": "*",
        "$where": f"fecha_de_firma between '{day}T00:00:00' and '{day}T23:59:59'",
        "$limit": limit,
        "$offset": offset
    }

def socrata_get(params: dict) -> list:
    qs = parse.urlencode(params, safe=":,><= '")
    url = f"{BASE_URL}?{qs}"
    for attempt in range(MAX_RETRIES):
        try:
            req = request.Request(url, headers={"User-Agent": "lambda-secops/1.0"})
            with request.urlopen(req, timeout=60) as resp:
                if resp.status == 200:
                    return json.loads(resp.read())
                raise RuntimeError(f"HTTP {resp.status}")
        except Exception:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(BACKOFF_BASE * (2 ** attempt))
    return []

def handler(event, context):
    day = calculate_days_back(int(os.getenv("DAYS_BACK", "2")))
    key = f"contracts/day={day}/contracts_{day}.csv"

    fieldnames = None
    total = 0

    with open(OUT_DIR, "w", newline="", encoding="utf-8") as f:
        writer = None
        offset = 0

        while True:
            page = socrata_get(query_fechadefirma(day, PAGE_SIZE, offset))
            if not page:
                break

            for rec in page:
                if fieldnames is None:
                    # Fijamos cabecera inicial
                    fieldnames = list(rec.keys())
                    writer = csv.DictWriter(
                        f,
                        fieldnames=fieldnames,
                        extrasaction="ignore",
                        restval=""
                    )
                    writer.writeheader()

                writer.writerow(rec)
                total += 1

            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

    if total == 0:
        print(f"No data for {day}")
        return {"day": day, "count": 0, "s3": None}

    s3.upload_file(OUT_DIR, BUCKET_NAME, key)
    print(f"Uploaded CSV to s3://{BUCKET_NAME}/{key} ({total} rows)")

    return {"day": day, "count": total, "s3": f"s3://{BUCKET_NAME}/{key}"}

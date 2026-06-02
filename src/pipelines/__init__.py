import os
import argparse

from datetime import date
from pathlib import Path

from prefect import flow, task
from prefect.futures import wait

from extractor.bolsa_familia import BolsaFamiliaExtractor
from ingestion.bolsa_familia import BolsaFamiliaDownloader, Program


@task(retries=2, retry_delay_seconds=2)
def download(year: int, month: int) -> Path:
  return BolsaFamiliaDownloader(year, month).fetch()

@task(retries=2, retry_delay_seconds=2)
def extract(path: Path) -> Path:
  return BolsaFamiliaExtractor(path).run()

@flow(log_prints=True)
def run_pipeline(since: date, until: date):
  extractions = []
  current = since

  while until >= current:
    path = download.with_options(
      name=f"download-bf-{current.year}-{current.month:02}"
    ).submit(
      year=current.year,
      month=current.month,
    )

    extraction = extract.with_options(
      name=f"extract-bf-{current.year}-{current.month:02}"
    ).submit(path)

    extractions.append(extraction)

    if len(extractions) >= int(os.getenv('CONCURRENCY', '50')):
      wait(extractions)
      extractions.clear()

    current = get_next_month(current)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "--since",
    metavar="YYYY-MM",
    help="Data inicial a processar (ex: 2004-01). Padrão: início do programa.",
  )
  parser.add_argument(
    "--until",
    metavar="YYYY-MM",
    help="Data final a processar (ex: 2024-06). Padrão: mês atual.",
  )
  args = parser.parse_args()

  since = date.fromisoformat(f"{args.since}-01") if args.since else Program.BOLSA_FAMILIA_1.value
  until = date.fromisoformat(f"{args.until}-01") if args.until else date.today().replace(day=1)

  run_pipeline(since=since, until=until)

def get_next_month(current: date) -> date:
  year = current.year
  month = current.month

  month += 1

  if month > 12:
    month = 1
    year += 1

  return date(year, month, 1)

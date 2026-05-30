import os

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
def main():
  extractions = []
  current = Program.BOLSA_FAMILIA_1.value

  while date.today() >= current:
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

def get_next_month(current: date) -> date:
  year = current.year
  month = current.month

  month += 1

  if month > 12:
    month = 1
    year += 1

  return date(year, month, 1)

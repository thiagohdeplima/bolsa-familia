from datetime import date
from pathlib import Path

from prefect import flow, task

from ingestion.bolsa_familia import BolsaFamiliaDownloader, Program

@task(retries=2, retry_delay_seconds=2)
def download(year: int, month: int) -> Path:
  return BolsaFamiliaDownloader(year, month).fetch()

@flow
def main():
  current = Program.BOLSA_FAMILIA_1.value
  month = current.month
  year = current.year

  while date.today() >= current:
    download.with_options(
      name=f"download-{current.year}-{current.month:02}"
    ).submit(
      year=current.year,
      month=current.month,
    )

    month += 1

    if month > 12:
      month = 1
      year += 1

    current = date(year, month, 1)

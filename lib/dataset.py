import os
import zipfile

from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from enum import Enum
from pathlib import Path

import requests

import pandas as pd

class Program(Enum):
  BOLSA_FAMILIA_1 = "BOLSA_FAMILIA_1"
  AUXILIO_BRASIL  = "AUXILIO_BRASIL"
  BOLSA_FAMILIA_2 = "BOLSA_FAMILIA_2"

class FailedToDownload(Exception):
  def __init__(self, url: str):
    super().__init__(f"failed to download from {url}")

class NoProgramInEffectError(Exception):
  def __init__(self, year: int, month: int):
    super().__init__(f"in {year}/{month} there are no program in effect")

def get_url_for_download(year: int, month: int) -> str:
  base_url = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida"

  match get_program_in_effect(year, month):
    case Program.BOLSA_FAMILIA_1:
      return f"{base_url}/bolsa-familia-pagamentos/{year}{month:02}_BolsaFamilia_Pagamentos.zip", Program.BOLSA_FAMILIA_1

    case Program.AUXILIO_BRASIL:
      return f"{base_url}/auxilio-brasil-saques/{year}{month:02}_AuxilioBrasil.zip", Program.AUXILIO_BRASIL

    case Program.BOLSA_FAMILIA_2:
      return f"{base_url}/novo-bolsa-familia/{year}{month:02}_NovoBolsaFamilia.zip", Program.BOLSA_FAMILIA_2


def get_program_in_effect(year: int, month: int) -> Program:
  ref = date(year, month, 1)

  if ref <= date(2012, 12, 1):
    raise NoProgramInEffectError(year, month)

  if ref <= date(2021, 11, 1):
    return Program.BOLSA_FAMILIA_1

  if ref <= date(2022, 12, 1):
    return Program.AUXILIO_BRASIL

  return Program.BOLSA_FAMILIA_2

def download_and_unpack(program: Program, url: str, dst: Path):
  filename = os.path.basename(url)

  zippath = dst / filename
  csvpath = zippath.with_suffix('.csv')
  parquetpath = zippath.with_suffix('.parquet')

  def download_and_save_zip():
    download = requests.get(url)

    if download.status_code != 200:
      raise FailedToDownload(url)

    with open(dst / filename, 'wb') as f:
      f.write(download.content)

  def extract_zip():
    with zipfile.ZipFile(zippath, 'r') as zip:
      zip.extractall(dst)

  def delete_unused_files():
    for path in [csvpath, zippath]:
      try:
        path.unlink()
      except FileNotFoundError:
        continue

  def save_parquet():
    df = pd.read_csv(csvpath, sep=';', encoding='latin1', dtype='string')

    df['PROGRAMA'] = program.value
    df['VALOR PARCELA'] = df['VALOR PARCELA'].apply(lambda val: val.replace(',', '.')).astype(float)
    df['MÊS COMPETÊNCIA'] =  df['MÊS COMPETÊNCIA'].apply(lambda val: datetime.strptime(val, "%Y%m").date())
    df['MÊS REFERÊNCIA'] =  df['MÊS REFERÊNCIA'].apply(lambda val: datetime.strptime(val, "%Y%m").date())

    df = df.rename(columns={
        'MÊS COMPETÊNCIA': 'MES_COMP',
        'MÊS REFERÊNCIA':  'MES_REF',
        'NOME MUNICÍPIO':  'NM_MUNICIPIO',
        'CPF FAVORECIDO':  'CPF',
        'NIS FAVORECIDO':  'NIS',
        'NOME FAVORECIDO': 'NM_FAVORECIDO',
        'VALOR PARCELA':   'VALOR',
        'CÓDIGO MUNICÍPIO SIAFI': 'COD_SIAFI_MUNICIPIO'
    })

    df.to_parquet(parquetpath, index=False, compression='zstd')

  if parquetpath.exists():
    return

  if csvpath.exists():
    save_parquet()
    delete_unused_files()
    return

  if not zippath.exists():
    download_and_save_zip()

  extract_zip()
  save_parquet()
  delete_unused_files()

def get_all_url_for_download():
  urls  = list()
  start = date(2012, 12, 1)

  while start < date.today():
    start = start + relativedelta(months=1)
    url = get_url_for_download(start.year, start.month)

    urls.append(url)

  return urls

def download_and_unpack_all(dst: Path):
  for url, program in reversed(get_all_url_for_download()):
    try:
      download_and_unpack(program, url, dst)
    except NoProgramInEffectError:
      continue
    except FailedToDownload:
      continue

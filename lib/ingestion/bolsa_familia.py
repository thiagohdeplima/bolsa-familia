import os

import requests

from enum import Enum
from datetime import date
from ingestion import Downloader
from pathlib import Path

DATADIR = Path("./data")
DATE_START = date(2012, 12, 1)

class Program(Enum):
  """
  O que é conhecido popularmente conhecido como Bolsa Familía são
  na verdade três programas sociais distintos, mas, com a mesma
  finalidade.
  """
  BOLSA_FAMILIA_1 = date(2013,  1, 1)
  AUXILIO_BRASIL  = date(2021, 11, 1)
  BOLSA_FAMILIA_2 = date(2023,  3, 1)


class NoProgramInEffectError(Exception):
  """
  Acontece quando se tenta obter dados referente a um periodo que não tinha
  nenhum programa ainda em vigor.
  """
  def __init__(self, year: int, month: int):
    super().__init__(f"in {year}/{month} there are no program in effect")


class BolsaFamiliaDownloader(Downloader):
  def __init__(self, year: int, month: int):
    self.year  = year
    self.month = month

    self.destination = DATADIR / "raw" / str(year)

  @property
  def program(self) -> Program:
    ref = date(self.year, self.month, 1)

    if ref >= Program.BOLSA_FAMILIA_2.value:
      return Program.BOLSA_FAMILIA_2

    if ref >= Program.AUXILIO_BRASIL.value:
      return Program.AUXILIO_BRASIL

    if ref >= Program.BOLSA_FAMILIA_1.value:
      return Program.BOLSA_FAMILIA_1

    raise NoProgramInEffectError(self.year, self.month)

  @property
  def url(self) -> str:
    base_url = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida"

    match self.program:
      case Program.BOLSA_FAMILIA_1:
        return f"{base_url}/bolsa-familia-pagamentos/{self.year}{self.month:02}_BolsaFamilia_Pagamentos.zip"

      case Program.AUXILIO_BRASIL:
        return f"{base_url}/auxilio-brasil-saques/{self.year}{self.month:02}_AuxilioBrasil.zip"

      case Program.BOLSA_FAMILIA_2:
        return f"{base_url}/novo-bolsa-familia/{self.year}{self.month:02}_NovoBolsaFamilia.zip"

  def fetch(self) -> Path:
    target_size = 0
    target_path = self.destination / os.path.basename(self.url)
    req_headers = dict()

    if not self.destination.exists():
      self.destination.mkdir(exist_ok=True, parents=True)

    if target_path.is_file():
      target_size = os.path.getsize(target_path)
      req_headers = dict(Range=f"bytes={target_size}-")

    with requests.get(self.url, headers=req_headers, stream=True) as download:
      download.raise_for_status()

      with open(self.destination / os.path.basename(self.url), 'wb') as f:
        for chunck in download.iter_content(chunk_size=4096):
          f.write(chunck)

    return target_path

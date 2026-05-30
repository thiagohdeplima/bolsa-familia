import gc
import os
import re

import pandas as pd

from pathlib import Path

from ingestion.bolsa_familia import Program

DATADIR = Path("./data/bronze")

class BolsaFamiliaExtractor:
  def __init__(self, source: Path):
    self.source = source

    self.filename = os.path.basename(self.source)
    self.destination = (DATADIR / "bolsa_familia" / self.filename).with_suffix(".parquet")

    self.csv_args = dict(
      compression="zip",
      sep=';',
      encoding='latin1',
      dtype='string'
    )

  def run(self) -> Path:
    program = self.__get_program_by_filename()

    self.destination.parent.mkdir(parents=True, exist_ok=True)

    if self.destination.exists():
      return self.destination

    df = pd.read_csv(self.source, **self.csv_args)
    df = self.__prepare_dataframe(df, program)
    df.to_parquet(self.destination,
      engine="pyarrow",
      compression="zstd",
      compression_level=12,
      index=False
    )

    del(df)
    gc.collect()

    return self.destination

  def __get_program_by_filename(self) -> Program:
    extracted = re.match(r"(\d{6})\_(\w+)+", self.filename)

    if extracted.group(2) == "NovoBolsaFamilia":
      return Program.BOLSA_FAMILIA_2.name

    if extracted.group(2) == "AuxilioBrasil":
      return Program.AUXILIO_BRASIL.name

    if extracted.group(2) == "BolsaFamilia_Pagamentos":
      return Program.BOLSA_FAMILIA_1.name

    raise ValueError(f"Invalid filename {self.filename}, group {extracted.group(2)} doesn't match")

  def __prepare_dataframe(self, df: pd.DataFrame, program: Program) -> pd.DataFrame:
    df["PROGRAMA"] = program
    df["PROGRAMA"] = df["PROGRAMA"].astype("category")
    df["UF"] = df["UF"].astype("category")


    df["NOME MUNICÍPIO"] = df["NOME MUNICÍPIO"].astype("category")
    df["CÓDIGO MUNICÍPIO SIAFI"] = df["CÓDIGO MUNICÍPIO SIAFI"].astype("category")

    df['MÊS REFERÊNCIA'] = pd.to_datetime(df['MÊS REFERÊNCIA'], format="%Y%m").dt.date
    df['MÊS COMPETÊNCIA'] = pd.to_datetime(df['MÊS REFERÊNCIA'], format="%Y%m").dt.date

    df['VALOR PARCELA'] = df['VALOR PARCELA'].apply(lambda val: val.replace(',', '.')).astype(float).mul(100).round().astype('int32')

    return df

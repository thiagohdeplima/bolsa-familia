import glob
import os


def model(dbt, session):
  dbt.config(materialized="table")

  project_root = os.getenv("DEVENV_ROOT")

  bronze_glob = os.path.join(project_root, "data", "bronze", "bolsa_familia", "*.parquet")
  silver_path = os.path.join(project_root, "data", "silver", "bolsa_familia")

  os.makedirs(silver_path, exist_ok=True)

  for parquet_file in sorted(glob.glob(bronze_glob)):
    session.execute(f"""
      COPY (
        SELECT
          YEAR("MÊS COMPETÊNCIA")::INTEGER  AS ANO,
          MONTH("MÊS COMPETÊNCIA")::INTEGER AS MES,
          "UF",
          "PROGRAMA",
          "MÊS COMPETÊNCIA"         AS MES_COMPETENCIA,
          "MÊS REFERÊNCIA"          AS MES_REFERENCIA,
          "CÓDIGO MUNICÍPIO SIAFI"  AS COD_SIAFI_MUNICIPIO,
          "NOME MUNICÍPIO"          AS NOME_MUNICIPIO,
          "CPF FAVORECIDO"          AS CPF_FAVORECIDO,
          "NIS FAVORECIDO"          AS NIS_FAVORECIDO,
          "NOME FAVORECIDO"         AS NOME_FAVORECIDO,
          "VALOR PARCELA"           AS VALOR_PARCELA
        FROM read_parquet('{parquet_file}')
        ORDER BY VALOR_PARCELA
      ) TO '{silver_path}' (
        FORMAT PARQUET,
        PARTITION_BY (ANO, MES, UF),
        OVERWRITE_OR_IGNORE TRUE
      )
    """)

  return session.sql("SELECT now() AS finished_at")

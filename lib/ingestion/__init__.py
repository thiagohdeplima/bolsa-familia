from abc import abstractmethod
from pathlib import Path
from typing import Protocol

class Downloader(Protocol):
  """
  Protocolo que deve ser implementada por todas as classes
  que fazem download (ingerem) dados de alguma fonte
  implementada neste projeto.

  A obtenção de dados é sempre baseado em ano e mês, e o
  conteúdo será salvo na camada raw.
  """

  @abstractmethod
  def __init__(self, year: int, month: int): ...

  @abstractmethod
  def fetch(self) -> Path: ...


import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Optional

import pandas as pd



@dataclass
class SourceFile:
    url: str
    index_url: str
    source: str
    path: str
    run_date: date
    fxx: int
    product: str
    member: Optional[int]



class BaseSource(ABC):
    SOURCES: dict[str, str] = {}   # {"aws": "https://...", ...}
    PRODUCTS: dict[str, str] = {}  # {"atmos.25": "description", ...}

    def __init__(
        self,
        priority: tuple[str, ...],
        timeout: int = 10,
    ):
        self.priority = priority
        self.timeout = timeout
        self.logger = logging.getLogger(
            f"weather_agent.source.{self.__class__.__name__}"
        )


    ## ------------------------------------------------------------------
    ## Abstract — реализуется в каждом источнике
    ## ------------------------------------------------------------------
    @abstractmethod
    def build_path(self, **kwargs) -> str:
        """
        Строит относительный путь к файлу (без base URL).
        Используется для построения URL под любой источник.
        """
        ...


    @abstractmethod
    def _parse_idx(self, idx_text: str, source_url: str) -> pd.DataFrame:
        """
        Парсит сырой .idx текст в DataFrame.
        Формат разный для wgrib2 (GEFS) и eccodes (IFS).
        """
        ...


    @abstractmethod
    def _idx_url(self, file_url: str) -> str:
        """Строит URL для .idx файла из URL основного файла."""
        ...

    ## ------------------------------------------------------------------
    ## Concrete — общая логика для всех источников
    ## ------------------------------------------------------------------

    def build_url(self, source: str, **kwargs) -> str:
        """Строит полный URL для конкретного источника."""
        if source not in self.SOURCES:
            raise ValueError(
                f"Unknown source '{source}'. Available: {list(self.SOURCES)}"
            )
        path = self.build_path(**kwargs)
        return f"{self.SOURCES[source]}/{path}"


    def select_source(self, **kwargs) -> str:
        """
        Проверяет доступность источников по приоритету.
        Возвращает первый доступный.
        """
        import requests
        path = self.build_path(**kwargs)
        for source in self.priority:
            if source not in self.SOURCES:
                continue
            url = f"{self.SOURCES[source]}/{path}"
            try:
                r = requests.head(url, timeout=self.timeout, allow_redirects=True)
                if 200 <= r.status_code < 300:
                    self.logger.debug(f"Source selected: {source} ({r.status_code})")
                    return source
                self.logger.debug(f"Source {source} unavailable: {r.status_code}")
            except Exception as e:
                self.logger.debug(f"Source {source} error: {type(e).__name__}: {e}")
                continue

        raise RuntimeError(
            f"No available source found among {self.priority} for path={path}, url={url}"
        )


    def inventory(self, source: str, path: str,) -> pd.DataFrame:
        """
        Возвращает DataFrame со списком переменных в файле.
        Не скачивает сам файл — только .idx (~3-5KB).
        Агент вызывает это чтобы решить что качать.
        """
        import requests

        file_url = f"{self.SOURCES[source]}/{path}"
        idx_url = self._idx_url(file_url)
        try:
            r = requests.get(idx_url, timeout=self.timeout)
            r.raise_for_status()
            df = self._parse_idx(r.text, file_url)
            self.logger.debug(
                f"Inventory fetched from {source}: {len(df)} variables"
            )
            return df
        except Exception as e:
            self.logger.debug(
                f"Inventory failed from {source}: {type(e).__name__}: {e}"
            )

        raise RuntimeError(
            f"Could not fetch inventory from any source for path={path}"
        )


    def search(self, pattern: str, inventory: pd.DataFrame) -> pd.DataFrame:
        """
        Фильтрует инвентарь по regex паттерну.
        Возвращает только строки где search_this матчит паттерн.

        Пример:
            source.search("TMP:10 mb|UGRD:10 mb", run_date=..., member=1, fxx=24)
        """
        matched = inventory[inventory["search_this"].str.contains(pattern, regex=True)]
        if matched.empty:
            self.logger.warning(
                f"No variables matched pattern='{pattern}'"
            )
        return matched


    def get_source_file(self, source: str, **kwargs) -> SourceFile:
        """
        Используется лоадером как точка входа.
        """
        path = self.build_path(**kwargs)
        url = f"{self.SOURCES[source]}/{path}"
        return SourceFile(
            url=url,
            index_url=self._idx_url(url),
            source=source,
            path=path,
            run_date=kwargs.get("run_date"),
            member=kwargs.get("member"),
            fxx=kwargs.get("fxx"),
            product=kwargs.get("product", ""),
        )
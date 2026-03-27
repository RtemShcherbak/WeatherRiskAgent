import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import pandas as pd

from .base_source import BaseSource


class GEFSSource(BaseSource):
    """
    Источник данных GEFS (Global Ensemble Forecast System).

    Продукты:
        atmos.25  — 0.25°, ~35 основных переменных  (pgrb2sp25)
        atmos.5   — 0.50°, ~83 переменных            (pgrb2ap5)
        atmos.5b  — 0.50°, ~500 переменных           (pgrb2bp5)  ← агентные переменные

    Члены ансамбля:
        0      → control run (c00)
        1-30   → perturbation members (p01-p30)

    Источники (в порядке приоритета по умолчанию):
        aws     → s3, архив с 2017-01-01
        google  → GCS, архив с 2021-01-01
        nomads  → официальный, последние ~10 дней, риск блокировки
    """

    SOURCES = {
        "aws":    "https://noaa-gefs-pds.s3.amazonaws.com",
        "google": "https://storage.googleapis.com/gfs-ensemble-forecast-system",
        "nomads": "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod",
        "azure":  "https://noaagefs.blob.core.windows.net/gefs",
    }

    PRODUCTS = {
        "atmos.25": "Quarter degree PRIMARY fields (~35 variables)",
        "atmos.5":  "Half degree PRIMARY fields (~83 variables)",
        "atmos.5b": "Half degree SECONDARY fields (~500 variables) — стратосфера, агент",
    }

    # Маппинг product → директория и суффикс файла
    _PRODUCT_META = {
        "atmos.25": ("atmos/pgrb2sp25", "pgrb2s", "0p25"),
        "atmos.5":  ("atmos/pgrb2ap5",  "pgrb2a", "0p50"),
        "atmos.5b": ("atmos/pgrb2bp5",  "pgrb2b", "0p50"),
    }


    def __init__(
        self,
        # priority: tuple[str, ...], #("aws", "google", "nomads"),
        timeout: int = 10,
    ):
        priority = [
            "aws", 
            # "google", 
            # "nomads",
            # "azure",
        ]
        super().__init__(priority=priority, timeout=timeout)


    ## ------------------------------------------------------------------
    ## Abstract implementations
    ## ------------------------------------------------------------------

    def build_path(
        self,
        run_date: date,
        cycle_hh: int,
        member: int,
        fxx: int,
        product: str,
    ) -> str:
        """
        Строит относительный путь к файлу.

        Пример:
            gefs.20260325/00/atmos/pgrb2sp25/gep01.t00z.pgrb2s.0p25.f024
        """
        if product not in self._PRODUCT_META:
            raise ValueError(
                f"Unknown product '{product}'. Available: {list(self.PRODUCTS)}"
            )

        subdir, file_type, resolution = self._PRODUCT_META[product]
        date_str = run_date.strftime("%Y%m%d")
        member_str = "c00" if member == 0 else f"p{member:02d}"

        filename = (
            f"ge{member_str}.t{cycle_hh:02d}z"
            f".{file_type}.{resolution}.f{fxx:03d}"
        )

        return f"gefs.{date_str}/{cycle_hh:02d}/{subdir}/{filename}"


    def _idx_url(self, file_url: str) -> str:
        """GEFS использует .idx суффикс."""
        return f"{file_url}.idx"


    def _parse_idx(self, idx_text: str, source_url: str) -> pd.DataFrame:
        """
        Парсит wgrib2-style .idx в DataFrame.

        Формат строки:
            {N}:{byte_offset}:d={YYYYMMDDCC}:{variable}:{level}:{forecast}:

        Возвращает DataFrame с колонками:
            grib_message, start_byte, end_byte, date,
            variable, level, forecast, search_this, source_url
        """
        rows = []
        lines = [l for l in idx_text.strip().split("\n") if l.strip()]

        for i, line in enumerate(lines):
            parts = line.split(":")
            if len(parts) < 6:
                continue

            start_byte = int(parts[1])

            # end_byte = начало следующей записи - 1
            if i + 1 < len(lines):
                next_parts = lines[i + 1].split(":")
                try:
                    end_byte = int(next_parts[1]) - 1
                except (IndexError, ValueError):
                    end_byte = None
            else:
                end_byte = None  # последняя переменная — до конца файла

            variable = parts[3].strip()
            level    = parts[4].strip()

            rows.append({
                "grib_message": int(parts[0]),
                "start_byte":   start_byte,
                "end_byte":     end_byte,
                "date":         parts[2].replace("d=", ""),
                "variable":     variable,
                "level":        level,
                "forecast":     parts[5].strip() if len(parts) > 5 else "",
                "search_this":  f"{variable}:{level}",
                "source_url":   source_url,
            })

        return pd.DataFrame(rows)

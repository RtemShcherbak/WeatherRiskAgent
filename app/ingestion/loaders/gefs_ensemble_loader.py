import logging
import aiohttp
import asyncio
import pandas as pd
from pathlib import Path
from datetime import date
from io import StringIO

from app.configs import Constant
from app.ingestion.loaders.base_loader import BaseLoader, LoadTask
from app.ingestion.sources import GEFSSource



class GEFSLoader(BaseLoader):
    def __init__(
            self, 
            raw_dir, 
            source = "gefs", 
            overwrite = False,
        ):
        super().__init__(source, raw_dir, overwrite)
        self.const = Constant()
        self.num_members = 30


    def _generate_tasks(
            self, 
            run_date: date, 
            lead_days: int, 
            cycles: tuple[int],
            step: int = 6,
            var: str = "TMP:2 m above ground",
    ):
        file_descriptions = []
        for lead_day in range(lead_days):
            for cycle in cycles:
                fxx = [(i + (lead_day+1)*24 - cycle) for i in range(0, 24, step)]
                for fxx_ in fxx:
                    for member in range(0, self.num_members, 1):

                        file_desc = {
                            'run_date': run_date,
                            'cycle_hh': cycle,
                            'member': member,
                            'fxx': fxx_,
                            'product': "atmos.25",
                        }
                        file_descriptions.append(file_desc)
        
        source = self.source.select_source(**file_descriptions[0])
        tasks = []
        for file_desc in file_descriptions:
            local_path = (
                self.raw_dir
                / "gefs"
                / file_desc['run_date'].strftime("%Y%m%d")
                / f"{file_desc['cycle_hh']:02d}"
                / ("c00" if file_desc['member'] == 0 else f"p{file_desc['member']:02d}")          # "p01", "p02", "c00"
                / f"f{file_desc['fxx']:03d}.grib2"
            )
            source_file = self.source.get_source_file(source, **file_desc)
            tasks.append(
                LoadTask(
                    url=source_file.url,
                    index_url=source_file.index_url,
                    source=source_file.source,
                    path=source_file.path,
                    run_date=source_file.run_date,
                    fxx=source_file.fxx,
                    product=source_file.product,
                    member=source_file.member,

                    data_type="forecast",
                    local_path=local_path,
                    variable=var,
                )
            )

        return tasks
    

    async def _download_one(
        self,
        session: aiohttp.ClientSession,
        load_task: LoadTask,
        retries: int = 3,
    ) -> LoadTask:
        tmp_path = load_task.local_path.with_suffix(".part")
        tmp_path.parent.mkdir(parents=True, exist_ok=True)

        base_headers = {"User-Agent": "weather-risk-agent/1.0"}

        for attempt in range(1, retries + 1):
            try:
                # 1. Качаем .idx
                async with session.get(
                    load_task.index_url,
                    headers=base_headers,
                    allow_redirects=True,
                ) as r:
                    r.raise_for_status()
                    text = await r.text()

                # 2. Парсим — находим byte_range
                inv = self.source._parse_idx(text, load_task.url)
                matched = self.source.search(load_task.variable, inv)
                row = matched.iloc[0]
                offset = int(row["start_byte"])
                end    = int(row["end_byte"]) if pd.notna(row["end_byte"]) else ""
                byte_range = f"bytes={offset}-{end}"

                # 3. Качаем данные с byte_range
                data_headers = {**base_headers, "Range": byte_range}
                async with session.get(
                    load_task.url,
                    headers=data_headers,
                    allow_redirects=True,
                ) as r:
                    if r.status not in (206,):
                        self.logger.error(f"HTTP {r.status} for {load_task.url}")
                        return load_task

                    with tmp_path.open("wb") as f:
                        async for chunk in r.content.iter_chunked(1024 * 1024):
                            f.write(chunk)

                tmp_path.replace(load_task.local_path)
                load_task.success_request = self._validate_one(load_task.local_path)
                return load_task

            except Exception as e:
                self.logger.debug(
                    f"Attempt {attempt}/{retries} failed "
                    f"member={load_task.member} fxx={load_task.fxx}: "
                    f"{type(e).__name__}: {e}"
                )
                if tmp_path.exists():
                    tmp_path.unlink()
                if attempt == retries:
                    self.logger.error(
                        f"All {retries} attempts failed "
                        f"member={load_task.member} fxx={load_task.fxx}"
                    )

        return load_task


    def _validate_one(self, path: Path) -> bool:
        return path.exists() and path.stat().st_size > 0

    

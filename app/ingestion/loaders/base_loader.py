import logging
import aiohttp
import asyncio

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Iterable, Literal, Any, List, Optional

from app.ingestion.sources import source_map


@dataclass()
class LoadTask:
    ## source description
    url: str
    source: str
    path: str
    run_date: date
    fxx: int
    product: str
    member: Optional[int | str]
    ## load descriprion
    data_type: Literal["forecast", "history"]
    local_path: Path
    variable: str
    start_byte: int
    end_byte: int
    # metadata: dict[str, Any] = field(default_factory=dict)
    success_request: bool = False



class BaseLoader(ABC):
    def __init__(
            self, 
            source: str,
            raw_dir: str, 
            overwrite: bool = False,
        ):
        self.source = source_map[source]
        self.overwrite = overwrite
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(
            f"weather_agent.loader.{source}"
        )
    

    @abstractmethod
    async def _download_one(
        self,
        session: aiohttp.ClientSession,
        load_task: LoadTask,
    ) -> LoadTask:
        """
        Download a single item of data and return its 
        filled LoadTask
        """
        ...    


    @abstractmethod
    def _validate_one(self, path: Path) -> bool:
        """
        Validate one loaded file after _download_one
        """
        ...

    @abstractmethod
    async def _generate_tasks(
        self, 
        var: str,
        run_date: date,
        lead_days: int,
        cycles: tuple[int],
    ) -> List[LoadTask]:
        ...


    
    async def _adownload_all(
        self,
        tasks: List[LoadTask],
        max_concurrent: int = 64,
    ) -> List[LoadTask]:
        semaphore = asyncio.Semaphore(max_concurrent)
        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(
                *[self._bounded(session, task, semaphore) for task in tasks],
                return_exceptions=True,
            )
        return self._collect_results(results)
    

    def download_all(
        self,
        tasks: List[LoadTask],
        max_concurrent: int = 64,
    ) -> List[LoadTask]:
        return asyncio.run(self._adownload_all(tasks, max_concurrent))
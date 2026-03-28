import logging
import aiohttp
import asyncio

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
from tqdm import tqdm
from typing import Iterable, Literal, Any, List, Optional

from app.ingestion.sources import source_map


@dataclass()
class LoadTask:
    ## source description
    url: str
    index_url: str
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
    # start_byte: int
    # end_byte: int
    # # metadata: dict[str, Any] = field(default_factory=dict)
    success_request: bool = False



class BaseLoader(ABC):
    def __init__(
            self, 
            source: str,
            raw_dir: str, 
            overwrite: bool = False,
        ):
        self.source = source_map[source]()
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


    async def _bounded(
        self,
        session: aiohttp.ClientSession,
        task: LoadTask,
        semaphore: asyncio.Semaphore,
    ) -> LoadTask:
        async with semaphore:
            if task.local_path.exists() and not self.overwrite:
                self.logger.debug(f"Skip exists: {task.local_path}")
                task.success_request = True
                return task
            return await self._download_one(session, task)


    def _collect_results(self, results: list) -> List[LoadTask]:
        tasks = []
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"Task exception: {type(result).__name__}: {result}")
            else:
                tasks.append(result)
        return tasks

    
    # async def _adownload_all(
    #     self,
    #     tasks: List[LoadTask],
    #     max_concurrent: int = 64,
    # ) -> List[LoadTask]:
    #     semaphore = asyncio.Semaphore(max_concurrent)
    #     async with aiohttp.ClientSession() as session:
    #         results = await asyncio.gather(
    #             *[self._bounded(session, task, semaphore) for task in tasks],
    #             return_exceptions=True,
    #         )
    #     return self._collect_results(results)

    async def _adownload_all(
        self,
        tasks: List[LoadTask],
        max_concurrent: int,
    ) -> List[LoadTask]:
        semaphore = asyncio.Semaphore(max_concurrent)
        connector = aiohttp.TCPConnector(limit_per_host=max_concurrent)
        
        progress = tqdm(
            total=len(tasks),
            desc="GEFS download",
            unit="file",
            ncols=80,
        )

        async def bounded_with_progress(task):
            result = await self._bounded(session, task, semaphore)
            progress.update(1)
            return result

        async with aiohttp.ClientSession(connector=connector) as session:
            results = await asyncio.gather(
                *[bounded_with_progress(task) for task in tasks],
                return_exceptions=True,
            )
        
        progress.close()
        return self._collect_results(results)


    def download_all(
        self,
        tasks: List[LoadTask],
        max_concurrent: int,
    ) -> List[LoadTask]:
        self.logger.info(f"Start loading {len(tasks)} files")
        results = asyncio.run(self._adownload_all(tasks, max_concurrent))
        success = sum(1 for r in results if r.success_request)
        self.logger.info(f"Done: {success}/{len(tasks)} success")
        return results
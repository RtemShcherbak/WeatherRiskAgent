from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Iterable, Literal, Optional, Sequence, Any, List

import logging


@dataclass()
class LoadTask:
    source: str
    data_type: Literal["forecast", "history"]
    run_date: date
    remote_path: str
    local_path: Path
    variables: tuple[str, ...]
    metadata: dict[str, Any] = field(default_factory=dict)
    success_request: bool = False



class BaseLoader(ABC):
    def __init__(
            self, 
            source: str,
            raw_dir: str, 
            overwrite: bool = False,
        ):
        self.source = source
        self.overwrite = overwrite
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(f"weather_agent.{source}")


    @abstractmethod
    def _generate_requests(self, dates: Iterable[date]) -> Iterable[LoadTask]:
        """
        Generate all download requests for the given date interval
        """
        raise NotImplementedError
    

    @abstractmethod
    def _download_one(self, req: LoadTask) -> LoadTask:
        """
        Download a single item of data and return its 
        filled LoadTask
        """
        raise NotImplementedError
    

    @abstractmethod
    def _validate_one(self, req: LoadTask) -> bool:
        """
        Validate one loaded file after _download_one
        """
        raise NotImplementedError


    def download_all(self, dates: Iterable[date]) -> dict[str, list]:
        success_paths: List[Path] = []
        error_results: List[dict] = []

        for request in self._generate_requests(dates):
            if request.local_path.exists() and not self.overwrite:
                success_paths.append(request.local_path)
                continue

            request = self._download_one(request)
            if request.success_request and self._validate_one(request):
                success_paths.append(request.local_path)
            else:
                error_results.append(asdict(request))

        return {
            "success_paths": success_paths,
            "error_results": error_results
        }
    
    

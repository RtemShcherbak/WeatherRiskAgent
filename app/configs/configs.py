import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field


ROOT_PATH = "/Users/artem/Documents/Zerich/weather_risk_agent/data"


@dataclass
class Constant:
    ## ----------------------------------------------------------------------
    ## Dates
    ## ----------------------------------------------------------------------
    today: pd.Timestamp = pd.Timestamp.today()
    start_date: pd.Timestamp = today - pd.Timedelta(days=90)
    ## ----------------------------------------------------------------------
    ## Dirs
    ## ----------------------------------------------------------------------
    root_dir: Path = Path(ROOT_PATH)
    data_dir: Path = root_dir / "data" / 'datasets'
    forecast_dir: Path = root_dir / "data" / 'datasets' / "forecast"


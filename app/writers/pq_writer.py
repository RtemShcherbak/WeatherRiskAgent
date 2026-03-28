from pathlib import Path
import pandas as pd


REQUIRED_COLS = [
    "region",
    "min",
    "mean",
    "median",
    "max",
    "country",
    "run_date",
    "valid_date",
    "cycle",
    "lead_days",
    "variable",
    "data_source",
]


def _ensure_date_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    # robust conversion: accepts date, datetime, string
    df = df.copy()
    df[col] = pd.to_datetime(df[col], errors="raise").dt.date
    return df


def _normalize_cycle(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # keep as 2-char string: "00","06","12","18"
    df["cycle"] = df["cycle"].astype(str).str.zfill(2)
    return df


def validate_forecast_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and normalize the dataframe according to the fixed schema.
    Returns a normalized copy of df (dates converted, cycle normalized).
    Raises ValueError on any schema / consistency problem.
    """
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    extra = [c for c in df.columns if c not in REQUIRED_COLS]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    if extra:
        raise ValueError(f"Unexpected extra columns: {extra}")

    out = df.copy()

    # Normalize types
    out["region"] = out["region"].astype(str)
    out["country"] = out["country"].astype(str)
    out["variable"] = out["variable"].astype(str)
    out["data_source"] = out["data_source"].astype(str)

    out = _ensure_date_col(out, "run_date")
    out = _ensure_date_col(out, "valid_date")
    out = _normalize_cycle(out)

    # Numeric columns
    for c in ["min", "mean", "median", "max"]:
        out[c] = pd.to_numeric(out[c], errors="raise")

    out["lead_days"] = pd.to_numeric(out["lead_days"], errors="raise").astype(int)

    # Basic sanity checks
    if out.empty:
        raise ValueError("DataFrame is empty")

    # Single-valued metadata per file
    for key in ["country", "variable", "run_date", "valid_date", "cycle"]:
        uniq = out[key].dropna().unique()
        if len(uniq) != 1:
            raise ValueError(f"Column '{key}' must have exactly one unique value per file, got: {uniq}")

    # Date consistency check
    run_date = out["run_date"].iloc[0]
    valid_date = out["valid_date"].iloc[0]
    expected_lead = (pd.Timestamp(valid_date) - pd.Timestamp(run_date)).days
    lead_days = int(out["lead_days"].iloc[0])
    if lead_days != expected_lead:
        raise ValueError(
            f"Inconsistent lead_days: got {lead_days}, expected {expected_lead} "
            f"from valid_date({valid_date})-run_date({run_date})"
        )

    # No missing regions
    if out["region"].isna().any() or (out["region"].str.len() == 0).any():
        raise ValueError("Found empty/NA values in 'region'")

    # Optional: enforce min<=median<=max and min<=mean<=max (not always strictly true but should be)
    bad = (out["min"] > out["max"]).any()
    if bad:
        raise ValueError("Found rows where min > max")

    return out


def write_forecast_parquet(
    df: pd.DataFrame,
    out_dir: str | Path,
    overwrite: bool = False,
    compression: str = "zstd",
) -> Path:
    """
    Write one (country, variable, run_date, cycle, valid_date) dataset to parquet.

    Directory layout:
      out_dir/
        data_source=<data_source>
            country=<country>/
                variable=<variable>/
                    run_date=YYYY-MM-DD/
                        cycle=HH/
                            valid_date=YYYY-MM-DD.parquet
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = validate_forecast_df(df)

    data_source = df["data_source"].iloc[0]
    country = df["country"].iloc[0]
    variable = df["variable"].iloc[0]
    run_date = df["run_date"].iloc[0].isoformat()
    cycle = df["cycle"].iloc[0]
    valid_date = df["valid_date"].iloc[0].isoformat()

    target_dir = (
        out_dir
        / f"data_source={data_source}"
        / f"country={country}"
        / f"variable={variable}"
        / f"run_date={run_date}"
        / f"cycle={cycle}"
    )
    target_dir.mkdir(parents=True, exist_ok=True)

    target_path = target_dir / f"valid_date={valid_date}.parquet"

    if target_path.exists() and not overwrite:
        raise FileExistsError(f"File already exists: {target_path} (set overwrite=True to replace)")

    # Make writing deterministic
    df_out = df.sort_values("region").reset_index(drop=True)

    df_out.to_parquet(
        target_path,
        engine="pyarrow",
        compression=compression,
        index=False,
    )

    return target_path

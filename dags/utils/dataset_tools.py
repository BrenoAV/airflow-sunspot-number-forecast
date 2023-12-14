from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def load_dataset(dataset_path: str) -> pd.DataFrame:
    df = pd.read_parquet(dataset_path, engine="fastparquet")
    return df


def save_dataset(df_copy: pd.DataFrame, dataset_out_path: str) -> None:
    table = pa.Table.from_pandas(df_copy)
    pq.write_table(table, dataset_out_path)


def filter_year(df_copy: pd.DataFrame, year: int) -> pd.DataFrame:
    """Remove date below of the year specified

    Args:
        df (pd.DataFrame): DataFrame to be filtered
        year (int): Threshold

    Returns:
        pd.DataFrame: DataFrame that the years are >= `year`
    """
    assert "year" in df_copy.columns, "You need to specify the `year` column"
    return df_copy[df_copy["year"] >= year]


def split_train_test(
    df_copy: pd.DataFrame, train_end_date: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    mask = df_copy["date"] < train_end_date
    train_df = df_copy[mask].set_index("date", drop=True).asfreq("D")
    test_df = (
        df_copy[df_copy["date"] >= train_end_date]
        .set_index("date", drop=True)
        .asfreq("D")
    )
    return train_df, test_df

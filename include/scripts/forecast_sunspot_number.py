import argparse
import logging
import tempfile
from datetime import datetime

import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from dags.utils.dataset_tools import (
    filter_year,
    load_dataset,
    save_dataset,
    split_train_test,
)

logging.basicConfig(level=logging.INFO)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-sd", "--start_date", type=str, required=True)
    parser.add_argument("-ed", "--end_date", type=str, required=True)
    parser.add_argument("-dp", "--dataset_path", type=str, required=True)

    return parser.parse_args()


def main():
    args = get_args()
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    diff = (end_date - start_date).days
    logging.info("Total of days to forecast %d", diff)
    df = load_dataset(args.dataset_path)
    logging.info("Dataset %s loaded!", args.dataset_path)
    df = filter_year(df.copy(), 1850)
    logging.info("Dataset filtered (year >= 1850)")

    train_df, test_df = split_train_test(df, start_date.strftime("%Y-%m-%d"))
    logging.info("Train shape: %s", train_df.shape)
    logging.info("Test shape: %s", test_df.shape)

    model_lts_add = ExponentialSmoothing(
        endog=train_df["daily_sunspot_number"],
        trend=None,
        seasonal="add",
        # seasonal_periods=365 * 11,  # 11 years
        freq="D",
    ).fit()
    train_df["HWL"] = model_lts_add.fittedvalues.copy()
    # Forecast
    y_pred = model_lts_add.forecast(diff)
    # Predictions below to zero are not possible
    y_pred[(y_pred < 0)] = 0
    y_pred = y_pred.to_frame(name="daily_sunspot_number")
    y_pred["year"] = y_pred.index.year
    y_pred["month"] = y_pred.index.month
    y_pred["day"] = y_pred.index.day
    # Calculating the monthly mean for testing values
    monthly_test_df = (
        test_df.groupby(by=["year", "month"])["daily_sunspot_number"]
        .mean()
        .reset_index()
    )
    monthly_test_df.rename(
        columns={"daily_sunspot_number": "test_monthly_sunspot_number"}, inplace=True
    )
    monthly_test_df["date"] = monthly_test_df[["year", "month"]].apply(
        lambda row: datetime(year=row["year"], month=row["month"], day=1), axis=1
    )
    # Calculating the monthly mean for predicted values
    monthly_pred_df = (
        y_pred.groupby(by=["year", "month"])["daily_sunspot_number"]
        .mean()
        .reset_index()
    )
    monthly_pred_df.rename(
        columns={"daily_sunspot_number": "pred_monthly_sunspot_number"}, inplace=True
    )
    monthly_pred_df["date"] = monthly_pred_df[["year", "month"]].apply(
        lambda row: datetime(year=row["year"], month=row["month"], day=1), axis=1
    )
    # Merge pred and test one dataset
    final_monthly_df = pd.merge(
        left=monthly_test_df,
        right=monthly_pred_df,
        on=["date", "year", "month"],
        how="outer",
    )
    final_monthly_df = final_monthly_df[
        ["date", "test_monthly_sunspot_number", "pred_monthly_sunspot_number"]
    ]
    with tempfile.NamedTemporaryFile(
        "wb", suffix=".parquet", delete=False
    ) as temp_file:
        save_dataset(
            final_monthly_df,
            dataset_out_path=temp_file.name,
        )
        print(temp_file.name)


if __name__ == "__main__":
    main()

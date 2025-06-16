from argparse import ArgumentParser
from datetime import date
from pathlib import Path

import duckdb
from yaml import safe_load

from src.utils import load_csv_to_table

parser = ArgumentParser()
parser.add_argument("--date_from", type=date.fromisoformat, default=date.today())
parser.add_argument("--date_to", type=date.fromisoformat, default=date.today())


def main():
    """
    Main function to load CSV files into DuckDB tables based on the configuration.
    """

    args = parser.parse_args()
    cfg = safe_load(Path("ingest.yaml").read_text())
    metrics_query = Path("metrics.sql").read_text()

    with duckdb.connect("casino.db") as con:
        for source in cfg["sources"]:
            load_csv_to_table(con, source)
        df_metric_results = con.sql(metrics_query, params=args.__dict__)
        print(df_metric_results)


if __name__ == "__main__":
    main()

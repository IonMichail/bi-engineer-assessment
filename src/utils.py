from typing import Any

from duckdb import DuckDBPyConnection, SQLExpression, StarExpression
from loguru import logger


def load_csv_to_table(
    con: DuckDBPyConnection, config: dict[str, Any], overwrite: bool = True
):
    """
    Load a CSV file into a DuckDB table.

    Args:
        con (DuckDBPyConnection): The DuckDB connection object.
        config (dict[str, Any]): A dictionary with parameters for reading the CSV file.
    """
    _source_config = config.copy()
    file = _source_config["load_config"]["path_or_buffer"]
    table = _source_config["write_config"]["table"]
    try:
        logger.info(f"Ingesting file {file} into table {table}...")
        if overwrite:
            con.sql(f"DROP TABLE IF EXISTS {table};")

        df_raw = con.read_csv(**_source_config["load_config"])
        transform_config = _source_config["write_config"].get("transform", {})
        transform_expressions = [
            SQLExpression(expr).alias(colname)
            for colname, expr in transform_config.items()
        ]
        identity_expressions = StarExpression(exclude=transform_config.keys())
        df_curated = df_raw.select(identity_expressions, *transform_expressions)
        con.sql(f"CREATE TABLE {table} AS SELECT * FROM df_curated;")
        logger.info(f"Successfully loaded file {file} into table {table}.")
    except Exception as e:
        logger.error(f"Error loading file: {file} to table: {e}", exc_info=True)
        raise

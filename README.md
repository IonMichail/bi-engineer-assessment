## Environment Prepartion

## Data Ingestion
The data are ingested using the DuckDB function read_csv from Python. The data ingestion is a implemented as a two step procedure. First step is to load each csv file as in memory and enforce column names and appropriate types. The second step is to apply any (if required) data cleaning transformations and persist the files as tables in the database.

The configurations of each source are specified on the `ingest.yaml` file. For each csv file there are two sections
    
    - load_config: A mapping of values passed to the read_csv function
    - write_config: The table name to be created for the csv file and an optional mapping of transformations to apply to columns before inserting.


ingest.yaml:
```
sources:
  - load_config:
      path_or_buffer: ../data/casinodaily.csv
      date_format: "%Y-%m-%d"
      columns:
        UserID: varchar
        CountryId: varchar
        CurrencyId: varchar
        Date: date
        CasinoGameId: varchar
        CasinoProviderId: varchar
        CasinoManufacturerId: varchar
        GGR: float
        Returns: float
    write_config:
      table: casino_daily

  - load_config:
      path_or_buffer: ../data/casinomanufacturers.csv
      timestamp_format: "%Y-%m-%d %H:%M:%S.%f"
      columns:
        CasinoManufacturerId: varchar
        CasinoManufacturerName: varchar
        FromDate: timestamp
        ToDate: timestamp
        LatestFlag: bool
    write_config:
      table: casino_manufacturers
      transform:
        CasinoManufacturerId: REPLACE(CasinoManufacturerId, '"', '')
        CasinoManufacturerName: REPLACE(CasinoManufacturerName, '"', '')
        LatestFlag: CAST(REPLACE(LatestFlag, '"', '') AS BOOL)
    ...
```
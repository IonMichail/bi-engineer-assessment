## Environment Preparation & Pipeline Execution
Ensure that you have Python >= 3.10 installed.

1. Open a terminal in the root of the project directory create a virtual environment and execute the following command:
```
python -m venv .venv
```
2. Activate the virtual environment:
```
.venv/Scripits/Activate/.ps1
```

3. Install Dependencies:
```
pip install -r requirements.txt
```

4. To execute the pipeline, execute the following command from inside the virtual environment:
```
py main.py --date_from <date range start> --date_to <date range end>
```
`date_from` & `date_to` should be provided in isoformat (e.g. 2025-01-01). 

Parameters `date_from` & `date_to` are optional. If either is not specified then the current date is passed.

Each execution of the pipeline persists the metric results a table named `metrics` Previous results are overwriten.

5. To inspect the results use the [results-nb](results-nb.ipynb) notebook. Use the .venv virtual environment as kernel


## Data Ingestion
The data are ingested using the DuckDB function `read_csv` from Python. The data ingestion is a implemented as a two step procedure:
  1. Load each csv file as in memory and enforce column names and appropriate types. 
  2. Apply any (if required) data cleaning transformations and persist the files as tables in the database.

The configurations of each source are specified on the  [ingest.yaml](ingest.yaml) file. For each csv file there are two fields:
    
    - load_config: A mapping of parameter values passed to the read_csv function
    - write_config: The table name to be created for the csv file and an optional mapping of transformations to apply to columns before inserting.


ingest.yaml:
```
sources:
  - load_config:
      path_or_buffer: data/casinodaily.csv
      date_format: "%Y-%m-%d"
      columns:
        UserID: varchar
        CountryId: int
        CurrencyId: int
        Date: date
        CasinoGameId: int
        CasinoProviderId: int
        CasinoManufacturerId: int
        GGR: float
        Returns: float
    write_config:
      table: casino_daily

  - load_config:
      path_or_buffer: data/casinomanufacturers.csv
      timestamp_format: "%Y-%m-%d %H:%M:%S.%f"
      columns:
        CasinoManufacturerId: varchar
        CasinoManufacturerName: varchar
        FromDate: timestamp
        ToDate: timestamp
        LatestFlag: varchar
    write_config:
      table: casino_manufacturers
      transform:
        CasinoManufacturerId: CAST(REPLACE(CasinoManufacturerId, '"', '') AS INT)
        CasinoManufacturerName: REPLACE(CasinoManufacturerName, '"', '')
        LatestFlag: CAST(REPLACE(LatestFlag, '"', '') AS BOOL)

  - load_config:
      path_or_buffer: data/casinoproviders.csv
      columns:
        CasinoProviderId: varchar
        CasinoProviderName: varchar
    write_config:
      table: casino_providers
      transform:
        CasinoProviderId: CAST(CasinoProviderId AS INT)

  - load_config:
      path_or_buffer: data/currencyrates.csv
      date_format: "%Y-%m-%d"
      columns:
        Date: date
        FromCurrencyId: int
        ToCurrencyId: int
        ToCurrencySysname: varchar
        EuroRate: float
    write_config:
      table: currency_rates
      transform:
        FromCurrencyId: CAST(FromCurrencyId AS INT)
        ToCurrencyId: CAST(ToCurrencyId AS INT)

  - load_config:
      path_or_buffer: data/users.csv
      date_format: "%Y-%m-%d"
      columns:
        UserID: varchar
        user_id: varchar
        BirthDate: date
        Sex: varchar
        VIPStatus: varchar
        Country: varchar
        CountryId: int
    write_config:
      table: users
      transform:
        UserID: CAST(UserID AS INT)
        user_id: REPLACE(user_id, '"', '')
        VIPStatus: CASE
          WHEN UPPER(VIPStatus) = 'NOT VIP' THEN UPPER(VIPStatus)
          ELSE UPPER(REGEXP_REPLACE(VIPStatus, '(\b)*\s(\b)*', '', 'g'))
          END

```
For each source file the columns are explicitely defined (`columns` field) and a data type is assigned. Columns that could not be cast to an appropriate data type on load are ingested as varchar and the transformations specified in the `write_config` are applied perform any additional processing required. The required transformations and and data types to cast to were identified by iteratively specifying the data types, catching errors and applying transformations to fix them.

## ETL Query
The ETL query is defined in the `metrics.sql` file. The query defines 5 CTEs:
1. manufactures_snapshot: It retrieves the latest manufacturer recod from the table based on the latest FromDate for each CasinoManufacturerId value.

2. filtered_casino_daily: It limits the casino daily rows to the range specified by the parameters $date_from & $date_to

3. all_currency_rates: It creates a bidirectiona exchange rate mapping between currency pairs with conversion rates to and from each currency.

4. granular_metrics: All available tables are joined, user Age is calculated as the difference between the transaction date and the birth date of the user. users & all_currency_rates are inner joined with casino_daily table to exclude users & currencies from the casino_daily table that are not included in the users & currencies tables


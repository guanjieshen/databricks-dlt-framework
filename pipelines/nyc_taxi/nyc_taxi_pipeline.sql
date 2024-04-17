CREATE
OR REFRESH STREAMING TABLE raw_data AS
SELECT
    *
FROM
    STREAM read_files(
        'abfss://container@storageAccount.dfs.core.windows.net/base/path'
    );
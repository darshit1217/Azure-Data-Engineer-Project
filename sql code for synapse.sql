select TOP 100 *
FROM
OPENROWSET(
    BULK 'https://olistdatastorageaccdjain.dfs.core.windows.net/olistdata/silver/',
    FORMAT = 'PARQUET'
) AS result1;


CREATE schema gold

CREATE VIEW gold.final AS
select TOP 100 *
FROM
OPENROWSET(
    BULK 'https://olistdatastorageaccdjain.dfs.core.windows.net/olistdata/silver/',
    FORMAT = 'PARQUET'
) AS result1;

SELECT * from gold.final;


CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'azureolistsynapse@123';
CREATE DATABASE SCOPED CREDENTIAL darshadmin WITH IDENTITY = 'Managed Identity';

SELECT * FROM sys.database_credentials;

CREATE EXTERNAL FILE FORMAT extfileformat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

CREATE EXTERNAL DATA SOURCE goldlayer WITH (
    LOCATION = 'https://olistdatastorageaccdjain.dfs.core.windows.net/olistdata/gold/',
    CREDENTIAL = darshadmin
);

CREATE EXTERNAL TABLE gold.finaltable WITH (
        LOCATION = 'finalServing',
        DATA_SOURCE = goldlayer,
        FILE_FORMAT = extfileformat
) AS
SELECT * FROM gold.final2;

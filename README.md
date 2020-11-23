# mssql2postgres
Migrate Data from MsSQL to PostgreSQL


## usage:
```
Usage:
  Mssql2Postgres [options] <--from> <--to>

Arguments:
  <--from>    Source Server (MS-Sql) connection string
  <--to>      Destination Server (Postgres) connection string

Options:
  --batch <batch>          Batch Size on Insert queries default 1000
  -o, --output <output>    Output file location
  -e, --execute            Execute generated sql commands in destination server
  --version                Show version information
  -?, -h, --help           Show help and usage information

```

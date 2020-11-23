using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Npgsql;

namespace Mssql2Postgres
{
    public class PgSql : ISql<NpgsqlDataReader>
    {
        private readonly string _connectionString;
        private readonly string _databaseName;
        public string Database => _databaseName;

        public PgSql(string connectionString, string databaseName)
        {
            _connectionString = connectionString;
            _databaseName = databaseName;
        }

        private NpgsqlConnection _activeConnection;

        private NpgsqlConnection ActiveConnection
        {
            get
            {
                if (_activeConnection == null || _activeConnection.State != ConnectionState.Open)
                    ReConnect();
                return _activeConnection;
            }
        }


        public void ReConnect()
        {
            _activeConnection?.Close();
            _activeConnection = new NpgsqlConnection(_connectionString);
            _activeConnection.Open();
        }

        public T ExecuteScalar<T>(string query)
        {
            using (var cmd = ActiveConnection.CreateCommand())
            {
                cmd.CommandText = query;
                return (T) cmd.ExecuteScalar();
            }
        }

        public IEnumerable<T> ExecuteReader<T>(string query, Func<NpgsqlDataReader, T> parser)
        {
            using (var cmd = ActiveConnection.CreateCommand())
            {
                cmd.CommandText = query;
                var reader = cmd.ExecuteReader();
                using (reader)
                {
                    while (reader.Read())
                    {
                        var result = parser(reader);
                        yield return (T) result;
                    }
                }
            }
        }

        public int ExecuteNonQuery(string query)
        {
            using (var cmd = ActiveConnection.CreateCommand())
            {
                cmd.CommandText = query;
                return cmd.ExecuteNonQuery();
            }
        }

        public DataTable ExecuteRawQuery(string query)
        {
            var dataTable = new DataTable();
            {
                using (var command = ActiveConnection.CreateCommand())
                {
                    command.CommandText = query;

                    using (var adapter = new NpgsqlDataAdapter())
                    {
                        adapter.SelectCommand = command;
                        adapter.Fill(dataTable);
                    }
                }
            }
            return dataTable;
        }

        private const string COLLATE = "COLLATE SQL_Latin1_General_CP1_CI_AS";

        /// <returns>System.String.</returns>
        public string GetReferenceColumnName(string referencedTableName, string referencingTableName)
        {
            var query = $@"select referencing_column_name from (SELECT    
OBJECT_NAME(fkeys.constraint_object_id) foreign_key_name
,OBJECT_NAME(fkeys.parent_object_id) referencing_table_name
,COL_NAME(fkeys.parent_object_id, fkeys.parent_column_id) referencing_column_name
,OBJECT_SCHEMA_NAME(fkeys.parent_object_id) referencing_schema_name
,OBJECT_NAME (fkeys.referenced_object_id) referenced_table_name
,COL_NAME(fkeys.referenced_object_id, fkeys.referenced_column_id) 
referenced_column_name
,OBJECT_SCHEMA_NAME(fkeys.referenced_object_id) referenced_schema_name
FROM sys.foreign_key_columns AS fkeys) X
where referenced_table_name={referencedTableName} {COLLATE} 
AND
referencing_table_name={referencingTableName}  {COLLATE}";
            return ExecuteScalar<string>(query);
        }

        /// <summary>
        /// Determines whether [is column from table] [the specified column name].
        /// </summary>
        /// <param name="columnName">Name of the column.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <returns><c>true</c> if [is column from table] [the specified column name]; otherwise, <c>false</c>.</returns>
        public bool IsColumnFromTable(string columnName, string tableName)
        {
            var query =
                $@"
 IF (EXISTS(SELECT 1 from INFORMATION_SCHEMA.COLUMNS where 
    TABLE_NAME={tableName}  {COLLATE} 
AND 
    COLUMN_NAME={columnName}    {COLLATE}))
 BEGIN
 select CAST(1 as bit);
 END;
 ELSE 
 BEGIN
 select CAST(0 as bit);
 END";
            return ExecuteScalar<bool>(query);
        }


        /// <summary>
        /// Corrects the name of the table.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns>System.String.</returns>
        public string CorrectTableName(string tableName)
        {
            if (tableName.IsNullOrEmpty())
                return null;

            tableName = tableName.Trim("[]".ToCharArray());

            var query =
                $"select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_NAME={tableName} {COLLATE};";

            var result = ExecuteScalar<string>(query);

            return result;
        }

        /// <summary>
        /// Corrects the name of the column.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="columnName">Name of the column.</param>
        /// <returns>System.String.</returns>
        public string CorrectColumnName(string tableName, string columnName)
        {
            if (tableName.IsNullOrEmpty()) return null;
            if (columnName.IsNullOrEmpty()) return null;
            var query =
                $"select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME={tableName} {COLLATE} AND COLUMN_NAME={columnName} {COLLATE};";
            return ExecuteScalar<string>(query);
        }


        public string FindColumnTableName(string columnName, string _baseTableName, List<string> _joinedTablesList)
        {
            var queryBuilder = new StringBuilder("");
            var format = $@"
 IF (EXISTS(SELECT 1 from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME={{0}} {COLLATE} AND COLUMN_NAME={columnName} {COLLATE}))
 BEGIN
 select {{0}}
 END;";
            //=========================================================

            queryBuilder.AppendLine(string.Format(format, _baseTableName));
            foreach (var tableName in _joinedTablesList)
            {
                queryBuilder.AppendLine(string.Format(format, tableName));
            }

            var query = queryBuilder.ToString();
            var ownTable = ExecuteScalar<string>(query);
            return ownTable;
        }

        /// <summary>
        /// Finds the name of the column fk table.
        /// </summary>
        /// <param name="ColumnName">Name of the column.</param>
        /// <param name="_baseTableName">Name of the base table.</param>
        /// <param name="_joinedTablesList">The joined tables list.</param>
        /// <returns>System.String.</returns>
        public string FindColumnFKTableName(string ColumnName, string _baseTableName, List<string> _joinedTablesList)
        {
            var queryBuilder = new StringBuilder("");
            var format = $@"
 IF (EXISTS(SELECT 1 from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME={{0}} {COLLATE} AND COLUMN_NAME={ColumnName} {COLLATE}))
 BEGIN
 select {{0}}
 END;";
            //=========================================================

            queryBuilder.AppendLine(string.Format(format, _baseTableName));
            foreach (var tableName in _joinedTablesList)
            {
                queryBuilder.AppendLine(string.Format(format, tableName));
            }

            var query = queryBuilder.ToString();
            var ownTable = ExecuteScalar<string>(query);
            //=========================================================
            queryBuilder.Clear();
            queryBuilder.AppendLine("declare @referenced_table_name NVARCHAR(MAX)=null;");
            query = $@"
declare @referenced_table_name NVARCHAR(MAX)=null;
set @referenced_table_name=null;
select @referenced_table_name=referenced_table_name from (SELECT    
OBJECT_NAME(fkeys.constraint_object_id) foreign_key_name
,OBJECT_NAME(fkeys.parent_object_id) referencing_table_name
,COL_NAME(fkeys.parent_object_id, fkeys.parent_column_id) referencing_column_name
,OBJECT_SCHEMA_NAME(fkeys.parent_object_id) referencing_schema_name
,OBJECT_NAME (fkeys.referenced_object_id) referenced_table_name
,COL_NAME(fkeys.referenced_object_id, fkeys.referenced_column_id) 
referenced_column_name
,OBJECT_SCHEMA_NAME(fkeys.referenced_object_id) referenced_schema_name
FROM sys.foreign_key_columns AS fkeys) X
where X.referencing_column_name={ColumnName}  {COLLATE}
and referencing_table_name={ownTable}  {COLLATE}

if(@referenced_table_name is not null)
BEGIN
select @referenced_table_name
END";

            var fkTable = ExecuteScalar<string>(query);
            return fkTable;
        }

        /// <summary>
        /// returns if sql is valid sql
        /// </summary>
        /// <param name="sqlSelectQuery">The SQL select query.</param>
        /// <returns><c>true</c> if XXXX, <c>false</c> otherwise.</returns>
        public bool ExecuteNoExec(string sqlSelectQuery)
        {
            ExecuteNonQuery("set NOEXEC ON");

            bool result;

            try
            {
                ExecuteNonQuery(sqlSelectQuery);
                result = true;
            }
            catch
            {
                result = false;
            }

            ExecuteNonQuery("set NOEXEC OFF");

            return result;
        }

        /// <summary>
        /// Gets the column names from SQL query.
        /// </summary>
        /// <param name="sqlSelectQuery">The SQL select query.</param>
        /// <returns>System.String[].</returns>
        public string[] GetColumnNamesFromSqlQuery(string sqlSelectQuery)
        {
            var dataTable = ExecuteRawQuery(sqlSelectQuery);

            return dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
        }

        public object GetColumnType(string tableName, string colName)
        {
            throw new System.NotImplementedException();
        }

        public ColumnInfo[] GetColumns(string tableName)
        {
            var query = $@"select * from {_databaseName}.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME=N'{tableName}'";
            var dt = ExecuteReader(query, reader => new ColumnInfo
            {
                TABLE_CATALOG = reader["TABLE_CATALOG"].ToString(),
                TABLE_SCHEMA = reader["TABLE_SCHEMA"].ToString(),
                TABLE_NAME = reader["TABLE_NAME"].ToString(),
                COLUMN_NAME = reader["COLUMN_NAME"].ToString(),
                ORDINAL_POSITION = reader["ORDINAL_POSITION"].ToString(),
                COLUMN_DEFAULT = reader["COLUMN_DEFAULT"].ToString(),
                IS_NULLABLE = reader["IS_NULLABLE"].ToString(),
                DATA_TYPE = reader["DATA_TYPE"].ToString(),
                CHARACTER_MAXIMUM_LENGTH = reader["CHARACTER_MAXIMUM_LENGTH"].ToString(),
                CHARACTER_OCTET_LENGTH = reader["CHARACTER_OCTET_LENGTH"].ToString(),
                NUMERIC_PRECISION = reader["NUMERIC_PRECISION"].ToString(),
                NUMERIC_PRECISION_RADIX = reader["NUMERIC_PRECISION_RADIX"].ToString(),
                NUMERIC_SCALE = reader["NUMERIC_SCALE"].ToString(),
                DATETIME_PRECISION = reader["DATETIME_PRECISION"].ToString(),
                CHARACTER_SET_CATALOG = reader["CHARACTER_SET_CATALOG"].ToString(),
                CHARACTER_SET_SCHEMA = reader["CHARACTER_SET_SCHEMA"].ToString(),
                CHARACTER_SET_NAME = reader["CHARACTER_SET_NAME"].ToString(),
                COLLATION_CATALOG = reader["COLLATION_CATALOG"].ToString(),
                COLLATION_SCHEMA = reader["COLLATION_SCHEMA"].ToString(),
                COLLATION_NAME = reader["COLLATION_NAME"].ToString(),
                DOMAIN_CATALOG = reader["DOMAIN_CATALOG"].ToString(),
                DOMAIN_SCHEMA = reader["DOMAIN_SCHEMA"].ToString(),
                DOMAIN_NAME = reader["DOMAIN_NAME"].ToString()
            }).ToArray();
            //var cols = GetColumnNamesFromSqlQuery(query).Select(s => $@"public string {s}{{get;set;}}");
            //var code = string.Join("\n", cols);
            return dt;
        }
    }
}
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.CommandLine.IO;
using System.IO;
using System.Linq;
using System.Text;

namespace Mssql2Postgres
{
    class Program
    {
        static IEnumerable<string> ProcessTable(Mssql mssql,TableInfo table,int batchSize)
        {
            var sb = new StringBuilder();
            var query = $"select * from {mssql.Database}.{table.TABLE_SCHEMA}.{table.TABLE_NAME}";
            var actualData = mssql.ExecuteReader(query, reader =>
            {
                var dict = new Dictionary<string, CellValue>();
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    var fieldName = reader.GetName(i);
                    var fieldType = reader.GetFieldType(i);
                    var fieldValue = reader[i];
                    var cell = new CellValue(fieldValue, fieldType);
                    dict.Add(fieldName, cell);
                }
                return dict;
            });
            foreach (var dArr in actualData.Batch(batchSize))
            {
                if (dArr.Length > 0)
                {
                    sb.Append($@"insert into ""{table.TABLE_NAME}"" (");
                    var columns = dArr[0].Keys.ToArray();

                    sb.Append(string.Join(",", columns.Select(c => $@"""{c}""")));
                    sb.Append(") values");
                    var first = true;
                    foreach (var d in dArr)
                    {
                        if (!first)
                        {
                            sb.Append(",");
                        }

                        first = false;
                        sb.Append("(");
                        sb.Append(string.Join(",", d.Values.Select(c => c.ToSql())));
                        sb.Append(")");
                    }
                }
                sb.Append(";");
                yield return sb.ToString();
                sb.Clear();
            }
        }
        public static void Main(string[] args)
        {
//#if DEBUG
//            args = new[]
//            {
//                "data source=MATRIX\\SERVER17;Initial Catalog=my_database;Integrated Security=True;",
//                "--to",
//                "Server=localhost;Port=5432;Database=msk4;User Id=postgres;Password=super_secure_password;",
//                //"--output",
//                //"output.pg.sql",
//                "--batch",
//                "100000"
//            };
//#endif
            var cmd = new RootCommand
            {
                new Argument<string>("--from", "Source Server (MS-Sql) connection string"),
                new Argument<string>("--to", "Destination Server (Postgres) connection string"),
                new Option<int>("--batch", "Batch Size on Insert queries default 1000"),
                new Option<string>(new[] {"--output", "-o"}, "Output file location"),
                new Option<bool?>(new[] {"--execute", "-e"}, "Execute generated sql commands in destination server"),
            };
            cmd.Handler = CommandHandler.Create<string, string, int,string, bool?,IConsole>(Migrate);
            cmd.Invoke(args);
        }

        private static void Migrate(string @from, string to,int batch, string output, bool? execute, 
            IConsole console)
        {
            void log(string line)
            {
                console.Out.WriteLine("[INFO]\t" + line);
            }
            void error(Exception e)
            {
                console.Out.WriteLine("[ERROR]\t" + e.Message);
            }
            log("testing connection to mssql server...");
            Mssql mssql;
            try
            {
                mssql = new Mssql(@from);
                log("ok");
            }
            catch (Exception e)
            {
                error(e);
                return;
            }

            if (!string.IsNullOrWhiteSpace(to))
            {
                PgSql pgSql;
                log("testing connection to pgsql server...");
                try
                {
                    pgSql = new PgSql(to, null);
                    log("ok");
                }
                catch (Exception e)
                {
                    error(e);
                    return;
                }

                log("getting tables list from mssql server...");
                TableInfo[] tables;
                try
                {
                    tables = mssql.GetTablesList();
                }
                catch (Exception e)
                {
                    error(e);
                    return;
                }

                TextWriter tw = null;
                if (!string.IsNullOrWhiteSpace(output))
                {
                    if (File.Exists(output))
                        File.Delete(output);
                    tw=new StreamWriter(File.OpenWrite(output));
                }
                foreach (var table in tables)
                {
                    log("processing table " + table.TABLE_SCHEMA + "." + table.TABLE_NAME);
                    pgSql.ExecuteNonQuery($@"truncate table ""{table.TABLE_NAME}""");
                    var sqlList = ProcessTable(mssql, table, batch);
                    var counter = 1;
                    foreach (var sql in sqlList)
                    {
                        tw?.Write(sql);
                        ++counter;
                        log("executing next batch...");
                        try
                        {
                            var result = pgSql.ExecuteNonQuery(sql);
                            log($"{counter}:\t{result} row(s) affected.");
                        }
                        catch (Exception e)
                        {
                            tw?.Flush();
                            tw?.Close();
                            error(e);
                            Console.ReadKey();
                        }
                        tw?.Write(sql);
                        tw?.Flush();
                    }
                    tw?.Flush();
                }
                tw?.Close();
            }
        }
    }
}

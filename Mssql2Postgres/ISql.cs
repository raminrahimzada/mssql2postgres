using System;
using System.Collections.Generic;
using System.Data;

namespace Mssql2Postgres
{
    public interface ISql<out TReader>
    {
        string Database { get; }
        void ReConnect();
        T ExecuteScalar<T>(string query);
        IEnumerable<T> ExecuteReader<T>(string query, Func<TReader, T> parser);
        int ExecuteNonQuery(string query);
        DataTable ExecuteRawQuery(string query);
    }
}
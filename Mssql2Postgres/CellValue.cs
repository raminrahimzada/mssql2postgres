using System;

namespace Mssql2Postgres
{
    public class CellValue
    {
        public object FieldValue { get; }
        public Type FieldType { get; }

        public CellValue(object fieldValue, Type fieldType)
        {
            FieldValue = fieldValue;
            FieldType = fieldType;
        }

        public string ToSql()
        {
            if (FieldValue == null) return "NULL";
            if (FieldValue == System.DBNull.Value) return "NULL";
            if (FieldType == typeof(string))
            {
                var f = FieldValue + string.Empty;
                f = f.Replace("'", "''");
                return $"'{f}'";
            }
            
            if (FieldType == typeof(int))
            {
                return $"{FieldValue}";
            }
            if (FieldType == typeof(byte))
            {
                return $"{FieldValue}";
            }
            if (FieldType == typeof(short))
            {
                return $"{FieldValue}";
            }
            if (FieldType == typeof(long))
            {
                return $"{FieldValue}";
            }

            throw new NotImplementedException(
                $"Type-{FieldType} is not implemented in CellValue.ToSql method,pls implement it yourself");

            return null;
        }
    }
}
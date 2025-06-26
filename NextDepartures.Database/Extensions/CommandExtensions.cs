namespace NextDepartures.Database.Extensions;

using System;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;

public static class CommandExtensions
{
    public static void AddWithValue(this DbCommand command, string parameterName, object value)
    {
        switch (command)
        {
            case SqliteCommand sqliteCommand:
                sqliteCommand.Parameters.AddWithValue(parameterName, value);
                break;
            case SqlCommand sqlCommand:
                sqlCommand.Parameters.AddWithValue(parameterName, value);
                break;
            default:
                throw new NotSupportedException("Unsupported command type");
        }
    }
}

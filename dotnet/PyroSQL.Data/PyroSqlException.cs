using System.Data.Common;

namespace PyroSQL.Data;

/// <summary>
/// The exception thrown when PyroSQL returns an error or a protocol violation occurs.
/// </summary>
public sealed class PyroSqlException : DbException
{
    /// <summary>
    /// The SQLSTATE code returned by the server, or null if the error is client-side.
    /// </summary>
    public new string? SqlState { get; }

    public PyroSqlException(string message) : base(message) { }

    public PyroSqlException(string message, Exception innerException) : base(message, innerException) { }

    public PyroSqlException(string sqlState, string message) : base(message)
    {
        SqlState = sqlState;
    }
}

using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace PyroSQL.Data;

/// <summary>
/// Represents a parameter to a PyroSqlCommand.
/// </summary>
public sealed class PyroSqlParameter : DbParameter
{
    private string _parameterName = "";
    private string _sourceColumn = "";

    public PyroSqlParameter() { }

    public PyroSqlParameter(string parameterName, object? value)
    {
        _parameterName = parameterName ?? "";
        Value = value;
    }

    public PyroSqlParameter(string parameterName, DbType dbType)
    {
        _parameterName = parameterName ?? "";
        DbType = dbType;
    }

    public override DbType DbType { get; set; } = DbType.String;

    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;

    public override bool IsNullable { get; set; }

    [AllowNull]
    public override string ParameterName
    {
        get => _parameterName;
        set => _parameterName = value ?? "";
    }

    public override int Size { get; set; }

    [AllowNull]
    public override string SourceColumn
    {
        get => _sourceColumn;
        set => _sourceColumn = value ?? "";
    }

    public override bool SourceColumnNullMapping { get; set; }

    public override object? Value { get; set; }

    public override void ResetDbType()
    {
        DbType = DbType.String;
    }

    /// <summary>
    /// Converts the parameter value to its wire-format string representation.
    /// </summary>
    internal string ToWireString()
    {
        if (Value == null || Value == DBNull.Value)
            return "";

        return Value switch
        {
            string s => s,
            bool b => b ? "1" : "0",
            byte v => v.ToString(CultureInfo.InvariantCulture),
            short v => v.ToString(CultureInfo.InvariantCulture),
            int v => v.ToString(CultureInfo.InvariantCulture),
            long v => v.ToString(CultureInfo.InvariantCulture),
            float v => v.ToString(CultureInfo.InvariantCulture),
            double v => v.ToString(CultureInfo.InvariantCulture),
            decimal v => v.ToString(CultureInfo.InvariantCulture),
            DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            DateTimeOffset dto => dto.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            byte[] bytes => Convert.ToBase64String(bytes),
            _ => Value.ToString() ?? ""
        };
    }
}

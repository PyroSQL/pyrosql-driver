using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

namespace PyroSQL.EntityFrameworkCore.Storage;

/// <summary>
/// Type mapping source for PyroSQL. Maps CLR types to PyroSQL's native types:
/// BIGINT (i64), DOUBLE (f64), TEXT, BOOLEAN, BLOB.
/// </summary>
public class PyroSqlTypeMappingSource : RelationalTypeMappingSource
{
    // PyroSQL native type mappings
    private static readonly RelationalTypeMapping LongMapping
        = new LongTypeMapping("BIGINT", DbType.Int64);
    private static readonly RelationalTypeMapping IntMapping
        = new IntTypeMapping("INTEGER", DbType.Int32);
    private static readonly RelationalTypeMapping ShortMapping
        = new ShortTypeMapping("SMALLINT", DbType.Int16);
    private static readonly RelationalTypeMapping ByteMapping
        = new ByteTypeMapping("TINYINT", DbType.Byte);
    private static readonly RelationalTypeMapping DoubleMapping
        = new DoubleTypeMapping("DOUBLE", DbType.Double);
    private static readonly RelationalTypeMapping FloatMapping
        = new FloatTypeMapping("FLOAT", DbType.Single);
    private static readonly RelationalTypeMapping DecimalMapping
        = new DecimalTypeMapping("DECIMAL", DbType.Decimal);
    private static readonly RelationalTypeMapping BoolMapping
        = new BoolTypeMapping("BOOLEAN", DbType.Boolean);
    private static readonly RelationalTypeMapping StringMapping
        = new StringTypeMapping("TEXT", DbType.String);
    private static readonly RelationalTypeMapping ByteArrayMapping
        = new ByteArrayTypeMapping("BLOB", DbType.Binary);
    private static readonly RelationalTypeMapping DateTimeMapping
        = new DateTimeTypeMapping("TIMESTAMP", DbType.DateTime);
    private static readonly RelationalTypeMapping DateTimeOffsetMapping
        = new DateTimeOffsetTypeMapping("TIMESTAMP", DbType.DateTimeOffset);
    private static readonly RelationalTypeMapping GuidMapping
        = new GuidTypeMapping("TEXT", DbType.String);

    private static readonly Dictionary<Type, RelationalTypeMapping> ClrTypeMappings = new()
    {
        { typeof(long), LongMapping },
        { typeof(int), IntMapping },
        { typeof(short), ShortMapping },
        { typeof(byte), ByteMapping },
        { typeof(double), DoubleMapping },
        { typeof(float), FloatMapping },
        { typeof(decimal), DecimalMapping },
        { typeof(bool), BoolMapping },
        { typeof(string), StringMapping },
        { typeof(byte[]), ByteArrayMapping },
        { typeof(DateTime), DateTimeMapping },
        { typeof(DateTimeOffset), DateTimeOffsetMapping },
        { typeof(Guid), GuidMapping },
    };

    private static readonly Dictionary<string, RelationalTypeMapping> StoreTypeMappings
        = new(StringComparer.OrdinalIgnoreCase)
    {
        { "BIGINT", LongMapping },
        { "INTEGER", IntMapping },
        { "INT", IntMapping },
        { "SMALLINT", ShortMapping },
        { "TINYINT", ByteMapping },
        { "DOUBLE", DoubleMapping },
        { "FLOAT", FloatMapping },
        { "REAL", DoubleMapping },
        { "DECIMAL", DecimalMapping },
        { "NUMERIC", DecimalMapping },
        { "BOOLEAN", BoolMapping },
        { "BOOL", BoolMapping },
        { "TEXT", StringMapping },
        { "VARCHAR", StringMapping },
        { "CHAR", StringMapping },
        { "BLOB", ByteArrayMapping },
        { "BYTEA", ByteArrayMapping },
        { "TIMESTAMP", DateTimeMapping },
        { "DATETIME", DateTimeMapping },
        { "DATE", DateTimeMapping },
    };

    /// <summary>
    /// Creates a new PyroSQL type mapping source.
    /// </summary>
    public PyroSqlTypeMappingSource(TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    /// <inheritdoc />
    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        // Try CLR type first
        if (mappingInfo.ClrType != null && ClrTypeMappings.TryGetValue(mappingInfo.ClrType, out var clrMapping))
            return clrMapping;

        // Try store type name
        if (mappingInfo.StoreTypeName != null)
        {
            // Strip size/precision from type name (e.g., "VARCHAR(255)" -> "VARCHAR")
            var storeTypeName = mappingInfo.StoreTypeName;
            var parenIdx = storeTypeName.IndexOf('(');
            if (parenIdx >= 0)
                storeTypeName = storeTypeName[..parenIdx].Trim();

            if (StoreTypeMappings.TryGetValue(storeTypeName, out var storeMapping))
                return storeMapping;
        }

        return base.FindMapping(mappingInfo);
    }
}

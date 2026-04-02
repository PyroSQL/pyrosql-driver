using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Moq;
using PyroSQL.EntityFrameworkCore.Storage;
using Xunit;

namespace PyroSQL.EntityFrameworkCore.Tests;

public class TypeMappingSourceTests
{
    private readonly PyroSqlTypeMappingSource _source;

    public TypeMappingSourceTests()
    {
        var typeMappingSourceDeps = new TypeMappingSourceDependencies(
            Array.Empty<ITypeMappingSourcePlugin>());

        var relationalTypeMappingSourceDeps = new RelationalTypeMappingSourceDependencies(
            Array.Empty<IRelationalTypeMappingSourcePlugin>());

        _source = new PyroSqlTypeMappingSource(typeMappingSourceDeps, relationalTypeMappingSourceDeps);
    }

    [Theory]
    [InlineData(typeof(long), "BIGINT")]
    [InlineData(typeof(int), "INTEGER")]
    [InlineData(typeof(short), "SMALLINT")]
    [InlineData(typeof(byte), "TINYINT")]
    [InlineData(typeof(double), "DOUBLE")]
    [InlineData(typeof(float), "FLOAT")]
    [InlineData(typeof(decimal), "DECIMAL")]
    [InlineData(typeof(bool), "BOOLEAN")]
    [InlineData(typeof(string), "TEXT")]
    [InlineData(typeof(byte[]), "BLOB")]
    [InlineData(typeof(DateTime), "TIMESTAMP")]
    [InlineData(typeof(DateTimeOffset), "TIMESTAMP")]
    [InlineData(typeof(Guid), "TEXT")]
    public void FindMapping_ClrType_ReturnsCorrectStoreType(Type clrType, string expectedStoreType)
    {
        var mapping = _source.FindMapping(clrType);
        Assert.NotNull(mapping);
        Assert.Equal(expectedStoreType, mapping!.StoreType);
    }

    [Theory]
    [InlineData("BIGINT", typeof(long))]
    [InlineData("INTEGER", typeof(int))]
    [InlineData("INT", typeof(int))]
    [InlineData("DOUBLE", typeof(double))]
    [InlineData("REAL", typeof(double))]
    [InlineData("BOOLEAN", typeof(bool))]
    [InlineData("BOOL", typeof(bool))]
    [InlineData("TEXT", typeof(string))]
    [InlineData("VARCHAR", typeof(string))]
    [InlineData("BLOB", typeof(byte[]))]
    [InlineData("BYTEA", typeof(byte[]))]
    [InlineData("TIMESTAMP", typeof(DateTime))]
    [InlineData("DATETIME", typeof(DateTime))]
    public void FindMapping_StoreType_ReturnsCorrectClrType(string storeType, Type expectedClrType)
    {
        var mapping = _source.FindMapping(storeType);
        Assert.NotNull(mapping);
        Assert.Equal(expectedClrType, mapping!.ClrType);
    }

    [Fact]
    public void FindMapping_NullableType_ReturnsMappingForUnderlyingType()
    {
        var mapping = _source.FindMapping(typeof(int?));
        // Nullable<int> resolves through the int mapping
        Assert.NotNull(mapping);
    }
}

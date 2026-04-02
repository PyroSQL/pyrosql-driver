using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using PyroSQL.EntityFrameworkCore.Extensions;
using Xunit;

namespace PyroSQL.EntityFrameworkCore.Tests;

public class OptionsExtensionTests
{
    [Fact]
    public void UsePyroSql_SetsConnectionString()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        optionsBuilder.UsePyroSql("Host=localhost;Port=12520;Database=test;Username=admin;Password=secret");

        var extension = optionsBuilder.Options.FindExtension<PyroSqlOptionsExtension>();
        Assert.NotNull(extension);
        Assert.Contains("localhost", extension!.ConnectionString);
        Assert.Contains("12520", extension.ConnectionString);
    }

    [Fact]
    public void UsePyroSql_WithOptions_SetsCommandTimeout()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        optionsBuilder.UsePyroSql(
            "Host=localhost;Database=test;Username=admin;Password=secret",
            opts => opts.CommandTimeout(60));

        var extension = optionsBuilder.Options.FindExtension<PyroSqlOptionsExtension>();
        Assert.NotNull(extension);
        Assert.Equal(60, extension!.CommandTimeoutSeconds);
    }

    [Fact]
    public void UsePyroSql_WithMaxRetry_SetsRetryCount()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        optionsBuilder.UsePyroSql(
            "Host=localhost;Database=test;Username=admin;Password=secret",
            opts => opts.MaxRetryCount(3));

        var extension = optionsBuilder.Options.FindExtension<PyroSqlOptionsExtension>();
        Assert.NotNull(extension);
        Assert.Equal(3, extension!.MaxRetryCountValue);
    }

    [Fact]
    public void Extension_Info_IsDatabaseProvider()
    {
        var extension = new PyroSqlOptionsExtension();
        Assert.True(extension.Info.IsDatabaseProvider);
    }

    [Fact]
    public void Extension_Info_LogFragment_ContainsTimeout()
    {
        var extension = new PyroSqlOptionsExtension().WithCommandTimeout(45);
        Assert.Contains("45", extension.Info.LogFragment);
    }

    [Fact]
    public void UsePyroSql_ThrowsOnNullConnectionString()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        Assert.Throws<ArgumentException>(() => optionsBuilder.UsePyroSql(""));
    }
}

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using PyroSQL.EntityFrameworkCore.Extensions;
using PyroSQL.EntityFrameworkCore.Migrations;
using Xunit;

namespace PyroSQL.EntityFrameworkCore.Tests;

public class MigrationsSqlGeneratorTests
{
    private IMigrationsSqlGenerator CreateGenerator()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        optionsBuilder.UsePyroSql("Host=localhost;Database=test;Username=admin;Password=secret");

        using var ctx = new TestDbContext(optionsBuilder.Options);
        var services = ((IInfrastructure<IServiceProvider>)ctx).Instance;
        return services.GetRequiredService<IMigrationsSqlGenerator>();
    }

    [Fact]
    public void Generate_CreateTable_ProducesValidSql()
    {
        var generator = CreateGenerator();
        var operation = new CreateTableOperation
        {
            Name = "Users",
            Columns =
            {
                new AddColumnOperation
                {
                    Name = "Id",
                    Table = "Users",
                    ClrType = typeof(long),
                    ColumnType = "BIGINT",
                    IsNullable = false
                },
                new AddColumnOperation
                {
                    Name = "Name",
                    Table = "Users",
                    ClrType = typeof(string),
                    ColumnType = "TEXT",
                    IsNullable = false
                },
                new AddColumnOperation
                {
                    Name = "Email",
                    Table = "Users",
                    ClrType = typeof(string),
                    ColumnType = "TEXT",
                    IsNullable = true
                }
            },
            PrimaryKey = new AddPrimaryKeyOperation
            {
                Name = "PK_Users",
                Columns = new[] { "Id" }
            }
        };

        var commands = generator.Generate(new[] { operation });
        Assert.Single(commands);

        var sql = commands[0].CommandText;
        Assert.Contains("CREATE TABLE", sql);
        Assert.Contains("\"Users\"", sql);
        Assert.Contains("\"Id\" BIGINT NOT NULL", sql);
        Assert.Contains("\"Name\" TEXT NOT NULL", sql);
        Assert.Contains("\"Email\" TEXT", sql);
        Assert.Contains("PRIMARY KEY", sql);
    }

    [Fact]
    public void Generate_DropTable_ProducesValidSql()
    {
        var generator = CreateGenerator();
        var operation = new DropTableOperation { Name = "Users" };

        var commands = generator.Generate(new[] { operation });
        Assert.Single(commands);

        var sql = commands[0].CommandText;
        Assert.Contains("DROP TABLE IF EXISTS", sql);
        Assert.Contains("\"Users\"", sql);
    }

    [Fact]
    public void Generate_AddColumn_ProducesValidSql()
    {
        var generator = CreateGenerator();
        var operation = new AddColumnOperation
        {
            Table = "Users",
            Name = "Age",
            ClrType = typeof(int),
            ColumnType = "INTEGER",
            IsNullable = true
        };

        var commands = generator.Generate(new[] { operation });
        Assert.Single(commands);

        var sql = commands[0].CommandText;
        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("ADD COLUMN", sql);
        Assert.Contains("\"Age\" INTEGER", sql);
    }

    [Fact]
    public void Generate_DropColumn_ProducesValidSql()
    {
        var generator = CreateGenerator();
        var operation = new DropColumnOperation
        {
            Table = "Users",
            Name = "Age"
        };

        var commands = generator.Generate(new[] { operation });
        Assert.Single(commands);

        var sql = commands[0].CommandText;
        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("DROP COLUMN", sql);
        Assert.Contains("\"Age\"", sql);
    }

    [Fact]
    public void Generate_CreateIndex_ProducesValidSql()
    {
        var generator = CreateGenerator();
        var operation = new CreateIndexOperation
        {
            Name = "IX_Users_Email",
            Table = "Users",
            Columns = new[] { "Email" },
            IsUnique = true
        };

        var commands = generator.Generate(new[] { operation });
        Assert.Single(commands);

        var sql = commands[0].CommandText;
        Assert.Contains("CREATE UNIQUE INDEX", sql);
        Assert.Contains("\"IX_Users_Email\"", sql);
        Assert.Contains("\"Users\"", sql);
        Assert.Contains("\"Email\"", sql);
    }

    [Fact]
    public void Generate_RenameTable_ProducesValidSql()
    {
        var generator = CreateGenerator();
        var operation = new RenameTableOperation
        {
            Name = "Users",
            NewName = "Accounts"
        };

        var commands = generator.Generate(new[] { operation });
        Assert.Single(commands);

        var sql = commands[0].CommandText;
        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("RENAME TO", sql);
        Assert.Contains("\"Accounts\"", sql);
    }

    [Fact]
    public void Generate_EnsureSchema_ProducesValidSql()
    {
        var generator = CreateGenerator();
        var operation = new EnsureSchemaOperation { Name = "app" };

        var commands = generator.Generate(new[] { operation });
        Assert.Single(commands);

        var sql = commands[0].CommandText;
        Assert.Contains("CREATE SCHEMA IF NOT EXISTS", sql);
        Assert.Contains("\"app\"", sql);
    }

    private class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions options) : base(options) { }
    }
}

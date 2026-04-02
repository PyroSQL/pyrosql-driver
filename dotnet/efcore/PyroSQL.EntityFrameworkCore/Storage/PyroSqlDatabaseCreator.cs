using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using PyroSQL.Data;

namespace PyroSQL.EntityFrameworkCore.Storage;

/// <summary>
/// Database creator for PyroSQL. Handles database existence checks,
/// creation, deletion, and schema generation via migrations.
/// </summary>
public class PyroSqlDatabaseCreator : RelationalDatabaseCreator
{
    private readonly IRelationalConnection _connection;
    private readonly IMigrationsSqlGenerator _migrationsSqlGenerator;
    private readonly IRawSqlCommandBuilder _rawSqlCommandBuilder;

    /// <summary>
    /// Creates a new database creator.
    /// </summary>
    public PyroSqlDatabaseCreator(
        RelationalDatabaseCreatorDependencies dependencies,
        IRelationalConnection connection,
        IRawSqlCommandBuilder rawSqlCommandBuilder)
        : base(dependencies)
    {
        _connection = connection;
        _migrationsSqlGenerator = dependencies.MigrationsSqlGenerator;
        _rawSqlCommandBuilder = rawSqlCommandBuilder;
    }

    /// <inheritdoc />
    public override bool Exists()
    {
        try
        {
            _connection.Open();
            // Attempt a simple query to check if the database is reachable
            using var cmd = _connection.DbConnection.CreateCommand();
            cmd.CommandText = "SELECT 1";
            cmd.ExecuteScalar();
            return true;
        }
        catch (PyroSqlException)
        {
            return false;
        }
        finally
        {
            _connection.Close();
        }
    }

    /// <inheritdoc />
    public override bool HasTables()
    {
        try
        {
            _connection.Open();
            using var cmd = _connection.DbConnection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM pyro_catalog.tables WHERE schema_name = 'public'";
            var result = cmd.ExecuteScalar();
            return result != null && Convert.ToInt64(result) > 0;
        }
        catch (PyroSqlException)
        {
            return false;
        }
        finally
        {
            _connection.Close();
        }
    }

    /// <inheritdoc />
    public override void Create()
    {
        try
        {
            _connection.Open();
            using var cmd = _connection.DbConnection.CreateCommand();
            cmd.CommandText = "SELECT 1"; // PyroSQL databases are implicitly created on connect
            cmd.ExecuteScalar();
        }
        finally
        {
            _connection.Close();
        }
    }

    /// <inheritdoc />
    public override void Delete()
    {
        try
        {
            _connection.Open();
            // Drop all user tables
            using var listCmd = _connection.DbConnection.CreateCommand();
            listCmd.CommandText = "SELECT table_name FROM pyro_catalog.tables WHERE schema_name = 'public'";
            var tables = new List<string>();
            using (var reader = listCmd.ExecuteReader())
            {
                while (reader.Read())
                    tables.Add(reader.GetString(0));
            }

            foreach (var table in tables)
            {
                using var dropCmd = _connection.DbConnection.CreateCommand();
                dropCmd.CommandText = $"DROP TABLE IF EXISTS \"{table}\"";
                dropCmd.ExecuteNonQuery();
            }
        }
        finally
        {
            _connection.Close();
        }
    }

    /// <inheritdoc />
    public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _connection.OpenAsync(cancellationToken);
            using var cmd = _connection.DbConnection.CreateCommand();
            cmd.CommandText = "SELECT 1";
            await cmd.ExecuteScalarAsync(cancellationToken);
            return true;
        }
        catch (PyroSqlException)
        {
            return false;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    /// <inheritdoc />
    public override async Task CreateAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _connection.OpenAsync(cancellationToken);
            using var cmd = _connection.DbConnection.CreateCommand();
            cmd.CommandText = "SELECT 1";
            await cmd.ExecuteScalarAsync(cancellationToken);
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    /// <inheritdoc />
    public override async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _connection.OpenAsync(cancellationToken);
            using var listCmd = _connection.DbConnection.CreateCommand();
            listCmd.CommandText = "SELECT table_name FROM pyro_catalog.tables WHERE schema_name = 'public'";
            var tables = new List<string>();
            using (var reader = await listCmd.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                    tables.Add(reader.GetString(0));
            }

            foreach (var table in tables)
            {
                using var dropCmd = _connection.DbConnection.CreateCommand();
                dropCmd.CommandText = $"DROP TABLE IF EXISTS \"{table}\"";
                await dropCmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }
}

using System.Data.Common;
using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;
using PyroSQL.Data;
using PyroSQL.EntityFrameworkCore.Migrations;
using PyroSQL.EntityFrameworkCore.Query;
using PyroSQL.EntityFrameworkCore.Storage;

namespace PyroSQL.EntityFrameworkCore;

/// <summary>
/// EF Core options extension for PyroSQL that registers all required services.
/// </summary>
public class PyroSqlOptionsExtension : RelationalOptionsExtension
{
    private PyroSqlOptionsExtensionInfo? _info;
    private int _commandTimeout = 30;
    private int _maxRetryCount;

    /// <summary>
    /// Creates a new instance of the PyroSQL options extension.
    /// </summary>
    public PyroSqlOptionsExtension() { }

    /// <summary>
    /// Copy constructor.
    /// </summary>
    protected PyroSqlOptionsExtension(PyroSqlOptionsExtension copyFrom) : base(copyFrom)
    {
        _commandTimeout = copyFrom._commandTimeout;
        _maxRetryCount = copyFrom._maxRetryCount;
    }

    /// <summary>
    /// The command timeout in seconds.
    /// </summary>
    public int CommandTimeoutSeconds => _commandTimeout;

    /// <summary>
    /// Maximum retry count for transient failures.
    /// </summary>
    public int MaxRetryCountValue => _maxRetryCount;

    /// <inheritdoc />
    public override DbContextOptionsExtensionInfo Info
        => _info ??= new PyroSqlOptionsExtensionInfo(this);

    /// <inheritdoc />
    protected override RelationalOptionsExtension Clone()
        => new PyroSqlOptionsExtension(this);

    /// <summary>
    /// Returns a copy with the specified command timeout.
    /// </summary>
    public PyroSqlOptionsExtension WithCommandTimeout(int seconds)
    {
        var clone = (PyroSqlOptionsExtension)Clone();
        clone._commandTimeout = seconds;
        return clone;
    }

    /// <summary>
    /// Returns a copy with the specified max retry count.
    /// </summary>
    public PyroSqlOptionsExtension WithMaxRetryCount(int count)
    {
        var clone = (PyroSqlOptionsExtension)Clone();
        clone._maxRetryCount = count;
        return clone;
    }

    /// <inheritdoc />
    public override void ApplyServices(IServiceCollection services)
    {
        services.AddEntityFrameworkPyroSql();
    }

    /// <inheritdoc />
    public override void Validate(IDbContextOptions options)
    {
        base.Validate(options);
    }

    private sealed class PyroSqlOptionsExtensionInfo : RelationalExtensionInfo
    {
        public PyroSqlOptionsExtensionInfo(IDbContextOptionsExtension extension) : base(extension) { }

        private new PyroSqlOptionsExtension Extension => (PyroSqlOptionsExtension)base.Extension;

        public override bool IsDatabaseProvider => true;

        public override string LogFragment
            => $"Using PyroSQL - CommandTimeout={Extension._commandTimeout}";

        public override int GetServiceProviderHashCode()
        {
            var hashCode = new HashCode();
            hashCode.Add(Extension.ConnectionString);
            hashCode.Add(Extension._commandTimeout);
            hashCode.Add(Extension._maxRetryCount);
            return hashCode.ToHashCode();
        }

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is PyroSqlOptionsExtensionInfo;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            debugInfo["PyroSQL:ConnectionString"] = Extension.ConnectionString ?? "(null)";
            debugInfo["PyroSQL:CommandTimeout"] = Extension._commandTimeout.ToString(CultureInfo.InvariantCulture);
            debugInfo["PyroSQL:MaxRetryCount"] = Extension._maxRetryCount.ToString(CultureInfo.InvariantCulture);
        }
    }
}

/// <summary>
/// Extension methods for registering PyroSQL EF Core services.
/// </summary>
public static class PyroSqlServiceCollectionExtensionsInternal
{
    /// <summary>
    /// Adds PyroSQL-specific Entity Framework Core services to the service collection.
    /// </summary>
    public static IServiceCollection AddEntityFrameworkPyroSql(this IServiceCollection serviceCollection)
    {
        var builder = new EntityFrameworkRelationalServicesBuilder(serviceCollection);

        builder.TryAdd<IDatabaseProvider, DatabaseProvider<PyroSqlOptionsExtension>>();
        builder.TryAdd<IRelationalTypeMappingSource, PyroSqlTypeMappingSource>();
        builder.TryAdd<ISqlGenerationHelper, PyroSqlSqlGenerationHelper>();
        builder.TryAdd<IRelationalConnection, PyroSqlRelationalConnection>();
        builder.TryAdd<IMigrationsSqlGenerator, PyroSqlMigrationsSqlGenerator>();
        builder.TryAdd<IQuerySqlGeneratorFactory, PyroSqlQuerySqlGeneratorFactory>();
        builder.TryAdd<IRelationalDatabaseCreator, PyroSqlDatabaseCreator>();
        builder.TryAdd<IModificationCommandBatchFactory, PyroSqlModificationCommandBatchFactory>();

        builder.TryAddCoreServices();

        return serviceCollection;
    }
}

/// <summary>
/// SQL generation helper for PyroSQL (identifier quoting, literals, etc.).
/// </summary>
internal sealed class PyroSqlSqlGenerationHelper : RelationalSqlGenerationHelper
{
    public PyroSqlSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies) { }

    public override string DelimitIdentifier(string identifier)
        => $"\"{EscapeIdentifier(identifier)}\"";

    public override void DelimitIdentifier(System.Text.StringBuilder builder, string identifier)
    {
        builder.Append('"');
        EscapeIdentifier(builder, identifier);
        builder.Append('"');
    }

    public override string EscapeIdentifier(string identifier)
        => identifier.Replace("\"", "\"\"");

    public override void EscapeIdentifier(System.Text.StringBuilder builder, string identifier)
        => builder.Append(identifier.Replace("\"", "\"\""));
}

/// <summary>
/// Relational connection wrapper for PyroSQL, bridges EF Core to the ADO.NET driver.
/// </summary>
internal sealed class PyroSqlRelationalConnection : RelationalConnection
{
    public PyroSqlRelationalConnection(RelationalConnectionDependencies dependencies)
        : base(dependencies) { }

    protected override DbConnection CreateDbConnection()
        => new PyroSqlConnection(ConnectionString!);
}

/// <summary>
/// Modification command batch factory for PyroSQL.
/// </summary>
internal sealed class PyroSqlModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;

    public PyroSqlModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public ModificationCommandBatch Create()
        => new SingularModificationCommandBatch(_dependencies);
}

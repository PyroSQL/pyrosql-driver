using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace PyroSQL.EntityFrameworkCore.Extensions;

/// <summary>
/// Extension methods for configuring PyroSQL with Entity Framework Core.
/// </summary>
public static class PyroSqlServiceCollectionExtensions
{
    /// <summary>
    /// Registers the PyroSQL Entity Framework Core provider in the service collection.
    /// </summary>
    /// <param name="serviceCollection">The service collection to configure.</param>
    /// <param name="connectionString">The PyroSQL connection string.</param>
    /// <param name="pyroSqlOptionsAction">Optional action to configure PyroSQL-specific options.</param>
    /// <param name="dbContextOptionsAction">Optional action to configure additional DbContext options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddPyroSql<TContext>(
        this IServiceCollection serviceCollection,
        string connectionString,
        Action<PyroSqlDbContextOptionsBuilder>? pyroSqlOptionsAction = null,
        Action<DbContextOptionsBuilder>? dbContextOptionsAction = null)
        where TContext : DbContext
    {
        serviceCollection.AddDbContext<TContext>(options =>
        {
            options.UsePyroSql(connectionString, pyroSqlOptionsAction);
            dbContextOptionsAction?.Invoke(options);
        });

        return serviceCollection;
    }

    /// <summary>
    /// Configures the DbContext to use PyroSQL.
    /// </summary>
    /// <param name="optionsBuilder">The DbContext options builder.</param>
    /// <param name="connectionString">The PyroSQL connection string.</param>
    /// <param name="pyroSqlOptionsAction">Optional action to configure PyroSQL-specific options.</param>
    /// <returns>The options builder for chaining.</returns>
    public static DbContextOptionsBuilder UsePyroSql(
        this DbContextOptionsBuilder optionsBuilder,
        string connectionString,
        Action<PyroSqlDbContextOptionsBuilder>? pyroSqlOptionsAction = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentException.ThrowIfNullOrEmpty(connectionString);

        var extension = (PyroSqlOptionsExtension?)optionsBuilder.Options.FindExtension<PyroSqlOptionsExtension>()
            ?? new PyroSqlOptionsExtension();

        extension = extension.WithConnectionString(connectionString);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        var builder = new PyroSqlDbContextOptionsBuilder(optionsBuilder);
        pyroSqlOptionsAction?.Invoke(builder);

        return optionsBuilder;
    }
}

/// <summary>
/// Builder for PyroSQL-specific DbContext options.
/// </summary>
public class PyroSqlDbContextOptionsBuilder
{
    private readonly DbContextOptionsBuilder _optionsBuilder;

    /// <summary>
    /// Creates a new builder wrapping the given DbContext options builder.
    /// </summary>
    public PyroSqlDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    {
        _optionsBuilder = optionsBuilder;
    }

    /// <summary>
    /// Sets the command timeout in seconds.
    /// </summary>
    public PyroSqlDbContextOptionsBuilder CommandTimeout(int seconds)
    {
        var extension = GetOrCreateExtension().WithCommandTimeout(seconds);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    /// <summary>
    /// Sets the maximum number of retry attempts for transient failures.
    /// </summary>
    public PyroSqlDbContextOptionsBuilder MaxRetryCount(int count)
    {
        var extension = GetOrCreateExtension().WithMaxRetryCount(count);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    private PyroSqlOptionsExtension GetOrCreateExtension()
        => (PyroSqlOptionsExtension?)_optionsBuilder.Options.FindExtension<PyroSqlOptionsExtension>()
           ?? new PyroSqlOptionsExtension();
}

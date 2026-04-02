using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace PyroSQL.EntityFrameworkCore.Query;

/// <summary>
/// SQL generator factory for PyroSQL queries.
/// </summary>
public class PyroSqlQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    private readonly QuerySqlGeneratorDependencies _dependencies;

    /// <summary>
    /// Creates a new query SQL generator factory.
    /// </summary>
    public PyroSqlQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    /// <inheritdoc />
    public QuerySqlGenerator Create()
        => new PyroSqlQuerySqlGenerator(_dependencies);
}

/// <summary>
/// Generates PyroSQL-compatible SQL from LINQ expression trees.
/// Handles PyroSQL-specific syntax for LIMIT/OFFSET, string functions,
/// type casts, and boolean expressions.
/// </summary>
public class PyroSqlQuerySqlGenerator : QuerySqlGenerator
{
    /// <summary>
    /// Creates a new PyroSQL query SQL generator.
    /// </summary>
    public PyroSqlQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies)
        : base(dependencies) { }

    /// <inheritdoc />
    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        // PyroSQL uses LIMIT/OFFSET syntax (same as PostgreSQL)
        if (selectExpression.Limit != null)
        {
            Sql.AppendLine().Append("LIMIT ");
            Visit(selectExpression.Limit);
        }

        if (selectExpression.Offset != null)
        {
            if (selectExpression.Limit == null)
            {
                // PyroSQL requires LIMIT before OFFSET; use a large number as "unlimited"
                Sql.AppendLine().Append("LIMIT 9223372036854775807");
            }

            Sql.Append(" OFFSET ");
            Visit(selectExpression.Offset);
        }
    }

    /// <inheritdoc />
    protected override Expression VisitSqlFunction(SqlFunctionExpression sqlFunctionExpression)
    {
        // Map EF Core function names to PyroSQL equivalents
        var functionName = sqlFunctionExpression.Name;

        var mapped = functionName.ToUpperInvariant() switch
        {
            "LEN" => "LENGTH",
            "SUBSTRING" => "SUBSTR",
            "CHARINDEX" => "INSTR",
            "GETDATE" => "NOW",
            "GETUTCDATE" => "NOW",
            "ISNULL" => "COALESCE",
            "DATALENGTH" => "OCTET_LENGTH",
            _ => null
        };

        if (mapped != null)
        {
            Sql.Append(mapped);
            Sql.Append("(");

            if (sqlFunctionExpression.Arguments != null)
                GenerateList(sqlFunctionExpression.Arguments, e => Visit(e));

            Sql.Append(")");
            return sqlFunctionExpression;
        }

        return base.VisitSqlFunction(sqlFunctionExpression);
    }

    /// <inheritdoc />
    protected override Expression VisitSqlBinary(SqlBinaryExpression sqlBinaryExpression)
    {
        // PyroSQL uses || for string concatenation
        if (sqlBinaryExpression.OperatorType == ExpressionType.Add
            && sqlBinaryExpression.TypeMapping?.ClrType == typeof(string))
        {
            Visit(sqlBinaryExpression.Left);
            Sql.Append(" || ");
            Visit(sqlBinaryExpression.Right);
            return sqlBinaryExpression;
        }

        return base.VisitSqlBinary(sqlBinaryExpression);
    }

    private void GenerateList<T>(IReadOnlyList<T> items, Action<T> generationAction, Action<IRelationalCommandBuilder>? joinAction = null)
    {
        joinAction ??= (isb => isb.Append(", "));

        for (var i = 0; i < items.Count; i++)
        {
            if (i > 0)
                joinAction(Sql);
            generationAction(items[i]);
        }
    }
}

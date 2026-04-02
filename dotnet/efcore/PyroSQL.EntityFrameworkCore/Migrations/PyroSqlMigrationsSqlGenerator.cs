using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;

namespace PyroSQL.EntityFrameworkCore.Migrations;

/// <summary>
/// Generates PyroSQL-specific migration SQL from migration operations.
/// Supports CREATE/ALTER/DROP TABLE, indexes, columns, and schema operations.
/// </summary>
public class PyroSqlMigrationsSqlGenerator : MigrationsSqlGenerator
{
    /// <summary>
    /// Creates a new PyroSQL migrations SQL generator.
    /// </summary>
    public PyroSqlMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies)
        : base(dependencies) { }

    /// <inheritdoc />
    protected override void Generate(CreateTableOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
    {
        builder.Append("CREATE TABLE ");
        builder.Append(DelimitIdentifier(operation.Name, operation.Schema));
        builder.AppendLine(" (");

        using (builder.Indent())
        {
            for (int i = 0; i < operation.Columns.Count; i++)
            {
                var column = operation.Columns[i];
                if (i > 0)
                    builder.AppendLine(",");

                ColumnDefinition(column, model, builder);
            }

            if (operation.PrimaryKey != null)
            {
                builder.AppendLine(",");
                PrimaryKeyConstraint(operation.PrimaryKey, model, builder);
            }

            foreach (var uniqueConstraint in operation.UniqueConstraints)
            {
                builder.AppendLine(",");
                UniqueConstraint(uniqueConstraint, model, builder);
            }

            foreach (var foreignKey in operation.ForeignKeys)
            {
                builder.AppendLine(",");
                ForeignKeyConstraint(foreignKey, model, builder);
            }
        }

        builder.AppendLine();
        builder.Append(")");

        if (terminate)
        {
            builder.AppendLine(";");
            EndStatement(builder);
        }
    }

    /// <inheritdoc />
    protected override void Generate(DropTableOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
    {
        builder.Append("DROP TABLE IF EXISTS ");
        builder.Append(DelimitIdentifier(operation.Name, operation.Schema));

        if (terminate)
        {
            builder.AppendLine(";");
            EndStatement(builder);
        }
    }

    /// <inheritdoc />
    protected override void Generate(AddColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
    {
        builder.Append("ALTER TABLE ");
        builder.Append(DelimitIdentifier(operation.Table, operation.Schema));
        builder.Append(" ADD COLUMN ");
        ColumnDefinition(operation, model, builder);

        if (terminate)
        {
            builder.AppendLine(";");
            EndStatement(builder);
        }
    }

    /// <inheritdoc />
    protected override void Generate(DropColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
    {
        builder.Append("ALTER TABLE ");
        builder.Append(DelimitIdentifier(operation.Table, operation.Schema));
        builder.Append(" DROP COLUMN ");
        builder.Append(DelimitIdentifier(operation.Name));

        if (terminate)
        {
            builder.AppendLine(";");
            EndStatement(builder);
        }
    }

    /// <inheritdoc />
    protected override void Generate(AlterColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder.Append("ALTER TABLE ");
        builder.Append(DelimitIdentifier(operation.Table, operation.Schema));
        builder.Append(" ALTER COLUMN ");
        builder.Append(DelimitIdentifier(operation.Name));
        builder.Append(" TYPE ");
        builder.Append(operation.ColumnType ?? GetColumnType(operation.Schema, operation.Table, operation.Name, operation, model)!);

        if (!operation.IsNullable)
        {
            builder.AppendLine(";");
            builder.Append("ALTER TABLE ");
            builder.Append(DelimitIdentifier(operation.Table, operation.Schema));
            builder.Append(" ALTER COLUMN ");
            builder.Append(DelimitIdentifier(operation.Name));
            builder.Append(" SET NOT NULL");
        }

        builder.AppendLine(";");
        EndStatement(builder);
    }

    /// <inheritdoc />
    protected override void Generate(RenameTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder.Append("ALTER TABLE ");
        builder.Append(DelimitIdentifier(operation.Name, operation.Schema));
        builder.Append(" RENAME TO ");
        builder.Append(DelimitIdentifier(operation.NewName!, operation.NewSchema));
        builder.AppendLine(";");
        EndStatement(builder);
    }

    /// <inheritdoc />
    protected override void Generate(RenameColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder.Append("ALTER TABLE ");
        builder.Append(DelimitIdentifier(operation.Table, operation.Schema));
        builder.Append(" RENAME COLUMN ");
        builder.Append(DelimitIdentifier(operation.Name));
        builder.Append(" TO ");
        builder.Append(DelimitIdentifier(operation.NewName));
        builder.AppendLine(";");
        EndStatement(builder);
    }

    /// <inheritdoc />
    protected override void Generate(CreateIndexOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
    {
        builder.Append("CREATE ");
        if (operation.IsUnique)
            builder.Append("UNIQUE ");
        builder.Append("INDEX ");
        builder.Append(DelimitIdentifier(operation.Name));
        builder.Append(" ON ");
        builder.Append(DelimitIdentifier(operation.Table, operation.Schema));
        builder.Append(" (");
        builder.Append(string.Join(", ", operation.Columns.Select(DelimitIdentifier)));
        builder.Append(")");

        if (terminate)
        {
            builder.AppendLine(";");
            EndStatement(builder);
        }
    }

    /// <inheritdoc />
    protected override void Generate(DropIndexOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
    {
        builder.Append("DROP INDEX IF EXISTS ");
        builder.Append(DelimitIdentifier(operation.Name));

        if (terminate)
        {
            builder.AppendLine(";");
            EndStatement(builder);
        }
    }

    /// <inheritdoc />
    protected override void Generate(RenameIndexOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder.Append("ALTER INDEX ");
        builder.Append(DelimitIdentifier(operation.Name));
        builder.Append(" RENAME TO ");
        builder.Append(DelimitIdentifier(operation.NewName));
        builder.AppendLine(";");
        EndStatement(builder);
    }

    /// <inheritdoc />
    protected override void Generate(EnsureSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder.Append("CREATE SCHEMA IF NOT EXISTS ");
        builder.Append(DelimitIdentifier(operation.Name));
        builder.AppendLine(";");
        EndStatement(builder);
    }

    /// <inheritdoc />
    protected override void Generate(DropSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder.Append("DROP SCHEMA IF EXISTS ");
        builder.Append(DelimitIdentifier(operation.Name));
        builder.Append(" CASCADE");
        builder.AppendLine(";");
        EndStatement(builder);
    }

    /// <inheritdoc />
    protected override void ColumnDefinition(
        string? schema,
        string table,
        string name,
        ColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder.Append(DelimitIdentifier(name));
        builder.Append(" ");

        var columnType = operation.ColumnType
            ?? GetColumnType(schema, table, name, operation, model)
            ?? "TEXT";
        builder.Append(columnType);

        if (!operation.IsNullable)
            builder.Append(" NOT NULL");

        if (operation.DefaultValueSql != null)
        {
            builder.Append(" DEFAULT (");
            builder.Append(operation.DefaultValueSql);
            builder.Append(")");
        }
        else if (operation.DefaultValue != null)
        {
            builder.Append(" DEFAULT ");
            var typeMapping = Dependencies.TypeMappingSource.GetMappingForValue(operation.DefaultValue);
            builder.Append(typeMapping.GenerateSqlLiteral(operation.DefaultValue));
        }
    }

    /// <inheritdoc />
    protected override void PrimaryKeyConstraint(
        AddPrimaryKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder.Append("CONSTRAINT ");
        builder.Append(DelimitIdentifier(operation.Name));
        builder.Append(" PRIMARY KEY (");
        builder.Append(string.Join(", ", operation.Columns.Select(DelimitIdentifier)));
        builder.Append(")");
    }

    /// <inheritdoc />
    protected override void UniqueConstraint(
        AddUniqueConstraintOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder.Append("CONSTRAINT ");
        builder.Append(DelimitIdentifier(operation.Name));
        builder.Append(" UNIQUE (");
        builder.Append(string.Join(", ", operation.Columns.Select(DelimitIdentifier)));
        builder.Append(")");
    }

    /// <inheritdoc />
    protected override void ForeignKeyConstraint(
        AddForeignKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder.Append("CONSTRAINT ");
        builder.Append(DelimitIdentifier(operation.Name));
        builder.Append(" FOREIGN KEY (");
        builder.Append(string.Join(", ", operation.Columns.Select(DelimitIdentifier)));
        builder.Append(") REFERENCES ");
        builder.Append(DelimitIdentifier(operation.PrincipalTable, operation.PrincipalSchema));
        builder.Append(" (");
        builder.Append(string.Join(", ", operation.PrincipalColumns.Select(DelimitIdentifier)));
        builder.Append(")");

        if (operation.OnDelete != ReferentialAction.NoAction)
        {
            builder.Append(" ON DELETE ");
            builder.Append(ReferentialActionToSql(operation.OnDelete));
        }

        if (operation.OnUpdate != ReferentialAction.NoAction)
        {
            builder.Append(" ON UPDATE ");
            builder.Append(ReferentialActionToSql(operation.OnUpdate));
        }
    }

    private static string ReferentialActionToSql(ReferentialAction action) => action switch
    {
        ReferentialAction.Cascade => "CASCADE",
        ReferentialAction.Restrict => "RESTRICT",
        ReferentialAction.SetNull => "SET NULL",
        ReferentialAction.SetDefault => "SET DEFAULT",
        _ => "NO ACTION"
    };

    private string DelimitIdentifier(string identifier, string? schema = null)
    {
        var delimited = $"\"{identifier.Replace("\"", "\"\"")}\"";
        if (schema != null)
            delimited = $"\"{schema.Replace("\"", "\"\"")}\"." + delimited;
        return delimited;
    }
}

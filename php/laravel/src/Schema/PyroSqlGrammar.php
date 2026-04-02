<?php

declare(strict_types=1);

namespace PyroSQL\Laravel\Schema;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Schema\Grammars\Grammar;
use Illuminate\Support\Fluent;

/**
 * Schema grammar for PyroSQL DDL generation.
 *
 * Generates CREATE TABLE, ALTER TABLE, index, and constraint SQL
 * using PyroSQL's PostgreSQL-compatible DDL syntax.
 */
class PyroSqlGrammar extends Grammar
{
    /**
     * The possible column modifiers.
     *
     * @var string[]
     */
    protected $modifiers = ['Collate', 'Nullable', 'Default', 'VirtualAs', 'StoredAs', 'GeneratedAs', 'Increment'];

    /**
     * The possible column serials.
     *
     * @var string[]
     */
    protected $serials = ['bigInteger', 'integer', 'mediumInteger', 'smallInteger', 'tinyInteger'];

    /**
     * The commands to be executed outside of create or alter command.
     *
     * @var string[]
     */
    protected $fluentCommands = ['Comment'];

    /**
     * Compile a create table command.
     */
    public function compileCreate(Blueprint $blueprint, Fluent $command)
    {
        return sprintf(
            '%s table %s (%s)',
            $blueprint->temporary ? 'create temporary' : 'create',
            $this->wrapTable($blueprint),
            implode(', ', $this->getColumns($blueprint)),
        );
    }

    /**
     * Compile a column addition command.
     */
    public function compileAdd(Blueprint $blueprint, Fluent $command)
    {
        return sprintf(
            'alter table %s add column %s',
            $this->wrapTable($blueprint),
            $this->getColumn($blueprint, $command->column),
        );
    }

    /**
     * Compile a primary key command.
     */
    public function compilePrimary(Blueprint $blueprint, Fluent $command)
    {
        $columns = $this->columnize($command->columns);

        return 'alter table ' . $this->wrapTable($blueprint) . " add primary key ({$columns})";
    }

    /**
     * Compile a unique key command.
     */
    public function compileUnique(Blueprint $blueprint, Fluent $command)
    {
        return sprintf(
            'alter table %s add constraint %s unique (%s)',
            $this->wrapTable($blueprint),
            $this->wrap($command->index),
            $this->columnize($command->columns),
        );
    }

    /**
     * Compile a plain index key command.
     */
    public function compileIndex(Blueprint $blueprint, Fluent $command)
    {
        return sprintf(
            'create index %s on %s (%s)',
            $this->wrap($command->index),
            $this->wrapTable($blueprint),
            $this->columnize($command->columns),
        );
    }

    /**
     * Compile a fulltext index key command.
     */
    public function compileFulltext(Blueprint $blueprint, Fluent $command)
    {
        $language = $command->language ?: 'english';
        $columns = array_map(
            fn ($column) => "to_tsvector('{$language}', " . $this->wrap($column) . ')',
            $command->columns,
        );

        return sprintf(
            'create index %s on %s using gin ((%s))',
            $this->wrap($command->index),
            $this->wrapTable($blueprint),
            implode(' || ', $columns),
        );
    }

    /**
     * Compile a spatial index key command.
     */
    public function compileSpatialIndex(Blueprint $blueprint, Fluent $command)
    {
        return sprintf(
            'create index %s on %s using gist (%s)',
            $this->wrap($command->index),
            $this->wrapTable($blueprint),
            $this->columnize($command->columns),
        );
    }

    /**
     * Compile a drop table command.
     */
    public function compileDrop(Blueprint $blueprint, Fluent $command)
    {
        return 'drop table ' . $this->wrapTable($blueprint);
    }

    /**
     * Compile a drop table (if exists) command.
     */
    public function compileDropIfExists(Blueprint $blueprint, Fluent $command)
    {
        return 'drop table if exists ' . $this->wrapTable($blueprint);
    }

    /**
     * Compile a drop column command.
     */
    public function compileDropColumn(Blueprint $blueprint, Fluent $command)
    {
        $columns = $this->prefixArray('drop column', $this->wrapArray($command->columns));

        return 'alter table ' . $this->wrapTable($blueprint) . ' ' . implode(', ', $columns);
    }

    /**
     * Compile a drop primary key command.
     */
    public function compileDropPrimary(Blueprint $blueprint, Fluent $command)
    {
        $table = $blueprint->getTable();
        $index = $this->wrap("{$this->connection->getTablePrefix()}{$table}_pkey");

        return 'alter table ' . $this->wrapTable($blueprint) . " drop constraint {$index}";
    }

    /**
     * Compile a drop unique key command.
     */
    public function compileDropUnique(Blueprint $blueprint, Fluent $command)
    {
        return 'alter table ' . $this->wrapTable($blueprint) . ' drop constraint ' . $this->wrap($command->index);
    }

    /**
     * Compile a drop index command.
     */
    public function compileDropIndex(Blueprint $blueprint, Fluent $command)
    {
        return 'drop index ' . $this->wrap($command->index);
    }

    /**
     * Compile a drop fulltext index command.
     */
    public function compileDropFullText(Blueprint $blueprint, Fluent $command)
    {
        return $this->compileDropIndex($blueprint, $command);
    }

    /**
     * Compile a drop spatial index command.
     */
    public function compileDropSpatialIndex(Blueprint $blueprint, Fluent $command)
    {
        return $this->compileDropIndex($blueprint, $command);
    }

    /**
     * Compile a drop foreign key command.
     */
    public function compileDropForeign(Blueprint $blueprint, Fluent $command)
    {
        return 'alter table ' . $this->wrapTable($blueprint) . ' drop constraint ' . $this->wrap($command->index);
    }

    /**
     * Compile a rename table command.
     */
    public function compileRename(Blueprint $blueprint, Fluent $command)
    {
        return 'alter table ' . $this->wrapTable($blueprint) . ' rename to ' . $this->wrapTable($command->to);
    }

    /**
     * Compile a rename index command.
     */
    public function compileRenameIndex(Blueprint $blueprint, Fluent $command)
    {
        return 'alter index ' . $this->wrap($command->from) . ' rename to ' . $this->wrap($command->to);
    }

    /**
     * {@inheritDoc}
     */
    public function compileChange(Blueprint $blueprint, Fluent $command)
    {
        $column = $command->column;

        $changes = ['type ' . $this->getType($column) . $this->modifyCollate($blueprint, $column)];

        foreach ($this->modifiers as $modifier) {
            if ($modifier === 'Collate') {
                continue;
            }

            if (method_exists($this, $method = "modify{$modifier}")) {
                $constraints = (array) $this->{$method}($blueprint, $column);

                foreach ($constraints as $constraint) {
                    $changes[] = $constraint;
                }
            }
        }

        return sprintf(
            'alter table %s %s',
            $this->wrapTable($blueprint),
            implode(', ', $this->prefixArray('alter column ' . $this->wrap($column), $changes)),
        );
    }

    /**
     * Compile the command to enable foreign key constraints.
     */
    public function compileEnableForeignKeyConstraints()
    {
        return 'SET CONSTRAINTS ALL IMMEDIATE;';
    }

    /**
     * Compile the command to disable foreign key constraints.
     */
    public function compileDisableForeignKeyConstraints()
    {
        return 'SET CONSTRAINTS ALL DEFERRED;';
    }

    /**
     * Compile the query to determine the tables.
     */
    public function compileTables($schema)
    {
        $schemaFilter = $this->compileSchemaFilter($schema);

        return "select table_name as name, table_schema as schema "
            . "from information_schema.tables "
            . "where {$schemaFilter} and table_type = 'BASE TABLE' "
            . "order by table_name";
    }

    /**
     * Compile the query to determine the views.
     */
    public function compileViews($schema)
    {
        $schemaFilter = $this->compileSchemaFilter($schema);

        return "select table_name as name, view_definition as definition, table_schema as schema "
            . "from information_schema.views "
            . "where {$schemaFilter} "
            . "order by table_name";
    }

    /**
     * Compile the query to determine the columns.
     */
    public function compileColumns($schema, $table)
    {
        return sprintf(
            "select column_name, data_type, is_nullable, column_default as \"default\", "
            . "character_maximum_length, numeric_precision, numeric_scale, "
            . "collation_name "
            . "from information_schema.columns "
            . "where table_schema = %s and table_name = %s "
            . "order by ordinal_position",
            $schema ? $this->quoteString($schema) : "'public'",
            $this->quoteString($table),
        );
    }

    /**
     * Compile the query to determine the indexes.
     */
    public function compileIndexes($schema, $table)
    {
        return sprintf(
            "select indexname as name, indexdef as definition "
            . "from pg_indexes "
            . "where schemaname = %s and tablename = %s",
            $schema ? $this->quoteString($schema) : "'public'",
            $this->quoteString($table),
        );
    }

    /**
     * Compile the query to determine the foreign keys.
     */
    public function compileForeignKeys($schema, $table)
    {
        return sprintf(
            "select "
            . "tc.constraint_name, "
            . "kcu.column_name, "
            . "ccu.table_name as foreign_table_name, "
            . "ccu.column_name as foreign_column_name "
            . "from information_schema.table_constraints as tc "
            . "join information_schema.key_column_usage as kcu "
            . "on tc.constraint_name = kcu.constraint_name "
            . "join information_schema.constraint_column_usage as ccu "
            . "on ccu.constraint_name = tc.constraint_name "
            . "where tc.constraint_type = 'FOREIGN KEY' "
            . "and tc.table_schema = %s "
            . "and tc.table_name = %s",
            $schema ? $this->quoteString($schema) : "'public'",
            $this->quoteString($table),
        );
    }

    /**
     * Compile a drop all tables command.
     */
    public function compileDropAllTables($tables)
    {
        return 'drop table ' . implode(', ', $this->escapeNames($tables)) . ' cascade';
    }

    /**
     * Compile a drop all views command.
     */
    public function compileDropAllViews($views)
    {
        return 'drop view ' . implode(', ', $this->escapeNames($views)) . ' cascade';
    }

    /**
     * Compile a comment command.
     */
    public function compileComment(Blueprint $blueprint, Fluent $command)
    {
        $comment = $command->column->comment;

        if (! is_null($comment) || $command->column->change) {
            return sprintf(
                'comment on column %s.%s is %s',
                $this->wrapTable($blueprint),
                $this->wrap($command->column->name),
                is_null($comment) ? 'NULL' : "'" . str_replace("'", "''", $comment) . "'",
            );
        }
    }

    /**
     * Quote-escape the given tables, views, or types.
     */
    public function escapeNames($names)
    {
        return array_map(function ($name) {
            $parts = explode('.', $name);
            return implode('.', array_map(fn ($p) => $this->wrapValue($p), $parts));
        }, $names);
    }

    // ── Schema filtering helper ──────────────────────────────────────

    /**
     * Compile a schema filter for information_schema queries.
     */
    protected function compileSchemaFilter($schema): string
    {
        if (! empty($schema) && is_array($schema)) {
            return 'table_schema in (' . $this->quoteString($schema) . ')';
        }

        if (! empty($schema)) {
            return 'table_schema = ' . $this->quoteString($schema);
        }

        return "table_schema = 'public'";
    }

    // ── Column type declarations ──────────────────────────────────────

    protected function typeChar(Fluent $column)
    {
        if ($column->length) {
            return "char({$column->length})";
        }

        return 'char(255)';
    }

    protected function typeString(Fluent $column)
    {
        if ($column->length) {
            return "varchar({$column->length})";
        }

        return 'varchar(255)';
    }

    protected function typeTinyText(Fluent $column)
    {
        return 'text';
    }

    protected function typeText(Fluent $column)
    {
        return 'text';
    }

    protected function typeMediumText(Fluent $column)
    {
        return 'text';
    }

    protected function typeLongText(Fluent $column)
    {
        return 'text';
    }

    protected function typeBigInteger(Fluent $column)
    {
        return $column->autoIncrement && is_null($column->generatedAs) && ! $column->change
            ? 'bigserial'
            : 'bigint';
    }

    protected function typeInteger(Fluent $column)
    {
        return $column->autoIncrement && is_null($column->generatedAs) && ! $column->change
            ? 'serial'
            : 'integer';
    }

    protected function typeMediumInteger(Fluent $column)
    {
        return $this->typeInteger($column);
    }

    protected function typeTinyInteger(Fluent $column)
    {
        return $this->typeSmallInteger($column);
    }

    protected function typeSmallInteger(Fluent $column)
    {
        return $column->autoIncrement && is_null($column->generatedAs) && ! $column->change
            ? 'smallserial'
            : 'smallint';
    }

    protected function typeFloat(Fluent $column)
    {
        if ($column->precision) {
            return "float({$column->precision})";
        }

        return 'real';
    }

    protected function typeDouble(Fluent $column)
    {
        return 'double precision';
    }

    protected function typeDecimal(Fluent $column)
    {
        return "numeric({$column->total}, {$column->places})";
    }

    protected function typeBoolean(Fluent $column)
    {
        return 'boolean';
    }

    protected function typeEnum(Fluent $column)
    {
        return sprintf(
            'varchar(255) check (%s in (%s))',
            $this->wrap($column->name),
            implode(', ', array_map(
                fn ($value) => "'" . str_replace("'", "''", (string) $value) . "'",
                $column->allowed,
            )),
        );
    }

    protected function typeSet(Fluent $column)
    {
        return 'text';
    }

    protected function typeJson(Fluent $column)
    {
        return 'jsonb';
    }

    protected function typeJsonb(Fluent $column)
    {
        return 'jsonb';
    }

    protected function typeDate(Fluent $column)
    {
        return 'date';
    }

    protected function typeDateTime(Fluent $column)
    {
        $precision = $column->precision;

        if ($precision !== null) {
            return "timestamp({$precision}) without time zone";
        }

        return 'timestamp';
    }

    protected function typeDateTimeTz(Fluent $column)
    {
        $precision = $column->precision;

        if ($precision !== null) {
            return "timestamp({$precision}) with time zone";
        }

        return 'timestamp with time zone';
    }

    protected function typeTime(Fluent $column)
    {
        $precision = $column->precision;

        if ($precision !== null) {
            return "time({$precision}) without time zone";
        }

        return 'time';
    }

    protected function typeTimeTz(Fluent $column)
    {
        $precision = $column->precision;

        if ($precision !== null) {
            return "time({$precision}) with time zone";
        }

        return 'time with time zone';
    }

    protected function typeTimestamp(Fluent $column)
    {
        return $this->typeDateTime($column);
    }

    protected function typeTimestampTz(Fluent $column)
    {
        return $this->typeDateTimeTz($column);
    }

    protected function typeYear(Fluent $column)
    {
        return 'integer';
    }

    protected function typeBinary(Fluent $column)
    {
        return 'bytea';
    }

    protected function typeUuid(Fluent $column)
    {
        return 'uuid';
    }

    protected function typeIpAddress(Fluent $column)
    {
        return 'inet';
    }

    protected function typeMacAddress(Fluent $column)
    {
        return 'macaddr';
    }

    protected function typeGeometry(Fluent $column)
    {
        return 'geometry';
    }

    protected function typeGeography(Fluent $column)
    {
        return 'geography';
    }

    // ── Column modifiers ──────────────────────────────────────────────

    protected function modifyCollate(Blueprint $blueprint, Fluent $column)
    {
        if ($column->collation !== null) {
            return ' collate ' . $this->wrapValue($column->collation);
        }

        return null;
    }

    protected function modifyNullable(Blueprint $blueprint, Fluent $column)
    {
        if ($column->change) {
            return $column->nullable ? 'drop not null' : 'set not null';
        }

        if ($column->autoIncrement && in_array($column->type, $this->serials)) {
            return null;
        }

        return $column->nullable ? ' null' : ' not null';
    }

    protected function modifyDefault(Blueprint $blueprint, Fluent $column)
    {
        if ($column->change) {
            if (! is_null($column->default)) {
                return 'set default ' . $this->getDefaultValue($column->default);
            }
            return 'drop default';
        }

        if ($column->default !== null) {
            return ' default ' . $this->getDefaultValue($column->default);
        }

        return null;
    }

    protected function modifyVirtualAs(Blueprint $blueprint, Fluent $column)
    {
        if ($column->virtualAs !== null) {
            return " generated always as ({$column->virtualAs})";
        }

        return null;
    }

    protected function modifyStoredAs(Blueprint $blueprint, Fluent $column)
    {
        if ($column->storedAs !== null) {
            return " generated always as ({$column->storedAs}) stored";
        }

        return null;
    }

    protected function modifyGeneratedAs(Blueprint $blueprint, Fluent $column)
    {
        if ($column->generatedAs !== null) {
            $expression = is_bool($column->generatedAs) ? '' : " ({$column->generatedAs})";
            return " generated by default as identity{$expression}";
        }

        return null;
    }

    protected function modifyIncrement(Blueprint $blueprint, Fluent $column)
    {
        if (in_array($column->type, $this->serials) && $column->autoIncrement) {
            return ' primary key';
        }

        return null;
    }

    /**
     * Wrap a single string in keyword identifiers (double quotes for PyroSQL).
     */
    protected function wrapValue($value)
    {
        if ($value === '*') {
            return $value;
        }

        return '"' . str_replace('"', '""', $value) . '"';
    }
}

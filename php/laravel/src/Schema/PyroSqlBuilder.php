<?php

declare(strict_types=1);

namespace PyroSQL\Laravel\Schema;

use Illuminate\Database\Schema\Builder;

/**
 * Schema builder for PyroSQL.
 *
 * Provides PyroSQL-specific schema introspection and management,
 * using information_schema and pg_indexes for metadata queries.
 */
class PyroSqlBuilder extends Builder
{
    /**
     * Determine if the given table exists.
     *
     * @param  string  $table
     * @return bool
     */
    public function hasTable($table): bool
    {
        $table = $this->connection->getTablePrefix() . $table;

        $result = $this->connection->selectOne(
            "select exists (select 1 from information_schema.tables "
            . "where table_schema = 'public' and table_name = ?) as \"exists\"",
            [$table],
        );

        if ($result === null) {
            return false;
        }

        $result = (object) $result;

        return filter_var($result->exists, FILTER_VALIDATE_BOOLEAN);
    }

    /**
     * Get the column listing for a given table.
     *
     * @param  string  $table
     * @return array<int, string>
     */
    public function getColumnListing($table): array
    {
        $table = $this->connection->getTablePrefix() . $table;

        $results = $this->connection->select(
            "select column_name from information_schema.columns "
            . "where table_schema = 'public' and table_name = ? "
            . "order by ordinal_position",
            [$table],
        );

        return array_map(function ($result) {
            return ((object) $result)->column_name;
        }, $results);
    }

    /**
     * Determine if the given table has a given column.
     *
     * @param  string  $table
     * @param  string  $column
     * @return bool
     */
    public function hasColumn($table, $column): bool
    {
        return in_array(
            strtolower($column),
            array_map('strtolower', $this->getColumnListing($table)),
        );
    }

    /**
     * Determine if the given table has given columns.
     *
     * @param  string  $table
     * @param  array<int, string>  $columns
     * @return bool
     */
    public function hasColumns($table, array $columns): bool
    {
        $tableColumns = array_map('strtolower', $this->getColumnListing($table));

        foreach ($columns as $column) {
            if (!in_array(strtolower($column), $tableColumns)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Get the data type for the given column name.
     *
     * @param  string  $table
     * @param  string  $column
     * @param  bool  $fullDefinition
     * @return string
     */
    public function getColumnType($table, $column, $fullDefinition = false): string
    {
        $table = $this->connection->getTablePrefix() . $table;

        $result = $this->connection->selectOne(
            "select data_type from information_schema.columns "
            . "where table_schema = 'public' and table_name = ? and column_name = ?",
            [$table, $column],
        );

        if ($result === null) {
            return '';
        }

        return ((object) $result)->data_type;
    }

    /**
     * Drop all tables from the database.
     *
     * @return void
     */
    public function dropAllTables(): void
    {
        $tables = $this->getTables();

        if (empty($tables)) {
            return;
        }

        $names = array_map(fn ($table) => is_array($table) ? ($table['name'] ?? '') : ((object) $table)->name, $tables);

        $this->connection->statement(
            $this->grammar->compileDropAllTables($names),
        );
    }

    /**
     * Drop all views from the database.
     *
     * @return void
     */
    public function dropAllViews(): void
    {
        $views = $this->getViews();

        if (empty($views)) {
            return;
        }

        $names = array_map(fn ($view) => is_array($view) ? ($view['name'] ?? '') : ((object) $view)->name, $views);

        $this->connection->statement(
            $this->grammar->compileDropAllViews($names),
        );
    }

    /**
     * Get the tables that belong to the database.
     *
     * @param  string|null  $schema
     * @return array<int, array<string, mixed>>
     */
    public function getTables($schema = null): array
    {
        return $this->connection->select(
            $this->grammar->compileTables($schema ?? 'public'),
        );
    }

    /**
     * Get the views that belong to the database.
     *
     * @param  string|null  $schema
     * @return array<int, array<string, mixed>>
     */
    public function getViews($schema = null): array
    {
        return $this->connection->select(
            $this->grammar->compileViews($schema ?? 'public'),
        );
    }

    /**
     * Get the columns for a given table.
     *
     * @param  string  $table
     * @return array<int, array<string, mixed>>
     */
    public function getColumns($table): array
    {
        $table = $this->connection->getTablePrefix() . $table;

        $results = $this->connection->select(
            $this->grammar->compileColumns('public', $table),
        );

        return $this->connection->getPostProcessor()->processColumns($results);
    }

    /**
     * Get the indexes for a given table.
     *
     * @param  string  $table
     * @return array<int, array<string, mixed>>
     */
    public function getIndexes($table): array
    {
        $table = $this->connection->getTablePrefix() . $table;

        $results = $this->connection->select(
            $this->grammar->compileIndexes('public', $table),
        );

        return $this->connection->getPostProcessor()->processIndexes($results);
    }

    /**
     * Get the foreign keys for a given table.
     *
     * @param  string  $table
     * @return array<int, array<string, mixed>>
     */
    public function getForeignKeys($table): array
    {
        $table = $this->connection->getTablePrefix() . $table;

        $results = $this->connection->select(
            $this->grammar->compileForeignKeys('public', $table),
        );

        return $this->connection->getPostProcessor()->processForeignKeys($results);
    }
}

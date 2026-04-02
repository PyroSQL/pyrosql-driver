<?php

declare(strict_types=1);

namespace PyroSQL\Laravel\Query;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor;

/**
 * Query processor for PyroSQL.
 *
 * Handles the RETURNING clause for inserts to retrieve the last inserted ID
 * without requiring a separate query (unlike MySQL's lastInsertId).
 */
class PyroSqlProcessor extends Processor
{
    /**
     * Process an "insert get ID" query.
     *
     * PyroSQL supports RETURNING clause on INSERT statements, so we compile
     * the insert with RETURNING and extract the ID from the result set.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  string  $sql
     * @param  array<int, mixed>  $values
     * @param  string|null  $sequence
     * @return int
     */
    public function processInsertGetId(Builder $query, $sql, $values, $sequence = null): int
    {
        $connection = $query->getConnection();

        $connection->recordsHaveBeenModified();

        $result = $connection->selectOne($sql, $values);

        $sequence = $sequence ?: 'id';

        if (is_object($result)) {
            $id = $result->{$sequence} ?? $result->id ?? 0;
        } elseif (is_array($result)) {
            $id = $result[$sequence] ?? $result['id'] ?? 0;
        } else {
            $id = 0;
        }

        return is_numeric($id) ? (int) $id : 0;
    }

    /**
     * Process the results of a column listing query.
     *
     * @param  array<int, \stdClass>  $results
     * @return array<int, string>
     */
    public function processColumnListing($results): array
    {
        return array_map(function ($result) {
            return ((object) $result)->column_name;
        }, $results);
    }

    /**
     * Process the results of a columns query.
     *
     * @param  array<int, \stdClass>  $results
     * @return array<int, array<string, mixed>>
     */
    public function processColumns($results): array
    {
        return array_map(function ($result) {
            $result = (object) $result;

            $autoincrement = $result->default !== null
                && str_starts_with(strtolower((string) $result->default), 'nextval(');

            return [
                'name' => $result->column_name,
                'type_name' => $result->data_type,
                'type' => $result->data_type,
                'collation' => $result->collation_name ?? null,
                'nullable' => strtolower((string) $result->is_nullable) === 'yes',
                'default' => $result->default ?? $result->column_default ?? null,
                'auto_increment' => $autoincrement,
                'comment' => null,
                'generation' => null,
            ];
        }, $results);
    }

    /**
     * Process the results of an indexes query.
     *
     * @param  array<int, \stdClass>  $results
     * @return array<int, array<string, mixed>>
     */
    public function processIndexes($results): array
    {
        return array_map(function ($result) {
            $result = (object) $result;

            $definition = $result->definition ?? $result->indexdef ?? '';
            $unique = str_contains(strtoupper($definition), 'UNIQUE');
            $primary = str_contains(strtolower($result->name ?? $result->indexname ?? ''), '_pkey');

            // Extract column names from index definition.
            $columns = [];
            if (preg_match('/\(([^)]+)\)/', $definition, $matches)) {
                $columns = array_map('trim', explode(',', $matches[1]));
                $columns = array_map(fn ($c) => trim($c, '"'), $columns);
            }

            return [
                'name' => $result->name ?? $result->indexname,
                'columns' => $columns,
                'type' => $primary ? 'primary' : ($unique ? 'unique' : 'index'),
                'unique' => $unique || $primary,
                'primary' => $primary,
            ];
        }, $results);
    }

    /**
     * Process the results of a foreign keys query.
     *
     * @param  array<int, \stdClass>  $results
     * @return array<int, array<string, mixed>>
     */
    public function processForeignKeys($results): array
    {
        return array_map(function ($result) {
            $result = (object) $result;

            return [
                'name' => $result->constraint_name,
                'columns' => [$result->column_name],
                'foreign_schema' => $result->foreign_schema ?? 'public',
                'foreign_table' => $result->foreign_table_name,
                'foreign_columns' => [$result->foreign_column_name],
                'on_update' => $result->on_update ?? 'NO ACTION',
                'on_delete' => $result->on_delete ?? 'NO ACTION',
            ];
        }, $results);
    }
}

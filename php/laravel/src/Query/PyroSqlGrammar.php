<?php

declare(strict_types=1);

namespace PyroSQL\Laravel\Query;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;

/**
 * Query grammar for PyroSQL.
 *
 * PyroSQL uses PostgreSQL-compatible SQL syntax with:
 * - SERIAL / BIGSERIAL for auto-increment columns
 * - RETURNING clause for INSERT statements
 * - Native boolean literals (true/false, not 1/0)
 * - LIMIT / OFFSET syntax
 * - Double-quote identifier quoting
 * - ILIKE for case-insensitive LIKE
 * - JSONB operators
 */
class PyroSqlGrammar extends Grammar
{
    /**
     * All of the available clause operators.
     *
     * @var string[]
     */
    protected $operators = [
        '=', '<', '>', '<=', '>=', '<>', '!=',
        'like', 'not like', 'ilike', 'not ilike',
        '~', '~*', '!~', '!~*',
        'similar to', 'not similar to',
        '&', '|', '#', '<<', '>>',
        '&&',      // array overlap
        '@>', '<@', // containment
        '?', '?|', '?&', // jsonb
        '||',      // concatenation / jsonb merge
        '->', '->>', '#>', '#>>', // jsonb path
    ];

    /**
     * The components that make up a select clause.
     *
     * @var string[]
     */
    protected $selectComponents = [
        'aggregate',
        'columns',
        'from',
        'indexHint',
        'joins',
        'wheres',
        'groups',
        'havings',
        'orders',
        'limit',
        'offset',
        'lock',
    ];

    /**
     * Compile a select query into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return string
     */
    public function compileSelect(Builder $query): string
    {
        return parent::compileSelect($query);
    }

    /**
     * Compile the "limit" portions of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  int  $limit
     * @return string
     */
    protected function compileLimit(Builder $query, $limit): string
    {
        return 'limit ' . (int) $limit;
    }

    /**
     * Compile the "offset" portions of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  int  $offset
     * @return string
     */
    protected function compileOffset(Builder $query, $offset): string
    {
        return 'offset ' . (int) $offset;
    }

    /**
     * Compile the lock into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  bool|string  $value
     * @return string
     */
    protected function compileLock(Builder $query, $value): string
    {
        if (is_string($value)) {
            return $value;
        }

        return $value ? 'for update' : 'for share';
    }

    /**
     * Compile an insert statement with a RETURNING clause to get the ID.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array<string, mixed>  $values
     * @param  string  $sequence
     * @return string
     */
    public function compileInsertGetId(Builder $query, $values, $sequence): string
    {
        $sequence = $sequence ?: 'id';

        return $this->compileInsert($query, $values) . ' returning ' . $this->wrap($sequence);
    }

    /**
     * Compile an insert statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array<int, array<string, mixed>>  $values
     * @return string
     */
    public function compileInsert(Builder $query, array $values): string
    {
        return parent::compileInsert($query, $values);
    }

    /**
     * Compile an insert and get ID statement into SQL.
     * This handles the RETURNING clause used by PyroSQL for getting
     * the last inserted ID without a separate query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array<string, mixed>  $values
     * @param  string  $sequence
     * @return string
     */
    public function compileInsertOrIgnore(Builder $query, array $values): string
    {
        return $this->compileInsert($query, $values) . ' on conflict do nothing';
    }

    /**
     * Compile an "upsert" statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array<int, array<string, mixed>>  $values
     * @param  array<int, string>  $uniqueBy
     * @param  array<int, string>  $update
     * @return string
     */
    public function compileUpsert(Builder $query, array $values, array $uniqueBy, array $update): string
    {
        $sql = $this->compileInsert($query, $values);

        $columns = $this->columnize($uniqueBy);

        $sql .= ' on conflict (' . $columns . ') do update set ';

        $assignments = [];
        foreach ($update as $column) {
            $assignments[] = $this->wrap($column) . ' = ' . $this->wrap('excluded') . '.' . $this->wrap($column);
        }

        return $sql . implode(', ', $assignments);
    }

    /**
     * Compile an update statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array<string, mixed>  $values
     * @return string
     */
    public function compileUpdate(Builder $query, array $values): string
    {
        return parent::compileUpdate($query, $values);
    }

    /**
     * Compile a delete statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return string
     */
    public function compileDelete(Builder $query): string
    {
        return parent::compileDelete($query);
    }

    /**
     * Compile a truncate table statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return array<string, array<int, mixed>>
     */
    public function compileTruncate(Builder $query): array
    {
        return ['truncate ' . $this->wrapTable($query->from) . ' restart identity cascade' => []];
    }

    /**
     * Compile a "where date" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array<string, mixed>  $where
     * @return string
     */
    protected function whereDate(Builder $query, $where): string
    {
        $value = $this->parameter($where['value']);

        return $this->wrap($where['column']) . '::date ' . $where['operator'] . ' ' . $value;
    }

    /**
     * Compile a "where time" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array<string, mixed>  $where
     * @return string
     */
    protected function whereTime(Builder $query, $where): string
    {
        $value = $this->parameter($where['value']);

        return $this->wrap($where['column']) . '::time ' . $where['operator'] . ' ' . $value;
    }

    /**
     * Compile a date based where clause (day, month, year).
     *
     * @param  string  $type
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array<string, mixed>  $where
     * @return string
     */
    protected function dateBasedWhere($type, Builder $query, $where): string
    {
        $value = $this->parameter($where['value']);

        return 'extract(' . $type . ' from ' . $this->wrap($where['column']) . ') ' . $where['operator'] . ' ' . $value;
    }

    /**
     * Compile the "select *" portion of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array<int, string|\Illuminate\Database\Query\Expression>  $columns
     * @return string|null
     */
    protected function compileColumns(Builder $query, $columns): ?string
    {
        return parent::compileColumns($query, $columns);
    }

    /**
     * Wrap a value in keyword identifiers (double quotes for PyroSQL).
     *
     * @param  \Illuminate\Database\Query\Expression|string  $value
     * @param  bool  $prefixAlias
     * @return string
     */
    public function wrap($value, $prefixAlias = false): string
    {
        return parent::wrap($value, $prefixAlias);
    }

    /**
     * Wrap a single string in keyword identifiers.
     *
     * @param  string  $value
     * @return string
     */
    protected function wrapValue($value): string
    {
        if ($value === '*') {
            return $value;
        }

        return '"' . str_replace('"', '""', $value) . '"';
    }

    /**
     * Compile a "where fulltext" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array<string, mixed>  $where
     * @return string
     */
    public function whereFullText(Builder $query, $where): string
    {
        $language = $where['options']['language'] ?? 'english';
        $columns = $this->columnize($where['columns']);

        $mode = $where['options']['mode'] ?? 'plain';

        $function = match ($mode) {
            'plain' => 'plainto_tsquery',
            'phrase' => 'phraseto_tsquery',
            'websearch' => 'websearch_to_tsquery',
            default => 'plainto_tsquery',
        };

        return "to_tsvector('{$language}', {$columns}) @@ {$function}('{$language}', ?)";
    }

    /**
     * Compile a "JSON contains" statement.
     *
     * @param  string  $column
     * @param  string  $value
     * @return string
     */
    protected function compileJsonContains($column, $value): string
    {
        $column = $this->wrap($column);

        return '(' . $column . ')::jsonb @> ' . $value;
    }

    /**
     * Compile a "JSON contains key" statement.
     *
     * @param  string  $column
     * @return string
     */
    protected function compileJsonContainsKey($column): string
    {
        $parts = explode('->', $column);
        $field = $this->wrap(array_shift($parts));

        $lastKey = array_pop($parts);

        if (count($parts) > 0) {
            $path = implode(',', array_map(fn ($part) => "'" . trim($part, '"\'') . "'", $parts));
            return '(' . $field . '#>\'{' . $path . '}\')::jsonb ? \'' . trim($lastKey, '"\'') . '\'';
        }

        return $field . '::jsonb ? \'' . trim($lastKey, '"\'') . '\'';
    }

    /**
     * Compile a "JSON length" statement.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  string  $value
     * @return string
     */
    protected function compileJsonLength($column, $operator, $value): string
    {
        $column = $this->wrap($column);

        return 'jsonb_array_length((' . $column . ')::jsonb) ' . $operator . ' ' . $value;
    }

    /**
     * Get the grammar-specific bit operators.
     *
     * @return array<int, string>
     */
    public function getBitwiseOperators(): array
    {
        return ['&', '|', '#', '<<', '>>', '~'];
    }

    /**
     * Determine if the grammar supports savepoints.
     *
     * @return bool
     */
    public function supportsSavepoints(): bool
    {
        return true;
    }

    /**
     * Compile the SQL for creating a savepoint.
     *
     * @param  string  $name
     * @return string
     */
    public function compileSavepoint($name): string
    {
        return 'SAVEPOINT ' . $name;
    }

    /**
     * Compile the SQL for rolling back to a savepoint.
     *
     * @param  string  $name
     * @return string
     */
    public function compileSavepointRollBack($name): string
    {
        return 'ROLLBACK TO SAVEPOINT ' . $name;
    }

    /**
     * Prepare the bindings for a query with boolean value normalization.
     *
     * @param  array<string, array<int, mixed>>  $bindings
     * @return array<int, mixed>
     */
    public function prepareBindingsForUpdate(array $bindings, array $values): array
    {
        // Convert booleans to native string literals for PyroSQL.
        $cleaned = [];
        foreach ($bindings as $key => $binding) {
            if (is_array($binding)) {
                foreach ($binding as $b) {
                    $cleaned[] = $b;
                }
            } else {
                $cleaned[] = $binding;
            }
        }

        return $cleaned;
    }
}

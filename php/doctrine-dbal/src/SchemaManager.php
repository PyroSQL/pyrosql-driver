<?php

declare(strict_types=1);

namespace PyroSQL\DoctrineDBAL;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Schema\View;
use Doctrine\DBAL\Types\Type;

/**
 * Schema manager for PyroSQL.
 *
 * Queries information_schema and pg_indexes to introspect tables,
 * columns, indexes, and foreign keys -- matching the catalog structure
 * exposed by the PyroSQL server.
 */
class SchemaManager extends AbstractSchemaManager
{
    /**
     * {@inheritDoc}
     */
    protected function _getPortableTableColumnDefinition(array $tableColumn): Column
    {
        $dbType  = strtolower(trim($tableColumn['data_type'] ?? 'text'));
        $length  = isset($tableColumn['character_maximum_length'])
            ? (int) $tableColumn['character_maximum_length']
            : null;

        $precision = isset($tableColumn['numeric_precision'])
            ? (int) $tableColumn['numeric_precision']
            : null;

        $scale = isset($tableColumn['numeric_scale'])
            ? (int) $tableColumn['numeric_scale']
            : null;

        $notnull = isset($tableColumn['is_nullable'])
            && strtolower($tableColumn['is_nullable']) === 'no';

        $default = $tableColumn['column_default'] ?? null;

        // Detect autoincrement from nextval() defaults
        $autoincrement = false;
        if ($default !== null && str_contains(strtolower($default), 'nextval(')) {
            $autoincrement = true;
            $default = null; // Don't expose the sequence call as a default
        }

        $type = $this->extractDoctrineTypeFromDbType($dbType);

        $column = new Column(
            $tableColumn['column_name'],
            Type::getType($type),
        );

        $column->setNotnull($notnull);
        $column->setAutoincrement($autoincrement);

        if ($default !== null) {
            // Strip type casts like ::text, ::integer
            $cleanDefault = preg_replace('/::[\w\s]+$/', '', $default);
            // Strip surrounding quotes from string defaults
            if (preg_match("/^'(.*)'$/s", $cleanDefault, $m)) {
                $cleanDefault = str_replace("''", "'", $m[1]);
            }
            $column->setDefault($cleanDefault);
        }

        if ($length !== null) {
            $column->setLength($length);
        }

        if ($precision !== null) {
            $column->setPrecision($precision);
        }

        if ($scale !== null) {
            $column->setScale($scale);
        }

        // Detect fixed-length char
        if ($dbType === 'character' || $dbType === 'char') {
            $column->setFixed(true);
        }

        return $column;
    }

    /**
     * Map a database type string to a Doctrine abstract type name.
     */
    private function extractDoctrineTypeFromDbType(string $dbType): string
    {
        // Normalize type strings that may include size specifications
        $normalized = preg_replace('/\(.*\)/', '', $dbType);
        $normalized = trim($normalized);

        $map = [
            'smallint'                       => 'smallint',
            'int2'                           => 'smallint',
            'integer'                        => 'integer',
            'int'                            => 'integer',
            'int4'                           => 'integer',
            'serial'                         => 'integer',
            'bigint'                         => 'bigint',
            'int8'                           => 'bigint',
            'bigserial'                      => 'bigint',
            'real'                           => 'float',
            'float4'                         => 'float',
            'double precision'               => 'float',
            'float8'                         => 'float',
            'numeric'                        => 'decimal',
            'decimal'                        => 'decimal',
            'boolean'                        => 'boolean',
            'bool'                           => 'boolean',
            'character varying'              => 'string',
            'varchar'                        => 'string',
            'character'                      => 'string',
            'char'                           => 'string',
            'text'                           => 'text',
            'uuid'                           => 'guid',
            'bytea'                          => 'blob',
            'date'                           => 'date',
            'timestamp without time zone'    => 'datetime',
            'timestamp'                      => 'datetime',
            'timestamp with time zone'       => 'datetimetz',
            'timestamptz'                    => 'datetimetz',
            'time without time zone'         => 'time',
            'time'                           => 'time',
            'time with time zone'            => 'time',
            'json'                           => 'json',
            'jsonb'                          => 'json',
        ];

        return $map[$normalized] ?? 'text';
    }

    /**
     * {@inheritDoc}
     */
    protected function _getPortableTableIndexesList(array $tableIndexes, string $tableName): array
    {
        $indexes = [];

        foreach ($tableIndexes as $row) {
            $name = $row['name'] ?? '';
            $definition = $row['definition'] ?? '';

            if ($name === '') {
                continue;
            }

            $unique = str_contains(strtoupper($definition), 'UNIQUE');
            $primary = str_contains($name, '_pkey');

            // Parse column names from index definition
            // Example: CREATE INDEX idx_name ON table (col1, col2)
            $columns = [];
            if (preg_match('/\(([^)]+)\)/', $definition, $m)) {
                $columns = array_map('trim', explode(',', $m[1]));
                // Strip quoting
                $columns = array_map(
                    fn(string $c): string => trim($c, '"'),
                    $columns,
                );
            }

            if (empty($columns)) {
                continue;
            }

            $indexes[$name] = new Index(
                $name,
                $columns,
                $unique,
                $primary,
            );
        }

        return $indexes;
    }

    /**
     * {@inheritDoc}
     */
    protected function _getPortableTableForeignKeysList(array $tableForeignKeys): array
    {
        $foreignKeys = [];

        foreach ($tableForeignKeys as $row) {
            $constraintName = $row['constraint_name'] ?? '';
            $localColumn    = $row['column_name'] ?? '';
            $foreignTable   = $row['foreign_table_name'] ?? '';
            $foreignColumn  = $row['foreign_column_name'] ?? '';

            if ($constraintName === '' || $foreignTable === '') {
                continue;
            }

            // Group columns by constraint name
            if (!isset($foreignKeys[$constraintName])) {
                $foreignKeys[$constraintName] = [
                    'name'            => $constraintName,
                    'local_columns'   => [],
                    'foreign_table'   => $foreignTable,
                    'foreign_columns' => [],
                ];
            }

            $foreignKeys[$constraintName]['local_columns'][]   = $localColumn;
            $foreignKeys[$constraintName]['foreign_columns'][] = $foreignColumn;
        }

        $result = [];
        foreach ($foreignKeys as $fk) {
            $result[] = new ForeignKeyConstraint(
                $fk['local_columns'],
                $fk['foreign_table'],
                $fk['foreign_columns'],
                $fk['name'],
            );
        }

        return $result;
    }

    /**
     * {@inheritDoc}
     */
    protected function _getPortableTableDefinition(array $table): string
    {
        return $table['table_name'];
    }

    /**
     * {@inheritDoc}
     */
    protected function selectTableNames(string $databaseName): \Doctrine\DBAL\Result
    {
        $sql = "SELECT table_name FROM information_schema.tables "
            . "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
            . "ORDER BY table_name";

        return $this->_conn->executeQuery($sql);
    }

    /**
     * {@inheritDoc}
     */
    protected function selectTableColumns(string $databaseName, ?string $tableName = null): \Doctrine\DBAL\Result
    {
        $sql = "SELECT table_name, column_name, data_type, is_nullable, column_default, "
            . "character_maximum_length, numeric_precision, numeric_scale "
            . "FROM information_schema.columns "
            . "WHERE table_schema = 'public'";

        if ($tableName !== null) {
            $sql .= " AND table_name = " . $this->_platform->quoteStringLiteral($tableName);
        }

        $sql .= " ORDER BY table_name, ordinal_position";

        return $this->_conn->executeQuery($sql);
    }

    /**
     * {@inheritDoc}
     */
    protected function selectIndexColumns(string $databaseName, ?string $tableName = null): \Doctrine\DBAL\Result
    {
        $sql = "SELECT tablename AS table_name, indexname AS name, indexdef AS definition "
            . "FROM pg_indexes WHERE schemaname = 'public'";

        if ($tableName !== null) {
            $sql .= " AND tablename = " . $this->_platform->quoteStringLiteral($tableName);
        }

        $sql .= " ORDER BY tablename, indexname";

        return $this->_conn->executeQuery($sql);
    }

    /**
     * {@inheritDoc}
     */
    protected function selectForeignKeyColumns(string $databaseName, ?string $tableName = null): \Doctrine\DBAL\Result
    {
        $sql = "SELECT "
            . "tc.table_name, "
            . "tc.constraint_name, "
            . "kcu.column_name, "
            . "ccu.table_name AS foreign_table_name, "
            . "ccu.column_name AS foreign_column_name "
            . "FROM information_schema.table_constraints AS tc "
            . "JOIN information_schema.key_column_usage AS kcu "
            . "ON tc.constraint_name = kcu.constraint_name "
            . "JOIN information_schema.constraint_column_usage AS ccu "
            . "ON ccu.constraint_name = tc.constraint_name "
            . "WHERE tc.constraint_type = 'FOREIGN KEY' "
            . "AND tc.table_schema = 'public'";

        if ($tableName !== null) {
            $sql .= " AND tc.table_name = " . $this->_platform->quoteStringLiteral($tableName);
        }

        $sql .= " ORDER BY tc.table_name, tc.constraint_name";

        return $this->_conn->executeQuery($sql);
    }

    /**
     * {@inheritDoc}
     */
    protected function fetchTableColumnsByTable(string $databaseName): array
    {
        $result = $this->selectTableColumns($databaseName);
        $columns = [];

        foreach ($result->fetchAllAssociative() as $row) {
            $tableName = $row['table_name'];
            $columns[$tableName][] = $row;
        }

        return $columns;
    }

    /**
     * {@inheritDoc}
     */
    protected function fetchTableIndexesByTable(string $databaseName): array
    {
        $result = $this->selectIndexColumns($databaseName);
        $indexes = [];

        foreach ($result->fetchAllAssociative() as $row) {
            $tableName = $row['table_name'];
            $indexes[$tableName][] = $row;
        }

        return $indexes;
    }

    /**
     * {@inheritDoc}
     */
    protected function fetchTableForeignKeysByTable(string $databaseName): array
    {
        $result = $this->selectForeignKeyColumns($databaseName);
        $foreignKeys = [];

        foreach ($result->fetchAllAssociative() as $row) {
            $tableName = $row['table_name'];
            $foreignKeys[$tableName][] = $row;
        }

        return $foreignKeys;
    }

    /**
     * {@inheritDoc}
     */
    protected function fetchTableOptionsByTable(string $databaseName, ?string $tableName = null): array
    {
        // PyroSQL does not expose table-level options (engine, collation, etc.)
        // Return empty arrays keyed by table name.
        $result = $this->selectTableNames($databaseName);
        $options = [];

        foreach ($result->fetchAllAssociative() as $row) {
            $name = $row['table_name'];
            if ($tableName !== null && $name !== $tableName) {
                continue;
            }
            $options[$name] = [];
        }

        return $options;
    }

    /**
     * {@inheritDoc}
     */
    protected function _getPortableViewDefinition(array $view): View
    {
        $name = $view['viewname'] ?? $view['table_name'] ?? '';
        $sql  = $view['definition'] ?? $view['view_definition'] ?? '';

        return new View($name, $sql);
    }

    /**
     * {@inheritDoc}
     */
    protected function _getPortableTableForeignKeyDefinition(array $tableForeignKey): ForeignKeyConstraint
    {
        $constraintName = $tableForeignKey['constraint_name'] ?? '';
        $localColumn    = $tableForeignKey['column_name'] ?? '';
        $foreignTable   = $tableForeignKey['foreign_table_name'] ?? '';
        $foreignColumn  = $tableForeignKey['foreign_column_name'] ?? '';

        return new ForeignKeyConstraint(
            [$localColumn],
            $foreignTable,
            [$foreignColumn],
            $constraintName,
        );
    }
}

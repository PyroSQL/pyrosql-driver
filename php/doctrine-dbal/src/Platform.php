<?php

declare(strict_types=1);

namespace PyroSQL\DoctrineDBAL;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\DateIntervalUnit;
use Doctrine\DBAL\Platforms\TrimMode;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\TransactionIsolationLevel;
use Doctrine\DBAL\Types\Types;

/**
 * Doctrine DBAL Platform implementation for PyroSQL.
 *
 * PyroSQL uses PostgreSQL-compatible SQL syntax with some extensions.
 * This platform defines:
 *
 *  - Column type mappings (TEXT, INTEGER, BOOLEAN, TIMESTAMP, SERIAL, etc.)
 *  - Double-quote identifier quoting
 *  - LIMIT/OFFSET syntax
 *  - Boolean literals (true/false, not 1/0)
 *  - NOW() for current timestamp
 *  - CREATE TABLE / ALTER TABLE SQL generation
 */
class Platform extends AbstractPlatform
{
    /**
     * {@inheritDoc}
     */
    public function getListTablesSQL(): string
    {
        return "SELECT table_name FROM information_schema.tables "
            . "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'";
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableColumnsSQL(string $table, ?string $database = null): string
    {
        return sprintf(
            "SELECT column_name, data_type, is_nullable, column_default, "
            . "character_maximum_length, numeric_precision, numeric_scale "
            . "FROM information_schema.columns "
            . "WHERE table_schema = 'public' AND table_name = %s "
            . "ORDER BY ordinal_position",
            $this->quoteStringLiteral($table),
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableIndexesSQL(string $table, ?string $database = null): string
    {
        return sprintf(
            "SELECT indexname AS name, indexdef AS definition "
            . "FROM pg_indexes "
            . "WHERE schemaname = 'public' AND tablename = %s",
            $this->quoteStringLiteral($table),
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableForeignKeysSQL(string $table, ?string $database = null): string
    {
        return sprintf(
            "SELECT "
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
            . "AND tc.table_schema = 'public' "
            . "AND tc.table_name = %s",
            $this->quoteStringLiteral($table),
        );
    }

    // ── Identifier quoting ──────────────────────────────────────────────

    /**
     * {@inheritDoc}
     */
    protected function getQuotedNameOf(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }

    /**
     * {@inheritDoc}
     */
    public function quoteIdentifier(string $identifier): string
    {
        if (str_contains($identifier, '.')) {
            $parts = array_map(
                fn(string $p): string => $this->quoteSingleIdentifier($p),
                explode('.', $identifier),
            );
            return implode('.', $parts);
        }

        return $this->quoteSingleIdentifier($identifier);
    }

    /**
     * {@inheritDoc}
     */
    public function quoteSingleIdentifier(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }

    // ── Boolean handling ────────────────────────────────────────────────

    /**
     * PyroSQL uses native boolean literals, not integers.
     */
    public function convertBooleansToDatabaseValue(mixed $item): mixed
    {
        if ($item === null) {
            return null;
        }

        return $item ? 'true' : 'false';
    }

    /**
     * Convert a PyroSQL boolean value to PHP boolean.
     */
    public function convertFromBoolean(mixed $item): ?bool
    {
        if ($item === null) {
            return null;
        }

        if (is_bool($item)) {
            return $item;
        }

        if (is_string($item)) {
            $lower = strtolower($item);
            if ($lower === 't' || $lower === 'true' || $lower === '1') {
                return true;
            }
            return false;
        }

        return (bool) $item;
    }

    // ── SQL expressions ─────────────────────────────────────────────────

    /**
     * {@inheritDoc}
     */
    public function getCurrentTimestampSQL(): string
    {
        return 'NOW()';
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentDateSQL(): string
    {
        return 'CURRENT_DATE';
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentTimeSQL(): string
    {
        return 'CURRENT_TIME';
    }

    /**
     * {@inheritDoc}
     */
    public function getLocateExpression(string $string, string $substring, ?string $start = null): string
    {
        if ($start !== null) {
            return sprintf('POSITION(%s IN SUBSTRING(%s FROM %s))', $substring, $string, $start);
        }

        return sprintf('POSITION(%s IN %s)', $substring, $string);
    }

    /**
     * {@inheritDoc}
     */
    public function getSubstringExpression(string $string, string $start, ?string $length = null): string
    {
        if ($length !== null) {
            return sprintf('SUBSTRING(%s FROM %s FOR %s)', $string, $start, $length);
        }

        return sprintf('SUBSTRING(%s FROM %s)', $string, $start);
    }

    /**
     * {@inheritDoc}
     */
    public function getConcatExpression(string ...$strings): string
    {
        return implode(' || ', $strings);
    }

    /**
     * {@inheritDoc}
     */
    public function getLengthExpression(string $column): string
    {
        return sprintf('LENGTH(%s)', $column);
    }

    /**
     * {@inheritDoc}
     */
    public function getTrimExpression(
        string $str,
        TrimMode $mode = TrimMode::UNSPECIFIED,
        ?string $char = null,
    ): string {
        $positionClause = match ($mode) {
            TrimMode::LEADING    => 'LEADING',
            TrimMode::TRAILING   => 'TRAILING',
            TrimMode::BOTH       => 'BOTH',
            TrimMode::UNSPECIFIED => 'BOTH',
        };

        if ($char !== null) {
            return sprintf('TRIM(%s %s FROM %s)', $positionClause, $char, $str);
        }

        return sprintf('TRIM(%s FROM %s)', $positionClause, $str);
    }

    // ── LIMIT / OFFSET ─────────────────────────────────────────────────

    /**
     * {@inheritDoc}
     */
    protected function doModifyLimitQuery(string $query, ?int $limit, int $offset): string
    {
        if ($limit !== null) {
            $query .= ' LIMIT ' . $limit;
        }

        if ($offset > 0) {
            $query .= ' OFFSET ' . $offset;
        }

        return $query;
    }

    // ── Column type mappings ────────────────────────────────────────────

    /**
     * Map Doctrine abstract types to PyroSQL native column types.
     */
    public function getColumnDeclarationSQL(string $name, array $column): string
    {
        $typeDecl = $this->getColumnTypeDeclarationSQL($column);

        $declaration = $this->quoteSingleIdentifier($name) . ' ' . $typeDecl;

        if (isset($column['notnull']) && $column['notnull']) {
            $declaration .= ' NOT NULL';
        }

        if (isset($column['default'])) {
            $declaration .= ' DEFAULT ' . $this->getDefaultValueDeclarationSQL($column);
        }

        return $declaration;
    }

    /**
     * Return the SQL snippet for a column type.
     */
    private function getColumnTypeDeclarationSQL(array $column): string
    {
        if (!isset($column['type'])) {
            return 'TEXT';
        }

        $type = $column['type'];

        if (is_object($type)) {
            $typeName = $type->getName();
        } else {
            $typeName = (string) $type;
        }

        return match ($typeName) {
            Types::SMALLINT                      => 'SMALLINT',
            Types::INTEGER                       => isset($column['autoincrement']) && $column['autoincrement'] ? 'SERIAL' : 'INTEGER',
            Types::BIGINT                        => isset($column['autoincrement']) && $column['autoincrement'] ? 'BIGSERIAL' : 'BIGINT',
            Types::FLOAT                         => 'REAL',
            Types::DECIMAL                       => $this->getDecimalTypeDeclarationSQL($column),
            Types::BOOLEAN                       => 'BOOLEAN',
            Types::STRING                        => $this->getVarcharTypeDeclarationSQL($column),
            Types::TEXT                          => 'TEXT',
            Types::GUID                          => 'UUID',
            Types::BINARY                        => 'BYTEA',
            Types::BLOB                          => 'BYTEA',
            Types::DATE_MUTABLE,
            Types::DATE_IMMUTABLE                => 'DATE',
            Types::DATETIME_MUTABLE,
            Types::DATETIME_IMMUTABLE            => 'TIMESTAMP',
            Types::DATETIMETZ_MUTABLE,
            Types::DATETIMETZ_IMMUTABLE          => 'TIMESTAMP WITH TIME ZONE',
            Types::TIME_MUTABLE,
            Types::TIME_IMMUTABLE                => 'TIME',
            Types::JSON                          => 'JSONB',
            Types::SIMPLE_ARRAY,
            Types::ARRAY                         => 'TEXT',
            default                              => 'TEXT',
        };
    }

    /**
     * {@inheritDoc}
     */
    public function getVarcharTypeDeclarationSQL(array $column): string
    {
        $length = $column['length'] ?? 255;

        if (isset($column['fixed']) && $column['fixed']) {
            return 'CHAR(' . $length . ')';
        }

        return 'VARCHAR(' . $length . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getDecimalTypeDeclarationSQL(array $column): string
    {
        $precision = $column['precision'] ?? 10;
        $scale     = $column['scale'] ?? 0;

        return 'NUMERIC(' . $precision . ', ' . $scale . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getClobTypeDeclarationSQL(array $column): string
    {
        return 'TEXT';
    }

    /**
     * {@inheritDoc}
     */
    public function getBlobTypeDeclarationSQL(array $column): string
    {
        return 'BYTEA';
    }

    /**
     * {@inheritDoc}
     */
    public function getBooleanTypeDeclarationSQL(array $column): string
    {
        return 'BOOLEAN';
    }

    /**
     * {@inheritDoc}
     */
    public function getIntegerTypeDeclarationSQL(array $column): string
    {
        if (isset($column['autoincrement']) && $column['autoincrement']) {
            return 'SERIAL';
        }

        return 'INTEGER';
    }

    /**
     * {@inheritDoc}
     */
    public function getBigIntTypeDeclarationSQL(array $column): string
    {
        if (isset($column['autoincrement']) && $column['autoincrement']) {
            return 'BIGSERIAL';
        }

        return 'BIGINT';
    }

    /**
     * {@inheritDoc}
     */
    public function getSmallIntTypeDeclarationSQL(array $column): string
    {
        return 'SMALLINT';
    }

    /**
     * {@inheritDoc}
     */
    public function getFloatDeclarationSQL(array $column): string
    {
        return 'REAL';
    }

    // ── DDL generation ──────────────────────────────────────────────────

    /**
     * {@inheritDoc}
     */
    public function getCreateTableSQL(Table $table): array
    {
        $sql = [];

        $columnListSql = [];
        foreach ($table->getColumns() as $column) {
            $columnListSql[] = $this->getColumnDeclarationSQL(
                $column->getQuotedName($this),
                $this->columnToArray($column),
            );
        }

        // Primary key
        $primaryKey = $table->getPrimaryKey();
        if ($primaryKey !== null) {
            $columns = array_map(
                fn(string $col): string => $this->quoteSingleIdentifier($col),
                $primaryKey->getColumns(),
            );
            $columnListSql[] = sprintf('PRIMARY KEY (%s)', implode(', ', $columns));
        }

        // Unique constraints
        foreach ($table->getUniqueConstraints() as $uniqueConstraint) {
            $columns = array_map(
                fn(string $col): string => $this->quoteSingleIdentifier($col),
                $uniqueConstraint->getColumns(),
            );
            $columnListSql[] = sprintf('UNIQUE (%s)', implode(', ', $columns));
        }

        // Foreign keys
        foreach ($table->getForeignKeys() as $fk) {
            $localColumns = array_map(
                fn(string $col): string => $this->quoteSingleIdentifier($col),
                $fk->getLocalColumns(),
            );
            $foreignColumns = array_map(
                fn(string $col): string => $this->quoteSingleIdentifier($col),
                $fk->getForeignColumns(),
            );

            $columnListSql[] = sprintf(
                'FOREIGN KEY (%s) REFERENCES %s (%s)',
                implode(', ', $localColumns),
                $this->quoteSingleIdentifier($fk->getForeignTableName()),
                implode(', ', $foreignColumns),
            );
        }

        $tableName = $table->getQuotedName($this);
        $sql[] = sprintf(
            'CREATE TABLE %s (%s)',
            $tableName,
            implode(', ', $columnListSql),
        );

        // Indexes (non-primary)
        foreach ($table->getIndexes() as $index) {
            if ($index->isPrimary()) {
                continue;
            }

            $sql[] = $this->getCreateIndexSQL($index, $table->getName());
        }

        return $sql;
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateIndexSQL(Index $index, string $table): string
    {
        $tableName = $this->quoteSingleIdentifier($table);
        $indexName = $this->quoteSingleIdentifier($index->getName());

        $columns = array_map(
            fn(string $col): string => $this->quoteSingleIdentifier($col),
            $index->getColumns(),
        );

        $unique = $index->isUnique() ? 'UNIQUE ' : '';

        return sprintf(
            'CREATE %sINDEX %s ON %s (%s)',
            $unique,
            $indexName,
            $tableName,
            implode(', ', $columns),
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getDropTableSQL(string $table): string
    {
        return 'DROP TABLE ' . $this->quoteSingleIdentifier($table);
    }

    /**
     * {@inheritDoc}
     */
    public function getDropIndexSQL(string $name, string $table): string
    {
        return 'DROP INDEX ' . $this->quoteSingleIdentifier($name);
    }

    /**
     * {@inheritDoc}
     */
    public function getAlterTableSQL(TableDiff $diff): array
    {
        $sql = [];
        $tableName = $diff->getOldTable()->getQuotedName($this);

        // Added columns
        foreach ($diff->getAddedColumns() as $column) {
            $columnArray = $this->columnToArray($column);
            $sql[] = sprintf(
                'ALTER TABLE %s ADD COLUMN %s',
                $tableName,
                $this->getColumnDeclarationSQL($column->getQuotedName($this), $columnArray),
            );
        }

        // Dropped columns
        foreach ($diff->getDroppedColumns() as $column) {
            $sql[] = sprintf(
                'ALTER TABLE %s DROP COLUMN %s',
                $tableName,
                $this->quoteSingleIdentifier($column->getName()),
            );
        }

        // Modified columns
        foreach ($diff->getModifiedColumns() as $columnDiff) {
            $newColumn = $columnDiff->getNewColumn();
            $columnArray = $this->columnToArray($newColumn);
            $typeDecl = $this->getColumnTypeDeclarationSQL($columnArray);

            $sql[] = sprintf(
                'ALTER TABLE %s ALTER COLUMN %s TYPE %s',
                $tableName,
                $this->quoteSingleIdentifier($newColumn->getName()),
                $typeDecl,
            );

            // Nullability change
            if ($newColumn->getNotnull()) {
                $sql[] = sprintf(
                    'ALTER TABLE %s ALTER COLUMN %s SET NOT NULL',
                    $tableName,
                    $this->quoteSingleIdentifier($newColumn->getName()),
                );
            } else {
                $sql[] = sprintf(
                    'ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL',
                    $tableName,
                    $this->quoteSingleIdentifier($newColumn->getName()),
                );
            }

            // Default change
            $default = $newColumn->getDefault();
            if ($default !== null) {
                $sql[] = sprintf(
                    'ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s',
                    $tableName,
                    $this->quoteSingleIdentifier($newColumn->getName()),
                    $this->quoteStringLiteral($default),
                );
            } else {
                $sql[] = sprintf(
                    'ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT',
                    $tableName,
                    $this->quoteSingleIdentifier($newColumn->getName()),
                );
            }
        }

        // Renamed columns
        foreach ($diff->getRenamedColumns() as $oldName => $newColumn) {
            $sql[] = sprintf(
                'ALTER TABLE %s RENAME COLUMN %s TO %s',
                $tableName,
                $this->quoteSingleIdentifier($oldName),
                $this->quoteSingleIdentifier($newColumn->getName()),
            );
        }

        // Added indexes
        foreach ($diff->getAddedIndexes() as $index) {
            $sql[] = $this->getCreateIndexSQL($index, $tableName);
        }

        // Dropped indexes
        foreach ($diff->getDroppedIndexes() as $index) {
            $sql[] = $this->getDropIndexSQL($index->getName());
        }

        // Added foreign keys
        foreach ($diff->getAddedForeignKeys() as $fk) {
            $localColumns = array_map(
                fn(string $col): string => $this->quoteSingleIdentifier($col),
                $fk->getLocalColumns(),
            );
            $foreignColumns = array_map(
                fn(string $col): string => $this->quoteSingleIdentifier($col),
                $fk->getForeignColumns(),
            );

            $sql[] = sprintf(
                'ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)',
                $tableName,
                $this->quoteSingleIdentifier($fk->getName()),
                implode(', ', $localColumns),
                $this->quoteSingleIdentifier($fk->getForeignTableName()),
                implode(', ', $foreignColumns),
            );
        }

        // Dropped foreign keys
        foreach ($diff->getDroppedForeignKeys() as $fk) {
            $fkName = is_string($fk) ? $fk : $fk->getName();
            $sql[] = sprintf(
                'ALTER TABLE %s DROP CONSTRAINT %s',
                $tableName,
                $this->quoteSingleIdentifier($fkName),
            );
        }

        return $sql;
    }

    // ── Transaction isolation ───────────────────────────────────────────

    /**
     * {@inheritDoc}
     */
    public function getSetTransactionIsolationSQL(TransactionIsolationLevel $level): string
    {
        return 'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL '
            . $this->getTransactionIsolationLevelSQL($level);
    }

    private function getTransactionIsolationLevelSQL(TransactionIsolationLevel $level): string
    {
        return match ($level) {
            TransactionIsolationLevel::READ_UNCOMMITTED  => 'READ UNCOMMITTED',
            TransactionIsolationLevel::READ_COMMITTED    => 'READ COMMITTED',
            TransactionIsolationLevel::REPEATABLE_READ   => 'REPEATABLE READ',
            TransactionIsolationLevel::SERIALIZABLE      => 'SERIALIZABLE',
        };
    }

    // ── Sequences ───────────────────────────────────────────────────────

    /**
     * {@inheritDoc}
     */
    public function supportsSequences(): bool
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsIdentityColumns(): bool
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateSequenceSQL(\Doctrine\DBAL\Schema\Sequence $sequence): string
    {
        return sprintf(
            'CREATE SEQUENCE %s INCREMENT BY %d MINVALUE %d START %d',
            $this->quoteSingleIdentifier($sequence->getName()),
            $sequence->getAllocationSize(),
            $sequence->getInitialValue(),
            $sequence->getInitialValue(),
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getDropSequenceSQL(string $name): string
    {
        return 'DROP SEQUENCE ' . $this->quoteSingleIdentifier($name);
    }

    /**
     * {@inheritDoc}
     */
    public function getSequenceNextValSQL(string $sequence): string
    {
        return "SELECT nextval('" . $sequence . "')";
    }

    // ── Misc overrides ──────────────────────────────────────────────────

    /**
     * {@inheritDoc}
     */
    public function getForUpdateSQL(): string
    {
        return 'FOR UPDATE';
    }

    /**
     * {@inheritDoc}
     */
    public function supportsSchemas(): bool
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsSavepoints(): bool
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function createSavePoint(string $savepoint): string
    {
        return 'SAVEPOINT ' . $savepoint;
    }

    /**
     * {@inheritDoc}
     */
    public function releaseSavePoint(string $savepoint): string
    {
        return 'RELEASE SAVEPOINT ' . $savepoint;
    }

    /**
     * {@inheritDoc}
     */
    public function rollbackSavePoint(string $savepoint): string
    {
        return 'ROLLBACK TO SAVEPOINT ' . $savepoint;
    }

    /**
     * {@inheritDoc}
     */
    public function getTruncateTableSQL(string $tableName, bool $cascade = false): string
    {
        $truncateSql = 'TRUNCATE ' . $this->quoteSingleIdentifier($tableName);

        if ($cascade) {
            $truncateSql .= ' CASCADE';
        }

        return $truncateSql;
    }

    // ── Internal helpers ────────────────────────────────────────────────

    /**
     * Convert a Column object to an array for type declaration methods.
     */
    private function columnToArray(Column $column): array
    {
        return [
            'type'          => $column->getType(),
            'length'        => $column->getLength(),
            'notnull'       => $column->getNotnull(),
            'default'       => $column->getDefault(),
            'precision'     => $column->getPrecision(),
            'scale'         => $column->getScale(),
            'fixed'         => $column->getFixed(),
            'autoincrement' => $column->getAutoincrement(),
            'comment'       => $column->getComment(),
        ];
    }

    /**
     * {@inheritDoc}
     */
    protected function initializeDoctrineTypeMappings(): void
    {
        $this->doctrineTypeMapping = [
            'smallint'                  => Types::SMALLINT,
            'int2'                      => Types::SMALLINT,
            'integer'                   => Types::INTEGER,
            'int'                       => Types::INTEGER,
            'int4'                      => Types::INTEGER,
            'serial'                    => Types::INTEGER,
            'bigint'                    => Types::BIGINT,
            'int8'                      => Types::BIGINT,
            'bigserial'                 => Types::BIGINT,
            'real'                      => Types::FLOAT,
            'float4'                    => Types::FLOAT,
            'double precision'          => Types::FLOAT,
            'float8'                    => Types::FLOAT,
            'numeric'                   => Types::DECIMAL,
            'decimal'                   => Types::DECIMAL,
            'boolean'                   => Types::BOOLEAN,
            'bool'                      => Types::BOOLEAN,
            'varchar'                   => Types::STRING,
            'character varying'         => Types::STRING,
            'char'                      => Types::STRING,
            'character'                 => Types::STRING,
            'text'                      => Types::TEXT,
            'uuid'                      => Types::GUID,
            'bytea'                     => Types::BLOB,
            'date'                      => Types::DATE_MUTABLE,
            'timestamp'                 => Types::DATETIME_MUTABLE,
            'timestamp without time zone' => Types::DATETIME_MUTABLE,
            'timestamp with time zone'  => Types::DATETIMETZ_MUTABLE,
            'timestamptz'               => Types::DATETIMETZ_MUTABLE,
            'time'                      => Types::TIME_MUTABLE,
            'time without time zone'    => Types::TIME_MUTABLE,
            'time with time zone'       => Types::TIME_MUTABLE,
            'json'                      => Types::JSON,
            'jsonb'                     => Types::JSON,
        ];
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCommonIntegerTypeDeclarationSQL(array $column): string
    {
        $autoinc = isset($column['autoincrement']) && $column['autoincrement'];

        if ($autoinc) {
            return '';
        }

        return '';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateDiffExpression(string $date1, string $date2): string
    {
        return sprintf('((%s)::DATE - (%s)::DATE)', $date1, $date2);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateArithmeticIntervalExpression(
        string $date,
        string $operator,
        string $interval,
        DateIntervalUnit $unit,
    ): string {
        $intervalStr = match ($unit) {
            DateIntervalUnit::SECOND  => $interval . " || ' seconds'",
            DateIntervalUnit::MINUTE  => $interval . " || ' minutes'",
            DateIntervalUnit::HOUR    => $interval . " || ' hours'",
            DateIntervalUnit::DAY     => $interval . " || ' days'",
            DateIntervalUnit::WEEK    => '(' . $interval . " * 7) || ' days'",
            DateIntervalUnit::MONTH   => $interval . " || ' months'",
            DateIntervalUnit::QUARTER => '(' . $interval . " * 3) || ' months'",
            DateIntervalUnit::YEAR    => $interval . " || ' years'",
        };

        return sprintf('(%s %s (%s)::INTERVAL)', $date, $operator, $intervalStr);
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentDatabaseExpression(): string
    {
        return 'CURRENT_DATABASE()';
    }

    /**
     * {@inheritDoc}
     */
    public function getListViewsSQL(string $database): string
    {
        return "SELECT table_name AS viewname, view_definition AS definition "
            . "FROM information_schema.views "
            . "WHERE table_schema = 'public'";
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTypeDeclarationSQL(array $column): string
    {
        return 'TIMESTAMP';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTypeDeclarationSQL(array $column): string
    {
        return 'DATE';
    }

    /**
     * {@inheritDoc}
     */
    public function getTimeTypeDeclarationSQL(array $column): string
    {
        return 'TIME';
    }

    /**
     * {@inheritDoc}
     */
    public function createSchemaManager(\Doctrine\DBAL\Connection $connection): \Doctrine\DBAL\Schema\AbstractSchemaManager
    {
        return new SchemaManager($connection, $this);
    }

    /**
     * {@inheritDoc}
     */
    protected function createReservedKeywordsList(): \Doctrine\DBAL\Platforms\Keywords\KeywordList
    {
        return new Keywords\PyroSQLKeywords();
    }
}

<?php

declare(strict_types=1);

namespace PyroSQL\DoctrineDBAL\Keywords;

use Doctrine\DBAL\Platforms\Keywords\KeywordList;

/**
 * Reserved keywords for the PyroSQL SQL dialect.
 *
 * Based on the PostgreSQL reserved word list with additions for
 * PyroSQL-specific keywords (WATCH, UNWATCH, LISTEN, UNLISTEN,
 * NOTIFY, SUBSCRIBE, CDC).
 */
final class PyroSQLKeywords extends KeywordList
{
    /**
     * {@inheritDoc}
     */
    public function getName(): string
    {
        return 'PyroSQL';
    }

    /**
     * {@inheritDoc}
     */
    protected function getKeywords(): array
    {
        return [
            // SQL standard reserved words used by PyroSQL
            'ALL',
            'ALTER',
            'AND',
            'ANY',
            'AS',
            'ASC',
            'BEGIN',
            'BETWEEN',
            'BIGINT',
            'BIGSERIAL',
            'BOOLEAN',
            'BY',
            'CASCADE',
            'CASE',
            'CHAR',
            'CHARACTER',
            'CHECK',
            'COLUMN',
            'COMMIT',
            'CONSTRAINT',
            'CREATE',
            'CROSS',
            'CURRENT_DATE',
            'CURRENT_TIME',
            'CURRENT_TIMESTAMP',
            'DATABASE',
            'DATE',
            'DECIMAL',
            'DEFAULT',
            'DELETE',
            'DESC',
            'DISTINCT',
            'DO',
            'DROP',
            'ELSE',
            'END',
            'EXCEPT',
            'EXISTS',
            'FALSE',
            'FLOAT',
            'FOR',
            'FOREIGN',
            'FROM',
            'FULL',
            'GRANT',
            'GROUP',
            'HAVING',
            'IF',
            'IN',
            'INDEX',
            'INNER',
            'INSERT',
            'INT',
            'INTEGER',
            'INTERSECT',
            'INTO',
            'IS',
            'JOIN',
            'KEY',
            'LEFT',
            'LIKE',
            'LIMIT',
            'NATURAL',
            'NOT',
            'NULL',
            'NUMERIC',
            'OFFSET',
            'ON',
            'OR',
            'ORDER',
            'OUTER',
            'PRIMARY',
            'REAL',
            'REFERENCES',
            'RETURNING',
            'REVOKE',
            'RIGHT',
            'ROLLBACK',
            'SAVEPOINT',
            'SCHEMA',
            'SELECT',
            'SEQUENCE',
            'SERIAL',
            'SET',
            'SMALLINT',
            'TABLE',
            'TEXT',
            'THEN',
            'TIMESTAMP',
            'TIMESTAMPTZ',
            'TO',
            'TRANSACTION',
            'TRUE',
            'TRUNCATE',
            'UNION',
            'UNIQUE',
            'UPDATE',
            'USING',
            'UUID',
            'VALUES',
            'VARCHAR',
            'VARYING',
            'VIEW',
            'WHEN',
            'WHERE',
            'WITH',

            // PyroSQL-specific keywords
            'WATCH',
            'UNWATCH',
            'LISTEN',
            'UNLISTEN',
            'NOTIFY',
            'SUBSCRIBE',
            'CDC',
            'COPY',
            'BYTEA',
            'JSONB',
            'JSON',
        ];
    }
}

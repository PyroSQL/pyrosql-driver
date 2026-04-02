<?php

declare(strict_types=1);

namespace PyroSQL\DoctrineDBAL;

use Doctrine\DBAL\Driver\API\ExceptionConverter as ExceptionConverterInterface;
use Doctrine\DBAL\Driver\Exception;
use Doctrine\DBAL\Exception\ConnectionException;
use Doctrine\DBAL\Exception\DatabaseDoesNotExist;
use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Exception\ForeignKeyConstraintViolationException;
use Doctrine\DBAL\Exception\InvalidFieldNameException;
use Doctrine\DBAL\Exception\NonUniqueFieldNameException;
use Doctrine\DBAL\Exception\NotNullConstraintViolationException;
use Doctrine\DBAL\Exception\SyntaxErrorException;
use Doctrine\DBAL\Exception\TableExistsException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Query;

/**
 * Converts PyroSQL/PDO exceptions into typed Doctrine DBAL exceptions.
 *
 * SQLSTATE codes follow the PostgreSQL convention since PyroSQL uses
 * the same error code scheme.
 */
final class ExceptionConverter implements ExceptionConverterInterface
{
    /**
     * {@inheritDoc}
     */
    public function convert(Exception $exception, ?Query $query): DriverException
    {
        $sqlState = $exception->getSQLState();

        // Match by SQLSTATE class (first two characters)
        return match (true) {
            // Connection errors: 08xxx
            $sqlState !== null && str_starts_with($sqlState, '08')
                => new ConnectionException($exception, $query),

            // Syntax error: 42601
            $sqlState === '42601'
                => new SyntaxErrorException($exception, $query),

            // Undefined table: 42P01
            $sqlState === '42P01'
                => new TableNotFoundException($exception, $query),

            // Duplicate table: 42P07
            $sqlState === '42P07'
                => new TableExistsException($exception, $query),

            // Undefined column: 42703
            $sqlState === '42703'
                => new InvalidFieldNameException($exception, $query),

            // Ambiguous column: 42702
            $sqlState === '42702'
                => new NonUniqueFieldNameException($exception, $query),

            // Unique violation: 23505
            $sqlState === '23505'
                => new UniqueConstraintViolationException($exception, $query),

            // Not-null violation: 23502
            $sqlState === '23502'
                => new NotNullConstraintViolationException($exception, $query),

            // Foreign key violation: 23503
            $sqlState === '23503'
                => new ForeignKeyConstraintViolationException($exception, $query),

            // Invalid catalog name (database does not exist): 3D000
            $sqlState === '3D000'
                => new DatabaseDoesNotExist($exception, $query),

            // Default: generic driver exception
            default => new DriverException($exception, $query),
        };
    }
}

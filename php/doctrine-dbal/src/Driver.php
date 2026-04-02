<?php

declare(strict_types=1);

namespace PyroSQL\DoctrineDBAL;

use Doctrine\DBAL\Connection\StaticServerVersionProvider;
use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use SensitiveParameter;

/**
 * Doctrine DBAL driver for PyroSQL.
 *
 * Uses the pdo_pyrosql PHP extension as the underlying PDO connection layer,
 * similar to how pdo_pgsql backs the PostgreSQL DBAL driver.
 *
 * Configuration via driver_class in Symfony:
 *
 *     doctrine:
 *         dbal:
 *             driver_class: PyroSQL\DoctrineDBAL\Driver
 *             host: localhost
 *             port: 12520
 *             dbname: mydb
 *
 * Or via DSN: pyrosql://user:pass@host:12520/dbname
 */
final class Driver implements DriverInterface
{
    /**
     * {@inheritDoc}
     */
    public function connect(
        #[SensitiveParameter]
        array $params,
    ): DriverConnection {
        $host   = $params['host'] ?? '127.0.0.1';
        $port   = $params['port'] ?? 12520;
        $dbname = $params['dbname'] ?? '';
        $user   = $params['user'] ?? 'pyrosql';
        $password = $params['password'] ?? '';

        $dsn = sprintf('pyrosql:host=%s;port=%d;dbname=%s', $host, $port, $dbname);

        $driverOptions = $params['driverOptions'] ?? [];

        $pdo = new \PDO($dsn, $user, $password, $driverOptions);
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        return new Connection($pdo);
    }

    /**
     * {@inheritDoc}
     */
    public function getDatabasePlatform(
        \Doctrine\DBAL\ServerVersionProvider $versionProvider,
    ): AbstractPlatform {
        return new Platform();
    }

    /**
     * {@inheritDoc}
     */
    public function getSchemaManager(
        \Doctrine\DBAL\Connection $conn,
        AbstractPlatform $platform,
    ): AbstractSchemaManager {
        return new SchemaManager($conn, $platform);
    }

    /**
     * {@inheritDoc}
     */
    public function getExceptionConverter(): \Doctrine\DBAL\Driver\API\ExceptionConverter
    {
        return new ExceptionConverter();
    }
}

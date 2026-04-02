<?php

declare(strict_types=1);

namespace PyroSQL\Laravel;

use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use PDO;

class PyroSqlConnector extends Connector implements ConnectorInterface
{
    /**
     * The default PDO connection options.
     *
     * @var array<int, mixed>
     */
    protected $options = [
        PDO::ATTR_CASE => PDO::CASE_NATURAL,
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_ORACLE_NULLS => PDO::NULL_NATURAL,
        PDO::ATTR_STRINGIFY_FETCHES => false,
    ];

    /**
     * Establish a database connection.
     *
     * @param  array<string, mixed>  $config
     * @return PDO
     */
    public function connect(array $config): PDO
    {
        $dsn = $this->getDsn($config);
        $options = $this->getOptions($config);

        $connection = $this->createConnection($dsn, $config, $options);

        // Set the database schema search path if provided.
        if (isset($config['schema'])) {
            $schema = $config['schema'];
            $connection->exec("SET search_path TO \"{$schema}\"");
        }

        // Set the application name if provided.
        if (isset($config['application_name'])) {
            $appName = $config['application_name'];
            $connection->exec("SET application_name TO '{$appName}'");
        }

        // Set timezone if provided.
        if (isset($config['timezone'])) {
            $timezone = $config['timezone'];
            $connection->exec("SET timezone TO '{$timezone}'");
        }

        return $connection;
    }

    /**
     * Create the DSN string for the pdo_pyrosql connection.
     *
     * @param  array<string, mixed>  $config
     * @return string
     */
    protected function getDsn(array $config): string
    {
        $host = $config['host'] ?? '127.0.0.1';
        $port = $config['port'] ?? 12520;
        $database = $config['database'] ?? 'forge';

        return "pyrosql:host={$host};port={$port};dbname={$database}";
    }
}

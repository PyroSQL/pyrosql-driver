<?php

declare(strict_types=1);

namespace PyroSQL\Laravel;

use Illuminate\Database\Connection;
use Illuminate\Database\DatabaseManager;
use Illuminate\Support\ServiceProvider;

class PyroSqlServiceProvider extends ServiceProvider
{
    /**
     * Register the PyroSQL database driver with Laravel's DatabaseManager.
     */
    public function register(): void
    {
        Connection::resolverFor('pyrosql', function ($connection, $database, $prefix, $config) {
            $connector = new PyroSqlConnector();
            $pdo = $connector->connect($config);

            return new PyroSqlConnection($pdo, $database, $prefix, $config);
        });
    }

    /**
     * Boot the service provider.
     */
    public function boot(): void
    {
        //
    }
}

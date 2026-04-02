<?php

declare(strict_types=1);

namespace PyroSQL\Laravel;

use Illuminate\Database\Connection;
use Illuminate\Database\Query\Grammars\Grammar as QueryGrammar;
use Illuminate\Database\Query\Processors\Processor;
use Illuminate\Database\Schema\Grammars\Grammar as SchemaGrammar;
use PyroSQL\Laravel\Query\PyroSqlGrammar as PyroSqlQueryGrammar;
use PyroSQL\Laravel\Query\PyroSqlProcessor;
use PyroSQL\Laravel\Schema\PyroSqlBuilder;
use PyroSQL\Laravel\Schema\PyroSqlGrammar as PyroSqlSchemaGrammar;

class PyroSqlConnection extends Connection
{
    /**
     * Get the default query grammar instance.
     *
     * @return \Illuminate\Database\Query\Grammars\Grammar
     */
    protected function getDefaultQueryGrammar(): QueryGrammar
    {
        return new PyroSqlQueryGrammar($this);
    }

    /**
     * Get the default schema grammar instance.
     *
     * @return \Illuminate\Database\Schema\Grammars\Grammar
     */
    protected function getDefaultSchemaGrammar(): SchemaGrammar
    {
        return new PyroSqlSchemaGrammar($this);
    }

    /**
     * Get the default post processor instance.
     *
     * @return \Illuminate\Database\Query\Processors\Processor
     */
    protected function getDefaultPostProcessor(): Processor
    {
        return new PyroSqlProcessor();
    }

    /**
     * Get a schema builder instance for the connection.
     *
     * @return \PyroSQL\Laravel\Schema\PyroSqlBuilder
     */
    public function getSchemaBuilder(): PyroSqlBuilder
    {
        if ($this->schemaGrammar === null) {
            $this->useDefaultSchemaGrammar();
        }

        return new PyroSqlBuilder($this);
    }

    /**
     * Get the driver name.
     *
     * @return string
     */
    public function getDriverName(): string
    {
        return 'pyrosql';
    }

    /**
     * Bind values to their parameters in the given statement.
     *
     * @param  \PDOStatement  $statement
     * @param  array<int|string, mixed>  $bindings
     * @return void
     */
    public function bindValues($statement, $bindings): void
    {
        foreach ($bindings as $key => $value) {
            $pdoType = match (true) {
                is_int($value) => \PDO::PARAM_INT,
                is_bool($value) => \PDO::PARAM_BOOL,
                is_null($value) => \PDO::PARAM_NULL,
                default => \PDO::PARAM_STR,
            };

            $statement->bindValue(
                is_string($key) ? $key : $key + 1,
                $value,
                $pdoType,
            );
        }
    }

    /**
     * Run a select statement and return a single result using RETURNING clause.
     *
     * @param  string  $query
     * @param  array<int, mixed>  $bindings
     * @return mixed
     */
    public function selectOneReturning(string $query, array $bindings = []): mixed
    {
        $records = $this->select($query, $bindings);

        return array_shift($records);
    }

    /**
     * Get the server version.
     *
     * @return string
     */
    public function getServerVersion(): string
    {
        return $this->getPdo()->getAttribute(\PDO::ATTR_SERVER_VERSION);
    }
}

<?php

declare(strict_types=1);

namespace PyroSQL\DoctrineDBAL;

use Doctrine\DBAL\Driver\Connection as ConnectionInterface;
use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Driver\Statement as StatementInterface;

/**
 * Wraps a PDO connection established via the pdo_pyrosql extension.
 *
 * Implements the Doctrine DBAL Driver\Connection interface so that
 * Doctrine can issue queries, manage transactions, and prepare
 * statements through the PyroSQL PDO driver.
 */
final class Connection implements ConnectionInterface
{
    private \PDO $pdo;

    public function __construct(\PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * Return the underlying PDO instance for direct access when needed.
     */
    public function getNativeConnection(): \PDO
    {
        return $this->pdo;
    }

    /**
     * {@inheritDoc}
     */
    public function prepare(string $sql): StatementInterface
    {
        $pdoStatement = $this->pdo->prepare($sql);

        return new Statement($pdoStatement);
    }

    /**
     * {@inheritDoc}
     */
    public function query(string $sql): ResultInterface
    {
        $pdoStatement = $this->pdo->query($sql);

        return new Result($pdoStatement);
    }

    /**
     * {@inheritDoc}
     */
    public function quote(string $value): string
    {
        $quoted = $this->pdo->quote($value);

        // PDO::quote() can return false on failure; treat as empty-string quote
        if ($quoted === false) {
            return "''";
        }

        return $quoted;
    }

    /**
     * {@inheritDoc}
     */
    public function exec(string $sql): int|string
    {
        $result = $this->pdo->exec($sql);

        if ($result === false) {
            return 0;
        }

        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function lastInsertId(): int|string
    {
        $id = $this->pdo->lastInsertId();

        if ($id === false) {
            return 0;
        }

        return $id;
    }

    /**
     * {@inheritDoc}
     */
    public function beginTransaction(): void
    {
        $this->pdo->beginTransaction();
    }

    /**
     * {@inheritDoc}
     */
    public function commit(): void
    {
        $this->pdo->commit();
    }

    /**
     * {@inheritDoc}
     */
    public function rollBack(): void
    {
        $this->pdo->rollBack();
    }

    public function getServerVersion(): string
    {
        return $this->pdo->getAttribute(\PDO::ATTR_SERVER_VERSION) ?: 'PyroSQL 1.0';
    }
}

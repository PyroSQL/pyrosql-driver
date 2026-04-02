<?php

declare(strict_types=1);

namespace PyroSQL\DoctrineDBAL;

use Doctrine\DBAL\Driver\Result as ResultInterface;

/**
 * Wraps a PDOStatement result set for Doctrine DBAL.
 *
 * Provides the various fetch modes that Doctrine expects while
 * delegating to the underlying PDO result set from pdo_pyrosql.
 */
final class Result implements ResultInterface
{
    private \PDOStatement $pdoStatement;

    public function __construct(\PDOStatement $pdoStatement)
    {
        $this->pdoStatement = $pdoStatement;
    }

    /**
     * {@inheritDoc}
     */
    public function fetchNumeric(): array|false
    {
        return $this->pdoStatement->fetch(\PDO::FETCH_NUM);
    }

    /**
     * {@inheritDoc}
     */
    public function fetchAssociative(): array|false
    {
        return $this->pdoStatement->fetch(\PDO::FETCH_ASSOC);
    }

    /**
     * {@inheritDoc}
     */
    public function fetchOne(): mixed
    {
        return $this->pdoStatement->fetchColumn();
    }

    /**
     * {@inheritDoc}
     */
    public function fetchAllNumeric(): array
    {
        return $this->pdoStatement->fetchAll(\PDO::FETCH_NUM);
    }

    /**
     * {@inheritDoc}
     */
    public function fetchAllAssociative(): array
    {
        return $this->pdoStatement->fetchAll(\PDO::FETCH_ASSOC);
    }

    /**
     * {@inheritDoc}
     */
    public function fetchFirstColumn(): array
    {
        return $this->pdoStatement->fetchAll(\PDO::FETCH_COLUMN);
    }

    /**
     * {@inheritDoc}
     */
    public function rowCount(): int
    {
        return $this->pdoStatement->rowCount();
    }

    /**
     * {@inheritDoc}
     */
    public function columnCount(): int
    {
        return $this->pdoStatement->columnCount();
    }

    /**
     * {@inheritDoc}
     */
    public function free(): void
    {
        $this->pdoStatement->closeCursor();
    }
}

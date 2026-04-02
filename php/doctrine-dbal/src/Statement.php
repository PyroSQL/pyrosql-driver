<?php

declare(strict_types=1);

namespace PyroSQL\DoctrineDBAL;

use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Doctrine\DBAL\ParameterType;

/**
 * Wraps a PDOStatement for use by Doctrine DBAL.
 *
 * Translates Doctrine parameter types into the corresponding PDO
 * parameter type constants and delegates execution to the underlying
 * pdo_pyrosql statement.
 */
final class Statement implements StatementInterface
{
    private \PDOStatement $pdoStatement;

    public function __construct(\PDOStatement $pdoStatement)
    {
        $this->pdoStatement = $pdoStatement;
    }

    /**
     * {@inheritDoc}
     */
    public function bindValue(int|string $param, mixed $value, ParameterType $type = ParameterType::STRING): void
    {
        $pdoType = match ($type) {
            ParameterType::NULL            => \PDO::PARAM_NULL,
            ParameterType::INTEGER         => \PDO::PARAM_INT,
            ParameterType::STRING,
            ParameterType::ASCII           => \PDO::PARAM_STR,
            ParameterType::LARGE_OBJECT    => \PDO::PARAM_LOB,
            ParameterType::BOOLEAN         => \PDO::PARAM_BOOL,
            ParameterType::BINARY          => \PDO::PARAM_LOB,
        };

        $this->pdoStatement->bindValue($param, $value, $pdoType);
    }

    /**
     * {@inheritDoc}
     */
    public function execute(): ResultInterface
    {
        $this->pdoStatement->execute();

        return new Result($this->pdoStatement);
    }
}

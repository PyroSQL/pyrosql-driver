<?php

declare(strict_types=1);

namespace PyroSQL\DoctrineDBAL\Tests;

use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Driver\API\ExceptionConverter as ExceptionConverterInterface;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use PHPUnit\Framework\TestCase;
use PyroSQL\DoctrineDBAL\Connection;
use PyroSQL\DoctrineDBAL\Driver;
use PyroSQL\DoctrineDBAL\ExceptionConverter;
use PyroSQL\DoctrineDBAL\Platform;
use PyroSQL\DoctrineDBAL\Result;
use PyroSQL\DoctrineDBAL\SchemaManager;
use PyroSQL\DoctrineDBAL\Statement;
use Doctrine\DBAL\Platforms\TrimMode;

/**
 * Unit tests for the PyroSQL Doctrine DBAL driver.
 *
 * These tests verify interface compliance, platform SQL generation,
 * type mappings, and exception conversion without requiring a live
 * PyroSQL server.
 */
class DriverTest extends TestCase
{
    // ── Driver interface compliance ─────────────────────────────────────

    public function testDriverImplementsInterface(): void
    {
        $driver = new Driver();
        $this->assertInstanceOf(DriverInterface::class, $driver);
    }

    public function testGetDatabasePlatformReturnsPyroSQLPlatform(): void
    {
        $driver = new Driver();
        $versionProvider = $this->createMock(\Doctrine\DBAL\ServerVersionProvider::class);
        $platform = $driver->getDatabasePlatform($versionProvider);

        $this->assertInstanceOf(Platform::class, $platform);
        $this->assertInstanceOf(AbstractPlatform::class, $platform);
    }

    public function testGetSchemaManagerReturnsSchemaManager(): void
    {
        $driver = new Driver();
        $conn = $this->createMock(\Doctrine\DBAL\Connection::class);
        $platform = new Platform();

        $schemaManager = $driver->getSchemaManager($conn, $platform);

        $this->assertInstanceOf(SchemaManager::class, $schemaManager);
        $this->assertInstanceOf(AbstractSchemaManager::class, $schemaManager);
    }

    public function testGetExceptionConverterReturnsConverter(): void
    {
        $driver = new Driver();
        $converter = $driver->getExceptionConverter();

        $this->assertInstanceOf(ExceptionConverterInterface::class, $converter);
        $this->assertInstanceOf(ExceptionConverter::class, $converter);
    }

    // ── Connection wrapping ─────────────────────────────────────────────

    public function testConnectionImplementsInterface(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $conn = new Connection($pdo);

        $this->assertInstanceOf(DriverConnection::class, $conn);
    }

    public function testConnectionExposesPdo(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $conn = new Connection($pdo);

        $this->assertSame($pdo, $conn->getNativeConnection());
    }

    public function testConnectionQuote(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('quote')->with("test'value")->willReturn("'test''value'");

        $conn = new Connection($pdo);
        $this->assertSame("'test''value'", $conn->quote("test'value"));
    }

    public function testConnectionQuoteFalseReturnsEmpty(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('quote')->willReturn(false);

        $conn = new Connection($pdo);
        $this->assertSame("''", $conn->quote('anything'));
    }

    public function testConnectionExec(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('exec')->with('DELETE FROM users')->willReturn(5);

        $conn = new Connection($pdo);
        $this->assertEquals(5, $conn->exec('DELETE FROM users'));
    }

    public function testConnectionExecFalse(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('exec')->willReturn(false);

        $conn = new Connection($pdo);
        $this->assertEquals(0, $conn->exec('DELETE FROM users'));
    }

    public function testConnectionLastInsertId(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('lastInsertId')->willReturn('42');

        $conn = new Connection($pdo);
        $this->assertEquals('42', $conn->lastInsertId());
    }

    public function testConnectionLastInsertIdFalse(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('lastInsertId')->willReturn(false);

        $conn = new Connection($pdo);
        $this->assertEquals(0, $conn->lastInsertId());
    }

    public function testConnectionTransactions(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())->method('beginTransaction');
        $pdo->expects($this->once())->method('commit');

        $conn = new Connection($pdo);
        $conn->beginTransaction();
        $conn->commit();
    }

    public function testConnectionRollback(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())->method('beginTransaction');
        $pdo->expects($this->once())->method('rollBack');

        $conn = new Connection($pdo);
        $conn->beginTransaction();
        $conn->rollBack();
    }

    public function testConnectionPrepare(): void
    {
        $pdoStmt = $this->createMock(\PDOStatement::class);
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('prepare')->with('SELECT ? + ?')->willReturn($pdoStmt);

        $conn = new Connection($pdo);
        $stmt = $conn->prepare('SELECT ? + ?');

        $this->assertInstanceOf(Statement::class, $stmt);
    }

    public function testConnectionQuery(): void
    {
        $pdoStmt = $this->createMock(\PDOStatement::class);
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('query')->with('SELECT 1')->willReturn($pdoStmt);

        $conn = new Connection($pdo);
        $result = $conn->query('SELECT 1');

        $this->assertInstanceOf(Result::class, $result);
    }

    public function testConnectionServerVersion(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('getAttribute')
            ->with(\PDO::ATTR_SERVER_VERSION)
            ->willReturn('PyroSQL 1.0 (PWire)');

        $conn = new Connection($pdo);
        $this->assertSame('PyroSQL 1.0 (PWire)', $conn->getServerVersion());
    }

    // ── Statement wrapping ──────────────────────────────────────────────

    public function testStatementBindValue(): void
    {
        $pdoStmt = $this->createMock(\PDOStatement::class);
        $pdoStmt->expects($this->exactly(3))
            ->method('bindValue')
            ->willReturnCallback(function ($param, $value, $type) {
                static $call = 0;
                $call++;
                match ($call) {
                    1 => $this->assertEquals(\PDO::PARAM_INT, $type),
                    2 => $this->assertEquals(\PDO::PARAM_STR, $type),
                    3 => $this->assertEquals(\PDO::PARAM_NULL, $type),
                };
                return true;
            });

        $stmt = new Statement($pdoStmt);
        $stmt->bindValue(1, 42, \Doctrine\DBAL\ParameterType::INTEGER);
        $stmt->bindValue(2, 'hello', \Doctrine\DBAL\ParameterType::STRING);
        $stmt->bindValue(3, null, \Doctrine\DBAL\ParameterType::NULL);
    }

    public function testStatementExecuteReturnsResult(): void
    {
        $pdoStmt = $this->createMock(\PDOStatement::class);
        $pdoStmt->method('execute')->willReturn(true);

        $stmt = new Statement($pdoStmt);
        $result = $stmt->execute();

        $this->assertInstanceOf(Result::class, $result);
    }

    // ── Result wrapping ─────────────────────────────────────────────────

    public function testResultFetchNumeric(): void
    {
        $pdoStmt = $this->createMock(\PDOStatement::class);
        $pdoStmt->method('fetch')
            ->with(\PDO::FETCH_NUM)
            ->willReturn([1, 'Alice']);

        $result = new Result($pdoStmt);
        $this->assertSame([1, 'Alice'], $result->fetchNumeric());
    }

    public function testResultFetchAssociative(): void
    {
        $pdoStmt = $this->createMock(\PDOStatement::class);
        $pdoStmt->method('fetch')
            ->with(\PDO::FETCH_ASSOC)
            ->willReturn(['id' => 1, 'name' => 'Alice']);

        $result = new Result($pdoStmt);
        $this->assertSame(['id' => 1, 'name' => 'Alice'], $result->fetchAssociative());
    }

    public function testResultFetchOne(): void
    {
        $pdoStmt = $this->createMock(\PDOStatement::class);
        $pdoStmt->method('fetchColumn')->willReturn(42);

        $result = new Result($pdoStmt);
        $this->assertSame(42, $result->fetchOne());
    }

    public function testResultFetchAllNumeric(): void
    {
        $pdoStmt = $this->createMock(\PDOStatement::class);
        $pdoStmt->method('fetchAll')
            ->with(\PDO::FETCH_NUM)
            ->willReturn([[1, 'A'], [2, 'B']]);

        $result = new Result($pdoStmt);
        $this->assertSame([[1, 'A'], [2, 'B']], $result->fetchAllNumeric());
    }

    public function testResultFetchAllAssociative(): void
    {
        $pdoStmt = $this->createMock(\PDOStatement::class);
        $pdoStmt->method('fetchAll')
            ->with(\PDO::FETCH_ASSOC)
            ->willReturn([['id' => 1], ['id' => 2]]);

        $result = new Result($pdoStmt);
        $this->assertSame([['id' => 1], ['id' => 2]], $result->fetchAllAssociative());
    }

    public function testResultFetchFirstColumn(): void
    {
        $pdoStmt = $this->createMock(\PDOStatement::class);
        $pdoStmt->method('fetchAll')
            ->with(\PDO::FETCH_COLUMN)
            ->willReturn([1, 2, 3]);

        $result = new Result($pdoStmt);
        $this->assertSame([1, 2, 3], $result->fetchFirstColumn());
    }

    public function testResultRowCount(): void
    {
        $pdoStmt = $this->createMock(\PDOStatement::class);
        $pdoStmt->method('rowCount')->willReturn(5);

        $result = new Result($pdoStmt);
        $this->assertSame(5, $result->rowCount());
    }

    public function testResultColumnCount(): void
    {
        $pdoStmt = $this->createMock(\PDOStatement::class);
        $pdoStmt->method('columnCount')->willReturn(3);

        $result = new Result($pdoStmt);
        $this->assertSame(3, $result->columnCount());
    }

    public function testResultFree(): void
    {
        $pdoStmt = $this->createMock(\PDOStatement::class);
        $pdoStmt->expects($this->once())->method('closeCursor');

        $result = new Result($pdoStmt);
        $result->free();
    }

    // ── Platform: identifier quoting ────────────────────────────────────

    public function testQuoteIdentifier(): void
    {
        $platform = new Platform();
        $this->assertSame('"users"', $platform->quoteSingleIdentifier('users'));
    }

    public function testQuoteIdentifierWithDots(): void
    {
        $platform = new Platform();
        $this->assertSame('"public"."users"', $platform->quoteIdentifier('public.users'));
    }

    public function testQuoteIdentifierEscapesDoubleQuotes(): void
    {
        $platform = new Platform();
        $this->assertSame('"col""name"', $platform->quoteSingleIdentifier('col"name'));
    }

    // ── Platform: boolean handling ──────────────────────────────────────

    public function testBooleanToDatabaseValue(): void
    {
        $platform = new Platform();
        $this->assertSame('true', $platform->convertBooleansToDatabaseValue(true));
        $this->assertSame('false', $platform->convertBooleansToDatabaseValue(false));
        $this->assertNull($platform->convertBooleansToDatabaseValue(null));
    }

    public function testBooleanFromDatabase(): void
    {
        $platform = new Platform();
        $this->assertTrue($platform->convertFromBoolean('t'));
        $this->assertTrue($platform->convertFromBoolean('true'));
        $this->assertTrue($platform->convertFromBoolean('1'));
        $this->assertFalse($platform->convertFromBoolean('f'));
        $this->assertFalse($platform->convertFromBoolean('false'));
        $this->assertFalse($platform->convertFromBoolean('0'));
        $this->assertNull($platform->convertFromBoolean(null));
        $this->assertTrue($platform->convertFromBoolean(true));
        $this->assertFalse($platform->convertFromBoolean(false));
    }

    // ── Platform: SQL expressions ───────────────────────────────────────

    public function testCurrentTimestamp(): void
    {
        $platform = new Platform();
        $this->assertSame('NOW()', $platform->getCurrentTimestampSQL());
    }

    public function testCurrentDate(): void
    {
        $platform = new Platform();
        $this->assertSame('CURRENT_DATE', $platform->getCurrentDateSQL());
    }

    public function testCurrentTime(): void
    {
        $platform = new Platform();
        $this->assertSame('CURRENT_TIME', $platform->getCurrentTimeSQL());
    }

    public function testConcatExpression(): void
    {
        $platform = new Platform();
        $this->assertSame('a || b || c', $platform->getConcatExpression('a', 'b', 'c'));
    }

    public function testLengthExpression(): void
    {
        $platform = new Platform();
        $this->assertSame('LENGTH(name)', $platform->getLengthExpression('name'));
    }

    public function testSubstringExpression(): void
    {
        $platform = new Platform();
        $this->assertSame('SUBSTRING(col FROM 1 FOR 5)', $platform->getSubstringExpression('col', '1', '5'));
        $this->assertSame('SUBSTRING(col FROM 2)', $platform->getSubstringExpression('col', '2'));
    }

    public function testLocateExpression(): void
    {
        $platform = new Platform();
        $this->assertSame("POSITION('x' IN col)", $platform->getLocateExpression('col', "'x'"));
        $this->assertSame("POSITION('x' IN SUBSTRING(col FROM 3))", $platform->getLocateExpression('col', "'x'", '3'));
    }

    public function testTrimExpression(): void
    {
        $platform = new Platform();
        $this->assertSame('TRIM(BOTH FROM col)', $platform->getTrimExpression('col'));
        $this->assertSame("TRIM(LEADING 'x' FROM col)", $platform->getTrimExpression('col', TrimMode::LEADING, "'x'"));
        $this->assertSame("TRIM(TRAILING 'x' FROM col)", $platform->getTrimExpression('col', TrimMode::TRAILING, "'x'"));
    }

    // ── Platform: type declarations ─────────────────────────────────────

    public function testIntegerTypeDeclaration(): void
    {
        $platform = new Platform();
        $this->assertSame('INTEGER', $platform->getIntegerTypeDeclarationSQL([]));
    }

    public function testIntegerAutoincrement(): void
    {
        $platform = new Platform();
        $this->assertSame('SERIAL', $platform->getIntegerTypeDeclarationSQL(['autoincrement' => true]));
    }

    public function testBigIntTypeDeclaration(): void
    {
        $platform = new Platform();
        $this->assertSame('BIGINT', $platform->getBigIntTypeDeclarationSQL([]));
        $this->assertSame('BIGSERIAL', $platform->getBigIntTypeDeclarationSQL(['autoincrement' => true]));
    }

    public function testSmallIntTypeDeclaration(): void
    {
        $platform = new Platform();
        $this->assertSame('SMALLINT', $platform->getSmallIntTypeDeclarationSQL([]));
    }

    public function testBooleanTypeDeclaration(): void
    {
        $platform = new Platform();
        $this->assertSame('BOOLEAN', $platform->getBooleanTypeDeclarationSQL([]));
    }

    public function testClobTypeDeclaration(): void
    {
        $platform = new Platform();
        $this->assertSame('TEXT', $platform->getClobTypeDeclarationSQL([]));
    }

    public function testBlobTypeDeclaration(): void
    {
        $platform = new Platform();
        $this->assertSame('BYTEA', $platform->getBlobTypeDeclarationSQL([]));
    }

    public function testVarcharTypeDeclaration(): void
    {
        $platform = new Platform();
        $this->assertSame('VARCHAR(100)', $platform->getVarcharTypeDeclarationSQL(['length' => 100]));
        $this->assertSame('VARCHAR(255)', $platform->getVarcharTypeDeclarationSQL([]));
    }

    public function testCharTypeDeclaration(): void
    {
        $platform = new Platform();
        $this->assertSame('CHAR(10)', $platform->getVarcharTypeDeclarationSQL(['fixed' => true, 'length' => 10]));
    }

    public function testDecimalTypeDeclaration(): void
    {
        $platform = new Platform();
        $this->assertSame('NUMERIC(8, 2)', $platform->getDecimalTypeDeclarationSQL(['precision' => 8, 'scale' => 2]));
        $this->assertSame('NUMERIC(10, 0)', $platform->getDecimalTypeDeclarationSQL([]));
    }

    // ── Platform: DDL generation ────────────────────────────────────────

    public function testDropTable(): void
    {
        $platform = new Platform();
        $this->assertSame('DROP TABLE "users"', $platform->getDropTableSQL('users'));
    }

    public function testDropIndex(): void
    {
        $platform = new Platform();
        $this->assertSame('DROP INDEX "idx_user_email"', $platform->getDropIndexSQL('idx_user_email', 'users'));
    }

    public function testTruncateTable(): void
    {
        $platform = new Platform();
        $this->assertSame('TRUNCATE "users"', $platform->getTruncateTableSQL('users'));
        $this->assertSame('TRUNCATE "users" CASCADE', $platform->getTruncateTableSQL('users', true));
    }

    // ── Platform: sequences ─────────────────────────────────────────────

    public function testSupportsSequences(): void
    {
        $platform = new Platform();
        $this->assertTrue($platform->supportsSequences());
    }

    public function testSupportsIdentityColumns(): void
    {
        $platform = new Platform();
        $this->assertTrue($platform->supportsIdentityColumns());
    }

    public function testSequenceNextVal(): void
    {
        $platform = new Platform();
        $this->assertSame("SELECT nextval('users_id_seq')", $platform->getSequenceNextValSQL('users_id_seq'));
    }

    public function testDropSequence(): void
    {
        $platform = new Platform();
        $this->assertSame('DROP SEQUENCE "users_id_seq"', $platform->getDropSequenceSQL('users_id_seq'));
    }

    // ── Platform: savepoints ────────────────────────────────────────────

    public function testSavepoints(): void
    {
        $platform = new Platform();
        $this->assertTrue($platform->supportsSavepoints());
        $this->assertSame('SAVEPOINT sp1', $platform->createSavePoint('sp1'));
        $this->assertSame('RELEASE SAVEPOINT sp1', $platform->releaseSavePoint('sp1'));
        $this->assertSame('ROLLBACK TO SAVEPOINT sp1', $platform->rollbackSavePoint('sp1'));
    }

    // ── Platform: misc ──────────────────────────────────────────────────

    public function testForUpdateSQL(): void
    {
        $platform = new Platform();
        $this->assertSame('FOR UPDATE', $platform->getForUpdateSQL());
    }

    public function testSupportsSchemas(): void
    {
        $platform = new Platform();
        $this->assertTrue($platform->supportsSchemas());
    }

    // ── Platform: type mappings ─────────────────────────────────────────

    public function testDoctrineTypeMappings(): void
    {
        $platform = new Platform();

        $mappings = [
            'integer'   => 'integer',
            'int4'      => 'integer',
            'serial'    => 'integer',
            'bigint'    => 'bigint',
            'bigserial' => 'bigint',
            'smallint'  => 'smallint',
            'boolean'   => 'boolean',
            'bool'      => 'boolean',
            'text'      => 'text',
            'varchar'   => 'string',
            'character varying' => 'string',
            'uuid'      => 'guid',
            'bytea'     => 'blob',
            'date'      => 'date',
            'timestamp' => 'datetime',
            'timestamp with time zone' => 'datetimetz',
            'json'      => 'json',
            'jsonb'     => 'json',
            'real'      => 'float',
            'numeric'   => 'decimal',
        ];

        foreach ($mappings as $dbType => $doctrineType) {
            $this->assertSame(
                $doctrineType,
                $platform->getDoctrineTypeMapping($dbType),
                "Failed mapping for database type: $dbType",
            );
        }
    }

    // ── Platform: information schema queries ────────────────────────────

    public function testListTablesSQL(): void
    {
        $platform = new Platform();
        $sql = $platform->getListTablesSQL();
        $this->assertStringContainsString('information_schema.tables', $sql);
        $this->assertStringContainsString("table_type = 'BASE TABLE'", $sql);
    }

    // ── Exception converter ─────────────────────────────────────────────

    public function testExceptionConverterSyntaxError(): void
    {
        $converter = new ExceptionConverter();
        $exception = $this->createDriverException('42601', 'syntax error');

        $result = $converter->convert($exception, null);
        $this->assertInstanceOf(\Doctrine\DBAL\Exception\SyntaxErrorException::class, $result);
    }

    public function testExceptionConverterTableNotFound(): void
    {
        $converter = new ExceptionConverter();
        $exception = $this->createDriverException('42P01', 'table not found');

        $result = $converter->convert($exception, null);
        $this->assertInstanceOf(\Doctrine\DBAL\Exception\TableNotFoundException::class, $result);
    }

    public function testExceptionConverterTableExists(): void
    {
        $converter = new ExceptionConverter();
        $exception = $this->createDriverException('42P07', 'table already exists');

        $result = $converter->convert($exception, null);
        $this->assertInstanceOf(\Doctrine\DBAL\Exception\TableExistsException::class, $result);
    }

    public function testExceptionConverterUniqueViolation(): void
    {
        $converter = new ExceptionConverter();
        $exception = $this->createDriverException('23505', 'unique violation');

        $result = $converter->convert($exception, null);
        $this->assertInstanceOf(\Doctrine\DBAL\Exception\UniqueConstraintViolationException::class, $result);
    }

    public function testExceptionConverterNotNull(): void
    {
        $converter = new ExceptionConverter();
        $exception = $this->createDriverException('23502', 'not null violation');

        $result = $converter->convert($exception, null);
        $this->assertInstanceOf(\Doctrine\DBAL\Exception\NotNullConstraintViolationException::class, $result);
    }

    public function testExceptionConverterForeignKey(): void
    {
        $converter = new ExceptionConverter();
        $exception = $this->createDriverException('23503', 'foreign key violation');

        $result = $converter->convert($exception, null);
        $this->assertInstanceOf(\Doctrine\DBAL\Exception\ForeignKeyConstraintViolationException::class, $result);
    }

    public function testExceptionConverterConnection(): void
    {
        $converter = new ExceptionConverter();
        $exception = $this->createDriverException('08006', 'connection failure');

        $result = $converter->convert($exception, null);
        $this->assertInstanceOf(\Doctrine\DBAL\Exception\ConnectionException::class, $result);
    }

    public function testExceptionConverterInvalidColumn(): void
    {
        $converter = new ExceptionConverter();
        $exception = $this->createDriverException('42703', 'column not found');

        $result = $converter->convert($exception, null);
        $this->assertInstanceOf(\Doctrine\DBAL\Exception\InvalidFieldNameException::class, $result);
    }

    public function testExceptionConverterAmbiguousColumn(): void
    {
        $converter = new ExceptionConverter();
        $exception = $this->createDriverException('42702', 'ambiguous column');

        $result = $converter->convert($exception, null);
        $this->assertInstanceOf(\Doctrine\DBAL\Exception\NonUniqueFieldNameException::class, $result);
    }

    public function testExceptionConverterDatabaseNotExist(): void
    {
        $converter = new ExceptionConverter();
        $exception = $this->createDriverException('3D000', 'database does not exist');

        $result = $converter->convert($exception, null);
        $this->assertInstanceOf(\Doctrine\DBAL\Exception\DatabaseDoesNotExist::class, $result);
    }

    public function testExceptionConverterGeneric(): void
    {
        $converter = new ExceptionConverter();
        $exception = $this->createDriverException('HY000', 'something went wrong');

        $result = $converter->convert($exception, null);
        $this->assertInstanceOf(\Doctrine\DBAL\Exception\DriverException::class, $result);
    }

    // ── Keywords ────────────────────────────────────────────────────────

    public function testKeywordListContainsPyroSQLKeywords(): void
    {
        $keywords = new \PyroSQL\DoctrineDBAL\Keywords\PyroSQLKeywords();
        $this->assertSame('PyroSQL', $keywords->getName());
        $this->assertTrue($keywords->isKeyword('WATCH'));
        $this->assertTrue($keywords->isKeyword('LISTEN'));
        $this->assertTrue($keywords->isKeyword('CDC'));
        $this->assertTrue($keywords->isKeyword('SELECT'));
        $this->assertTrue($keywords->isKeyword('JSONB'));
        $this->assertFalse($keywords->isKeyword('notakeyword'));
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private function createDriverException(string $sqlState, string $message): \Doctrine\DBAL\Driver\Exception
    {
        return new class($message, $sqlState) extends \Exception implements \Doctrine\DBAL\Driver\Exception {
            private string $sqlState;

            public function __construct(string $message, string $sqlState)
            {
                parent::__construct($message);
                $this->sqlState = $sqlState;
            }

            public function getSQLState(): ?string
            {
                return $this->sqlState;
            }
        };
    }
}

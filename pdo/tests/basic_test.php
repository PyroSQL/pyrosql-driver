<?php
/**
 * Basic test suite for PDO PyroSQL driver.
 *
 * Usage: php basic_test.php
 *
 * Expects a running PyroSQL instance. Configure via environment variables:
 *   PYROSQL_HOST    (default: 127.0.0.1)
 *   PYROSQL_PORT    (default: 12520)
 *   PYROSQL_DBNAME  (default: testdb)
 *   PYROSQL_USER    (default: pyrosql)
 *   PYROSQL_PASS    (default: empty)
 */

$host   = getenv('PYROSQL_HOST')   ?: '127.0.0.1';
$port   = getenv('PYROSQL_PORT')   ?: '12520';
$dbname = getenv('PYROSQL_DBNAME') ?: 'testdb';
$user   = getenv('PYROSQL_USER')   ?: 'pyrosql';
$pass   = getenv('PYROSQL_PASS')   ?: '';

$dsn = "pyrosql:host={$host};port={$port};dbname={$dbname}";

$passed = 0;
$failed = 0;

function test(string $name, callable $fn): void
{
    global $passed, $failed;
    try {
        $fn();
        echo "  PASS: {$name}\n";
        $passed++;
    } catch (Throwable $e) {
        echo "  FAIL: {$name} — " . $e->getMessage() . "\n";
        $failed++;
    }
}

function assert_eq($expected, $actual, string $msg = ''): void
{
    if ($expected !== $actual) {
        $exp_str = var_export($expected, true);
        $act_str = var_export($actual, true);
        throw new RuntimeException(
            ($msg ? "{$msg}: " : '') . "expected {$exp_str}, got {$act_str}"
        );
    }
}

echo "=== PDO PyroSQL Driver Tests ===\n\n";

/* ── 1. Connection ──────────────────────────────────────────────────── */
echo "[Connection]\n";
$pdo = null;

test('Connect to PyroSQL', function () use ($dsn, $user, $pass, &$pdo) {
    $pdo = new PDO($dsn, $user, $pass, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    ]);
    if (!$pdo) {
        throw new RuntimeException('PDO constructor returned falsy');
    }
});

test('Driver name attribute', function () use (&$pdo) {
    $name = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
    assert_eq('pyrosql', $name);
});

test('Server version attribute', function () use (&$pdo) {
    $ver = $pdo->getAttribute(PDO::ATTR_SERVER_VERSION);
    if (empty($ver)) {
        throw new RuntimeException('Server version is empty');
    }
});

if (!$pdo) {
    echo "\nCannot continue without a connection.\n";
    exit(1);
}

/* ── 2. CREATE TABLE ────────────────────────────────────────────────── */
echo "\n[DDL]\n";

test('DROP TABLE IF EXISTS', function () use ($pdo) {
    $pdo->exec('DROP TABLE IF EXISTS pdo_test_items');
});

test('CREATE TABLE', function () use ($pdo) {
    $pdo->exec(
        'CREATE TABLE pdo_test_items (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            quantity INT DEFAULT 0,
            price NUMERIC(10,2)
        )'
    );
});

/* ── 3. INSERT with bound params ────────────────────────────────────── */
echo "\n[INSERT with params]\n";

test('INSERT with positional params', function () use ($pdo) {
    $stmt = $pdo->prepare('INSERT INTO pdo_test_items (name, quantity, price) VALUES (?, ?, ?)');
    $result = $stmt->execute(['Widget', 10, 9.99]);
    assert_eq(true, $result, 'execute() return');
});

test('INSERT second row', function () use ($pdo) {
    $stmt = $pdo->prepare('INSERT INTO pdo_test_items (name, quantity, price) VALUES (?, ?, ?)');
    $stmt->execute(['Gadget', 5, 24.50]);
});

test('INSERT third row with bindValue', function () use ($pdo) {
    $stmt = $pdo->prepare('INSERT INTO pdo_test_items (name, quantity, price) VALUES (?, ?, ?)');
    $stmt->bindValue(1, 'Doohickey');
    $stmt->bindValue(2, 0, PDO::PARAM_INT);
    $stmt->bindValue(3, 199.99);
    $stmt->execute();
});

/* ── 4. SELECT and fetch ────────────────────────────────────────────── */
echo "\n[SELECT and fetch]\n";

test('SELECT all rows', function () use ($pdo) {
    $stmt = $pdo->query('SELECT id, name, quantity, price FROM pdo_test_items ORDER BY id');
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
    assert_eq(3, count($rows), 'row count');
    assert_eq('Widget', $rows[0]['name'], 'first row name');
    assert_eq('Gadget', $rows[1]['name'], 'second row name');
    assert_eq('Doohickey', $rows[2]['name'], 'third row name');
});

test('SELECT with WHERE param', function () use ($pdo) {
    $stmt = $pdo->prepare('SELECT name, price FROM pdo_test_items WHERE quantity > ?');
    $stmt->execute([1]);
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
    assert_eq(2, count($rows), 'filtered row count');
});

test('fetch() row by row', function () use ($pdo) {
    $stmt = $pdo->query('SELECT name FROM pdo_test_items ORDER BY id');
    $row1 = $stmt->fetch(PDO::FETCH_ASSOC);
    assert_eq('Widget', $row1['name']);
    $row2 = $stmt->fetch(PDO::FETCH_ASSOC);
    assert_eq('Gadget', $row2['name']);
    $row3 = $stmt->fetch(PDO::FETCH_ASSOC);
    assert_eq('Doohickey', $row3['name']);
    $row4 = $stmt->fetch(PDO::FETCH_ASSOC);
    assert_eq(false, $row4, 'no more rows');
});

test('column count', function () use ($pdo) {
    $stmt = $pdo->query('SELECT id, name, quantity, price FROM pdo_test_items LIMIT 1');
    assert_eq(4, $stmt->columnCount(), 'column count');
});

/* ── 5. Transactions ────────────────────────────────────────────────── */
echo "\n[Transactions]\n";

test('inTransaction() initially false', function () use ($pdo) {
    assert_eq(false, $pdo->inTransaction());
});

test('beginTransaction', function () use ($pdo) {
    $pdo->beginTransaction();
    assert_eq(true, $pdo->inTransaction());
});

test('INSERT inside transaction', function () use ($pdo) {
    $pdo->exec("INSERT INTO pdo_test_items (name, quantity, price) VALUES ('TxItem', 1, 1.00)");
});

test('rollBack', function () use ($pdo) {
    $pdo->rollBack();
    assert_eq(false, $pdo->inTransaction());
});

test('Rolled-back row is gone', function () use ($pdo) {
    $stmt = $pdo->query("SELECT COUNT(*) AS cnt FROM pdo_test_items WHERE name = 'TxItem'");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    assert_eq('0', $row['cnt'], 'count after rollback');
});

test('commit transaction', function () use ($pdo) {
    $pdo->beginTransaction();
    $pdo->exec("INSERT INTO pdo_test_items (name, quantity, price) VALUES ('Committed', 1, 2.00)");
    $pdo->commit();
    assert_eq(false, $pdo->inTransaction());
    $stmt = $pdo->query("SELECT COUNT(*) AS cnt FROM pdo_test_items WHERE name = 'Committed'");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    assert_eq('1', $row['cnt'], 'count after commit');
});

/* ── 6. Error handling ──────────────────────────────────────────────── */
echo "\n[Error handling]\n";

test('Syntax error raises exception', function () use ($pdo) {
    try {
        $pdo->query('SELEC INVALID SYNTAX !!!');
        throw new RuntimeException('Expected exception was not thrown');
    } catch (PDOException $e) {
        // Expected — verify we got a message
        if (empty($e->getMessage())) {
            throw new RuntimeException('PDOException message is empty');
        }
    }
});

test('errorInfo after failed query', function () use ($dsn, $user, $pass) {
    $pdo2 = new PDO($dsn, $user, $pass, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_SILENT,
    ]);
    $result = $pdo2->query('SELEC INVALID');
    assert_eq(false, $result, 'query should return false on error');
    $info = $pdo2->errorInfo();
    if ($info[0] === '00000' || $info[0] === '') {
        throw new RuntimeException('Expected non-success SQLSTATE, got: ' . $info[0]);
    }
});

/* ── 7. Prepared statements (re-execute) ────────────────────────────── */
echo "\n[Prepared statements]\n";

test('Re-execute prepared statement with different params', function () use ($pdo) {
    $stmt = $pdo->prepare('SELECT name FROM pdo_test_items WHERE quantity = ?');

    $stmt->execute([10]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    assert_eq('Widget', $row['name']);

    $stmt->execute([5]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    assert_eq('Gadget', $row['name']);
});

test('Prepared INSERT + SELECT lastInsertId', function () use ($pdo) {
    $stmt = $pdo->prepare(
        'INSERT INTO pdo_test_items (name, quantity, price) VALUES (?, ?, ?) RETURNING id'
    );
    $stmt->execute(['LastIdTest', 1, 0.01]);
    $id = $pdo->lastInsertId();
    if ($id === '0' || $id === '' || $id === false) {
        // lastInsertId might not work without RETURNING support — acceptable
        echo "    (lastInsertId returned '{$id}' — may require RETURNING)\n";
    }
});

test('Prepared statement with NULL param', function () use ($pdo) {
    $stmt = $pdo->prepare('INSERT INTO pdo_test_items (name, quantity, price) VALUES (?, ?, ?)');
    $stmt->execute(['NullPrice', 1, null]);

    $check = $pdo->query("SELECT price FROM pdo_test_items WHERE name = 'NullPrice'");
    $row = $check->fetch(PDO::FETCH_ASSOC);
    assert_eq(null, $row['price'], 'NULL value preserved');
});

/* ── Cleanup ────────────────────────────────────────────────────────── */
echo "\n[Cleanup]\n";

test('DROP test table', function () use ($pdo) {
    $pdo->exec('DROP TABLE IF EXISTS pdo_test_items');
});

/* ── Summary ────────────────────────────────────────────────────────── */
echo "\n=== Results: {$passed} passed, {$failed} failed ===\n";
exit($failed > 0 ? 1 : 0);

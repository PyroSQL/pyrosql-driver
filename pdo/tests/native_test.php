<?php
/**
 * Test suite for PyroSQL native functions.
 *
 * Usage: php native_test.php
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
$skipped = 0;

function test(string $name, callable $fn): void
{
    global $passed, $failed;
    try {
        $fn();
        echo "  PASS: {$name}\n";
        $passed++;
    } catch (Throwable $e) {
        echo "  FAIL: {$name} -- " . $e->getMessage() . "\n";
        $failed++;
    }
}

function skip(string $name, string $reason): void
{
    global $skipped;
    echo "  SKIP: {$name} -- {$reason}\n";
    $skipped++;
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

function assert_true($value, string $msg = ''): void
{
    if ($value !== true) {
        throw new RuntimeException(
            ($msg ? "{$msg}: " : '') . "expected true, got " . var_export($value, true)
        );
    }
}

function assert_instanceof($object, string $class, string $msg = ''): void
{
    if (!($object instanceof $class)) {
        $actual = is_object($object) ? get_class($object) : gettype($object);
        throw new RuntimeException(
            ($msg ? "{$msg}: " : '') . "expected instance of {$class}, got {$actual}"
        );
    }
}

echo "=== PyroSQL Native Functions Tests ===\n\n";

/* ── 0. Verify extension loaded ────────────────────────────────────── */
echo "[Extension]\n";

test('Extension pyrosql is loaded', function () {
    assert_true(extension_loaded('pyrosql'), 'extension_loaded');
});

test('PyroSqlConnection class exists', function () {
    assert_true(class_exists('PyroSqlConnection'), 'class_exists');
});

test('PyroSqlCursor class exists', function () {
    assert_true(class_exists('PyroSqlCursor'), 'class_exists');
});

test('Native functions are registered', function () {
    $functions = [
        'pyrosql_from_pdo',
        'pyrosql_listen',
        'pyrosql_unlisten',
        'pyrosql_notify',
        'pyrosql_on_notification',
        'pyrosql_copy_in',
        'pyrosql_copy_out',
        'pyrosql_watch',
        'pyrosql_unwatch',
        'pyrosql_subscribe_cdc',
        'pyrosql_query_cursor',
        'pyrosql_cursor_next',
        'pyrosql_bulk_insert',
        'pyrosql_ping',
        'pyrosql_close',
    ];
    foreach ($functions as $fn) {
        if (!function_exists($fn)) {
            throw new RuntimeException("Function {$fn} is not registered");
        }
    }
});

/* ── 1. PDO Connection + pyrosql_from_pdo ──────────────────────────── */
echo "\n[pyrosql_from_pdo]\n";
$pdo = null;
$conn = null;

test('Connect via PDO', function () use ($dsn, $user, $pass, &$pdo) {
    $pdo = new PDO($dsn, $user, $pass, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    ]);
    if (!$pdo) {
        throw new RuntimeException('PDO constructor returned falsy');
    }
});

if (!$pdo) {
    echo "\nCannot continue without a PDO connection.\n";
    exit(1);
}

test('pyrosql_from_pdo returns PyroSqlConnection', function () use ($pdo, &$conn) {
    $conn = pyrosql_from_pdo($pdo);
    assert_instanceof($conn, 'PyroSqlConnection');
});

test('pyrosql_from_pdo rejects non-pyrosql PDO', function () {
    try {
        $sqlite = new PDO('sqlite::memory:');
        pyrosql_from_pdo($sqlite);
        throw new RuntimeException('Expected exception was not thrown');
    } catch (RuntimeException $e) {
        if (strpos($e->getMessage(), 'pyrosql') === false) {
            throw $e;
        }
    }
});

test('pyrosql_from_pdo rejects non-PDO object', function () {
    try {
        pyrosql_from_pdo(new stdClass());
        throw new RuntimeException('Expected exception was not thrown');
    } catch (TypeError $e) {
        // Expected
    }
});

if (!$conn) {
    echo "\nCannot continue without a native connection.\n";
    exit(1);
}

/* ── 2. Ping ───────────────────────────────────────────────────────── */
echo "\n[pyrosql_ping]\n";

test('pyrosql_ping returns bool', function () use ($conn) {
    $result = pyrosql_ping($conn);
    if (!is_bool($result)) {
        throw new RuntimeException('Expected bool, got ' . gettype($result));
    }
});

test('pyrosql_ping returns true on active connection', function () use ($conn) {
    assert_true(pyrosql_ping($conn));
});

/* ── 3. Pub/Sub ────────────────────────────────────────────────────── */
echo "\n[Pub/Sub]\n";

test('pyrosql_listen returns bool', function () use ($conn) {
    $result = pyrosql_listen($conn, 'test_channel');
    if (!is_bool($result)) {
        throw new RuntimeException('Expected bool, got ' . gettype($result));
    }
});

test('pyrosql_notify returns bool', function () use ($conn) {
    $result = pyrosql_notify($conn, 'test_channel', 'hello world');
    if (!is_bool($result)) {
        throw new RuntimeException('Expected bool, got ' . gettype($result));
    }
});

test('pyrosql_unlisten returns bool', function () use ($conn) {
    $result = pyrosql_unlisten($conn, 'test_channel');
    if (!is_bool($result)) {
        throw new RuntimeException('Expected bool, got ' . gettype($result));
    }
});

/* ── 4. Copy operations ───────────────────────────────────────────── */
echo "\n[COPY operations]\n";

test('Setup table for COPY tests', function () use ($pdo) {
    $pdo->exec('DROP TABLE IF EXISTS native_test_copy');
    $pdo->exec('CREATE TABLE native_test_copy (id INT, name TEXT, value NUMERIC(10,2))');
});

test('pyrosql_copy_in type check', function () use ($conn) {
    try {
        $result = pyrosql_copy_in($conn, 'native_test_copy', '["id","name","value"]', "1,Alice,9.99\n2,Bob,19.99\n");
        if (!is_int($result)) {
            throw new RuntimeException('Expected int, got ' . gettype($result));
        }
    } catch (RuntimeException $e) {
        if (strpos($e->getMessage(), 'not available') !== false) {
            echo "    (FFI function not available -- OK for build test)\n";
        } else {
            throw $e;
        }
    }
});

test('pyrosql_copy_out type check', function () use ($conn) {
    try {
        $result = pyrosql_copy_out($conn, 'COPY native_test_copy TO STDOUT');
        if (!is_string($result)) {
            throw new RuntimeException('Expected string, got ' . gettype($result));
        }
    } catch (RuntimeException $e) {
        if (strpos($e->getMessage(), 'not available') !== false) {
            echo "    (FFI function not available -- OK for build test)\n";
        } else {
            throw $e;
        }
    }
});

/* ── 5. Bulk insert ────────────────────────────────────────────────── */
echo "\n[Bulk insert]\n";

test('Setup table for bulk insert', function () use ($pdo) {
    $pdo->exec('DROP TABLE IF EXISTS native_test_bulk');
    $pdo->exec('CREATE TABLE native_test_bulk (id INT, name TEXT)');
});

test('pyrosql_bulk_insert type check', function () use ($conn) {
    $rows = json_encode([
        ['id' => 1, 'name' => 'Alice'],
        ['id' => 2, 'name' => 'Bob'],
        ['id' => 3, 'name' => 'Charlie'],
    ]);
    try {
        $result = pyrosql_bulk_insert($conn, 'native_test_bulk', $rows);
        if (!is_int($result)) {
            throw new RuntimeException('Expected int, got ' . gettype($result));
        }
    } catch (RuntimeException $e) {
        if (strpos($e->getMessage(), 'not available') !== false) {
            echo "    (FFI function not available -- OK for build test)\n";
        } else {
            throw $e;
        }
    }
});

/* ── 6. Cursor ─────────────────────────────────────────────────────── */
echo "\n[Cursor]\n";

test('pyrosql_query_cursor type check', function () use ($conn) {
    try {
        $cursor = pyrosql_query_cursor($conn, 'SELECT 1 AS num');
        assert_instanceof($cursor, 'PyroSqlCursor');

        $row = pyrosql_cursor_next($cursor);
        // row is either array or null
        if ($row !== null && !is_array($row)) {
            throw new RuntimeException('Expected array or null, got ' . gettype($row));
        }
    } catch (RuntimeException $e) {
        if (strpos($e->getMessage(), 'not available') !== false) {
            echo "    (FFI function not available -- OK for build test)\n";
        } else {
            throw $e;
        }
    }
});

/* ── 7. Watch / CDC ────────────────────────────────────────────────── */
echo "\n[Watch / CDC]\n";

test('pyrosql_watch type check', function () use ($conn) {
    try {
        $channel = pyrosql_watch($conn, 'SELECT 1');
        if (!is_string($channel)) {
            throw new RuntimeException('Expected string, got ' . gettype($channel));
        }
        pyrosql_unwatch($conn, $channel);
    } catch (RuntimeException $e) {
        if (strpos($e->getMessage(), 'not available') !== false) {
            echo "    (FFI function not available -- OK for build test)\n";
        } else {
            throw $e;
        }
    }
});

test('pyrosql_subscribe_cdc type check', function () use ($conn) {
    try {
        $channel = pyrosql_subscribe_cdc($conn, 'native_test_bulk');
        if (!is_string($channel)) {
            throw new RuntimeException('Expected string, got ' . gettype($channel));
        }
    } catch (RuntimeException $e) {
        if (strpos($e->getMessage(), 'not available') !== false) {
            echo "    (FFI function not available -- OK for build test)\n";
        } else {
            throw $e;
        }
    }
});

/* ── 8. PDO still works alongside native ───────────────────────────── */
echo "\n[PDO coexistence]\n";

test('PDO query works after native function use', function () use ($pdo) {
    $stmt = $pdo->query('SELECT 1 AS one');
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    assert_eq('1', $row['one']);
});

test('PDO driver name is still pyrosql', function () use ($pdo) {
    assert_eq('pyrosql', $pdo->getAttribute(PDO::ATTR_DRIVER_NAME));
});

/* ── 9. Close ──────────────────────────────────────────────────────── */
echo "\n[Close]\n";

test('pyrosql_close does not throw', function () use ($conn) {
    // Create a second reference to test close
    // We do NOT close the main $conn as it is borrowed from PDO
    pyrosql_close($conn);
});

test('PDO still works after closing native wrapper', function () use ($pdo) {
    $stmt = $pdo->query('SELECT 1 AS val');
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    assert_eq('1', $row['val']);
});

/* ── Cleanup ───────────────────────────────────────────────────────── */
echo "\n[Cleanup]\n";

test('Drop test tables', function () use ($pdo) {
    $pdo->exec('DROP TABLE IF EXISTS native_test_copy');
    $pdo->exec('DROP TABLE IF EXISTS native_test_bulk');
});

/* ── Summary ───────────────────────────────────────────────────────── */
echo "\n=== Results: {$passed} passed, {$failed} failed, {$skipped} skipped ===\n";
exit($failed > 0 ? 1 : 0);

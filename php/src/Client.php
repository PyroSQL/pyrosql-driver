<?php

declare(strict_types=1);

namespace PyroSQL;

/**
 * PyroSQL PHP client — connects to PyroSQL via QUIC using the C-ABI FFI shared library.
 *
 * Requires PHP 7.4+ with ext-ffi enabled.
 *
 * Usage:
 *
 *     $db = \PyroSQL\Client::connect("vsql://localhost:12520/mydb");
 *     $result = $db->query("SELECT * FROM users");
 *     foreach ($result['rows'] as $row) {
 *         echo $row[0] . "\n";
 *     }
 *     $db->close();
 */
class Client
{
    private static ?\FFI $ffi = null;
    private static bool $initialized = false;

    /** @var \FFI\CData|null Opaque handle to the Rust client */
    private $handle;

    private function __construct()
    {
        $this->handle = null;
    }

    /**
     * Locate the shared library for the current platform.
     */
    private static function findLibrary(): string
    {
        // Allow override via environment variable
        $env = getenv('PYROSQL_FFI_LIB');
        if ($env !== false && file_exists($env)) {
            return $env;
        }

        // Platform-specific library name
        if (PHP_OS_FAMILY === 'Darwin') {
            $name = 'libpyrosql_ffi.dylib';
        } elseif (PHP_OS_FAMILY === 'Windows') {
            $name = 'pyrosql_ffi.dll';
        } else {
            $name = 'libpyrosql_ffi.so';
        }

        // Search paths: next to this file, header dir, common install paths
        $searchPaths = [
            __DIR__ . '/../' . $name,
            __DIR__ . '/../../client-ffi/dist/' . $name,
            '/usr/local/lib/' . $name,
            '/usr/lib/' . $name,
        ];

        foreach ($searchPaths as $path) {
            if (file_exists($path)) {
                return realpath($path);
            }
        }

        throw new \RuntimeException(
            "Could not find $name. Set PYROSQL_FFI_LIB environment variable " .
            "to the full path of the shared library."
        );
    }

    /**
     * Initialize the FFI layer and Tokio runtime (called once automatically).
     */
    private static function ensureInit(): void
    {
        if (self::$initialized) {
            return;
        }

        $lib = self::findLibrary();
        $header = __DIR__ . '/../pyrosql.h';

        if (!file_exists($header)) {
            throw new \RuntimeException("C header not found at $header");
        }

        self::$ffi = \FFI::cdef(file_get_contents($header), $lib);
        self::$ffi->pyro_pwire_init();
        self::$initialized = true;
    }

    /**
     * Get the shared FFI instance (for use by Pool and other internal classes).
     *
     * @return \FFI
     * @throws \RuntimeException if FFI is not initialized
     * @internal
     */
    public static function ffi(): \FFI
    {
        self::ensureInit();
        return self::$ffi;
    }

    /**
     * Connect to a PyroSQL server.
     *
     * Supported URL schemes:
     * - "vsql://host:12520/db" — PyroSQL QUIC (fastest)
     * - "postgres://host:5432/db" — PostgreSQL wire protocol
     * - "mysql://host:3306/db" — MySQL wire protocol
     * - "unix:///path/to/sock?db=mydb" — Unix domain socket
     *
     * Append "?syntax_mode=mysql" to override SQL syntax mode.
     *
     * @param string $url Connection URL
     * @return self
     * @throws \RuntimeException on connection failure
     */
    public static function connect(string $url): self
    {
        self::ensureInit();

        $client = new self();
        $client->handle = self::$ffi->pyro_pwire_connect($url);

        if (\FFI::isNull($client->handle)) {
            throw new \RuntimeException("Failed to connect to $url");
        }

        return $client;
    }

    /**
     * Execute a SELECT query and return the result as an associative array.
     *
     * Returns: ["columns" => [...], "rows" => [[...], ...], "rows_affected" => int]
     *
     * @param string $sql SQL query
     * @return array<string, mixed>
     * @throws \RuntimeException on query failure or if result contains an error
     */
    public function query(string $sql): array
    {
        $this->assertConnected();

        $ptr = self::$ffi->pyro_pwire_query($this->handle, $sql);

        if (\FFI::isNull($ptr)) {
            throw new \RuntimeException("Query returned NULL (connection lost?)");
        }

        $json = \FFI::string($ptr);
        self::$ffi->pyro_pwire_free_string($ptr);

        $result = json_decode($json, true);

        if ($result === null) {
            throw new \RuntimeException("Failed to decode query result JSON");
        }

        if (isset($result['error'])) {
            throw new \RuntimeException("Query error: " . $result['error']);
        }

        return $result;
    }

    /**
     * Execute a DML statement (INSERT, UPDATE, DELETE).
     *
     * @param string $sql SQL statement
     * @return int Number of rows affected, or -1 on error
     */
    public function execute(string $sql): int
    {
        $this->assertConnected();

        $result = self::$ffi->pyro_pwire_execute($this->handle, $sql);

        if ($result < 0) {
            throw new \RuntimeException("Execute failed for: $sql");
        }

        return $result;
    }

    /**
     * Begin a transaction.
     *
     * @return Transaction
     * @throws \RuntimeException on failure
     */
    public function begin(): Transaction
    {
        $this->assertConnected();

        $ptr = self::$ffi->pyro_pwire_begin($this->handle);

        if (\FFI::isNull($ptr)) {
            throw new \RuntimeException("Begin transaction returned NULL");
        }

        $json = \FFI::string($ptr);
        self::$ffi->pyro_pwire_free_string($ptr);

        $result = json_decode($json, true);

        if ($result === null) {
            throw new \RuntimeException("Failed to decode begin transaction response");
        }

        if (isset($result['error'])) {
            throw new \RuntimeException("Begin transaction error: " . $result['error']);
        }

        return new Transaction($this, $result['transaction_id']);
    }

    /**
     * Prepare a statement for repeated execution.
     *
     * @param string $sql SQL statement to prepare
     * @return PreparedStatement
     */
    public function prepare(string $sql): PreparedStatement
    {
        return new PreparedStatement($this, $sql);
    }

    /**
     * Bulk insert rows into a table.
     *
     * @param string $table Target table name
     * @param array<string> $columns Column names
     * @param array<array<mixed>> $rows Rows to insert
     * @return int Number of rows inserted
     * @throws \RuntimeException on failure
     */
    public function bulkInsert(string $table, array $columns, array $rows): int
    {
        $this->assertConnected();

        $jsonData = json_encode([
            'columns' => $columns,
            'rows' => $rows,
        ]);

        $result = self::$ffi->pyro_pwire_bulk_insert($this->handle, $table, $jsonData);

        if ($result < 0) {
            throw new \RuntimeException("Bulk insert failed for table: $table");
        }

        return $result;
    }

    /**
     * Commit a transaction by ID (internal use by Transaction class).
     *
     * @param string $txId Transaction ID
     * @throws \RuntimeException on failure
     * @internal
     */
    public function commitTransaction(string $txId): void
    {
        $this->assertConnected();

        $result = self::$ffi->pyro_pwire_commit($this->handle, $txId);

        if ($result < 0) {
            throw new \RuntimeException("Commit failed for transaction: $txId");
        }
    }

    /**
     * Rollback a transaction by ID (internal use by Transaction class).
     *
     * @param string $txId Transaction ID
     * @throws \RuntimeException on failure
     * @internal
     */
    public function rollbackTransaction(string $txId): void
    {
        $this->assertConnected();

        $result = self::$ffi->pyro_pwire_rollback($this->handle, $txId);

        if ($result < 0) {
            throw new \RuntimeException("Rollback failed for transaction: $txId");
        }
    }

    /**
     * Subscribe to a reactive query via WATCH.
     *
     * @param string $sql SQL query to watch
     * @return string The channel name for change notifications
     * @throws \RuntimeException on failure
     */
    public function watch(string $sql): string
    {
        $this->assertConnected();

        $ptr = self::$ffi->pyro_pwire_watch($this->handle, $sql);

        if (\FFI::isNull($ptr)) {
            throw new \RuntimeException("Watch returned NULL");
        }

        $json = \FFI::string($ptr);
        self::$ffi->pyro_pwire_free_string($ptr);

        $result = json_decode($json, true);

        if ($result === null) {
            throw new \RuntimeException("Failed to decode watch response");
        }

        if (isset($result['error'])) {
            throw new \RuntimeException("Watch error: " . $result['error']);
        }

        return $result['channel'];
    }

    /**
     * Unsubscribe from a WATCH channel.
     *
     * @param string $channel Channel name returned by watch()
     * @throws \RuntimeException on failure
     */
    public function unwatch(string $channel): void
    {
        $this->assertConnected();

        $result = self::$ffi->pyro_pwire_unwatch($this->handle, $channel);

        if ($result < 0) {
            throw new \RuntimeException("Unwatch failed for channel: $channel");
        }
    }

    /**
     * Subscribe to a PubSub channel via LISTEN.
     *
     * @param string $channel Channel name
     * @throws \RuntimeException on failure
     */
    public function listen(string $channel): void
    {
        $this->assertConnected();

        $result = self::$ffi->pyro_pwire_listen($this->handle, $channel);

        if ($result < 0) {
            throw new \RuntimeException("Listen failed for channel: $channel");
        }
    }

    /**
     * Unsubscribe from a PubSub channel via UNLISTEN.
     *
     * @param string $channel Channel name
     * @throws \RuntimeException on failure
     */
    public function unlisten(string $channel): void
    {
        $this->assertConnected();

        $result = self::$ffi->pyro_pwire_unlisten($this->handle, $channel);

        if ($result < 0) {
            throw new \RuntimeException("Unlisten failed for channel: $channel");
        }
    }

    /**
     * Send a notification to a PubSub channel via NOTIFY.
     *
     * @param string $channel Channel name
     * @param string $payload Notification payload
     * @throws \RuntimeException on failure
     */
    public function notify(string $channel, string $payload): void
    {
        $this->assertConnected();

        $result = self::$ffi->pyro_pwire_notify($this->handle, $channel, $payload);

        if ($result < 0) {
            throw new \RuntimeException("Notify failed for channel: $channel");
        }
    }

    /**
     * COPY OUT: execute a query and return CSV data.
     *
     * @param string $sql SQL query (e.g. "COPY (SELECT * FROM t) TO STDOUT")
     * @return string CSV data including header
     * @throws \RuntimeException on failure
     */
    public function copyOut(string $sql): string
    {
        $this->assertConnected();

        $ptr = self::$ffi->pyro_pwire_copy_out($this->handle, $sql);

        if (\FFI::isNull($ptr)) {
            throw new \RuntimeException("COPY OUT returned NULL");
        }

        $csv = \FFI::string($ptr);
        self::$ffi->pyro_pwire_free_string($ptr);

        if (str_starts_with($csv, '{"error"')) {
            $result = json_decode($csv, true);
            throw new \RuntimeException("COPY OUT error: " . ($result['error'] ?? 'unknown'));
        }

        return $csv;
    }

    /**
     * COPY IN: send CSV data to a table.
     *
     * @param string $table Target table name
     * @param array<string> $columns Column names
     * @param string $csvData CSV data (no header)
     * @return int Number of rows inserted
     * @throws \RuntimeException on failure
     */
    public function copyIn(string $table, array $columns, string $csvData): int
    {
        $this->assertConnected();

        $columnsJson = json_encode($columns);
        $result = self::$ffi->pyro_pwire_copy_in($this->handle, $table, $columnsJson, $csvData);

        if ($result < 0) {
            throw new \RuntimeException("COPY IN failed for table: $table");
        }

        return $result;
    }

    /**
     * Subscribe to CDC events on a table.
     *
     * @param string $table Table name
     * @return array{subscription_id: string, table: string}
     * @throws \RuntimeException on failure
     */
    public function subscribeCdc(string $table): array
    {
        $this->assertConnected();

        $ptr = self::$ffi->pyro_pwire_subscribe_cdc($this->handle, $table);

        if (\FFI::isNull($ptr)) {
            throw new \RuntimeException("Subscribe CDC returned NULL");
        }

        $json = \FFI::string($ptr);
        self::$ffi->pyro_pwire_free_string($ptr);

        $result = json_decode($json, true);

        if ($result === null) {
            throw new \RuntimeException("Failed to decode CDC subscription response");
        }

        if (isset($result['error'])) {
            throw new \RuntimeException("Subscribe CDC error: " . $result['error']);
        }

        return $result;
    }

    /**
     * Execute a query and return a Cursor for row-by-row iteration.
     *
     * v1 implementation: fetches all rows up-front and iterates locally.
     * True server-side streaming cursors are planned for v2.
     *
     * @param string $sql SQL query
     * @return Cursor
     * @throws \RuntimeException on query failure
     */
    public function queryCursor(string $sql): Cursor
    {
        $result = $this->query($sql);
        return new Cursor($result);
    }

    /**
     * Poll for pending notifications.
     *
     * PHP cannot easily use async callbacks via FFI. This method provides a
     * synchronous polling alternative to onNotification(). Returns any
     * notifications that have been received since the last poll.
     *
     * Note: In the current v1 implementation, notifications are delivered via
     * the server-push mechanism and buffered in the Rust client. This method
     * drains that buffer.
     *
     * @return array<array{channel: string, payload: string}> List of notifications
     */
    public function pollNotifications(): array
    {
        // v1: Poll is not yet wired to the Rust client's internal notification
        // buffer. For now, return an empty array. Users should use LISTEN +
        // a query loop as a workaround until v2 adds native poll support.
        return [];
    }

    /**
     * Close the connection and free the handle.
     */
    public function close(): void
    {
        if ($this->handle !== null && !(\FFI::isNull($this->handle))) {
            self::$ffi->pyro_pwire_close($this->handle);
            $this->handle = null;
        }
    }

    /**
     * Execute a query with auto-reconnect on connection failure.
     *
     * If the initial query fails with a connection error, the client will
     * attempt to reconnect and retry the query once.
     *
     * @param string $sql SQL query
     * @return array<string, mixed>
     * @throws \RuntimeException on query failure
     */
    public function queryRetry(string $sql): array
    {
        $this->assertConnected();

        $ptr = self::$ffi->pyro_pwire_query_retry($this->handle, $sql);

        if (\FFI::isNull($ptr)) {
            throw new \RuntimeException("Query retry returned NULL (connection lost?)");
        }

        $json = \FFI::string($ptr);
        self::$ffi->pyro_pwire_free_string($ptr);

        $result = json_decode($json, true);

        if ($result === null) {
            throw new \RuntimeException("Failed to decode query retry result JSON");
        }

        if (isset($result['error'])) {
            throw new \RuntimeException("Query retry error: " . $result['error']);
        }

        return $result;
    }

    /**
     * Execute a DML statement with auto-reconnect on connection failure.
     *
     * @param string $sql SQL statement
     * @return int Number of rows affected
     * @throws \RuntimeException on failure
     */
    public function executeRetry(string $sql): int
    {
        $this->assertConnected();

        $result = self::$ffi->pyro_pwire_execute_retry($this->handle, $sql);

        if ($result < 0) {
            throw new \RuntimeException("Execute retry failed for: $sql");
        }

        return $result;
    }

    public function __destruct()
    {
        $this->close();
    }

    private function assertConnected(): void
    {
        if ($this->handle === null || \FFI::isNull($this->handle)) {
            throw new \RuntimeException("Not connected (handle is null)");
        }
    }
}

/**
 * A connection pool for PyroSQL.
 *
 * Manages a pool of reusable connections with a configurable maximum size.
 *
 * Usage:
 *
 *     $pool = \PyroSQL\Pool::create("vsql://localhost:12520/mydb", 10);
 *     $client = $pool->get();
 *     $result = $client->query("SELECT * FROM users");
 *     $pool->returnClient($client);
 *     $pool->destroy();
 */
class Pool
{
    private static ?\FFI $ffi = null;

    /** @var \FFI\CData|null Opaque handle to the Rust pool */
    private $handle;

    private function __construct()
    {
        $this->handle = null;
    }

    /**
     * Create a new connection pool.
     *
     * @param string $url Connection URL, e.g. "vsql://localhost:12520/mydb"
     * @param int $maxSize Maximum number of connections in the pool
     * @return self
     * @throws \RuntimeException on failure
     */
    public static function create(string $url, int $maxSize = 10): self
    {
        self::ensureFfi();

        $pool = new self();
        $pool->handle = self::$ffi->pyro_pwire_pool_create($url, $maxSize);

        if (\FFI::isNull($pool->handle)) {
            throw new \RuntimeException("Failed to create pool for $url");
        }

        return $pool;
    }

    /**
     * Get a connection from the pool.
     *
     * @return PooledClient
     * @throws \RuntimeException on failure
     */
    public function get(): PooledClient
    {
        $this->assertOpen();

        $clientHandle = self::$ffi->pyro_pwire_pool_get($this->handle);

        if (\FFI::isNull($clientHandle)) {
            throw new \RuntimeException("Failed to get connection from pool");
        }

        return new PooledClient($this, $clientHandle);
    }

    /**
     * Return a client connection to the pool.
     *
     * @param PooledClient $client The client to return
     * @internal
     */
    public function returnHandle($clientHandle): void
    {
        $this->assertOpen();
        self::$ffi->pyro_pwire_pool_return($this->handle, $clientHandle);
    }

    /**
     * Destroy the pool and free all resources.
     */
    public function destroy(): void
    {
        if ($this->handle !== null && !(\FFI::isNull($this->handle))) {
            self::$ffi->pyro_pwire_pool_destroy($this->handle);
            $this->handle = null;
        }
    }

    public function __destruct()
    {
        $this->destroy();
    }

    private static function ensureFfi(): void
    {
        if (self::$ffi === null) {
            self::$ffi = Client::ffi();
        }
    }

    private function assertOpen(): void
    {
        if ($this->handle === null || \FFI::isNull($this->handle)) {
            throw new \RuntimeException("Pool is not open (handle is null)");
        }
    }
}

/**
 * A client connection borrowed from a Pool.
 *
 * When done, call returnToPool() or let it auto-return on destruction.
 */
class PooledClient
{
    private Pool $pool;
    private $handle;
    private bool $returned = false;

    public function __construct(Pool $pool, $handle)
    {
        $this->pool = $pool;
        $this->handle = $handle;
    }

    /**
     * Execute a SELECT query.
     *
     * @param string $sql SQL query
     * @return array<string, mixed>
     * @throws \RuntimeException on failure
     */
    public function query(string $sql): array
    {
        $this->assertActive();

        $ffi = Client::ffi();
        $ptr = $ffi->pyro_pwire_query($this->handle, $sql);

        if (\FFI::isNull($ptr)) {
            throw new \RuntimeException("Query returned NULL (connection lost?)");
        }

        $json = \FFI::string($ptr);
        $ffi->pyro_pwire_free_string($ptr);

        $result = json_decode($json, true);

        if ($result === null) {
            throw new \RuntimeException("Failed to decode query result JSON");
        }

        if (isset($result['error'])) {
            throw new \RuntimeException("Query error: " . $result['error']);
        }

        return $result;
    }

    /**
     * Execute a DML statement.
     *
     * @param string $sql SQL statement
     * @return int Rows affected
     * @throws \RuntimeException on failure
     */
    public function execute(string $sql): int
    {
        $this->assertActive();

        $ffi = Client::ffi();
        $result = $ffi->pyro_pwire_execute($this->handle, $sql);

        if ($result < 0) {
            throw new \RuntimeException("Execute failed for: $sql");
        }

        return $result;
    }

    /**
     * Return this connection to the pool.
     */
    public function returnToPool(): void
    {
        if (!$this->returned) {
            $this->pool->returnHandle($this->handle);
            $this->returned = true;
            $this->handle = null;
        }
    }

    public function __destruct()
    {
        $this->returnToPool();
    }

    private function assertActive(): void
    {
        if ($this->returned || $this->handle === null) {
            throw new \RuntimeException("PooledClient already returned to pool");
        }
    }
}

/**
 * A transaction handle.
 *
 * Created by Client::begin(). Must be explicitly committed or rolled back.
 */
class Transaction
{
    private Client $client;
    private string $txId;
    private bool $finished = false;

    public function __construct(Client $client, string $txId)
    {
        $this->client = $client;
        $this->txId = $txId;
    }

    /**
     * Execute a query within this transaction.
     *
     * @param string $sql SQL query
     * @return array<string, mixed>
     */
    public function query(string $sql): array
    {
        $this->assertActive();
        return $this->client->query("/* tx:{$this->txId} */ $sql");
    }

    /**
     * Execute a DML statement within this transaction.
     *
     * @param string $sql SQL statement
     * @return int Rows affected
     */
    public function execute(string $sql): int
    {
        $this->assertActive();
        return $this->client->execute("/* tx:{$this->txId} */ $sql");
    }

    /**
     * Commit the transaction.
     *
     * @throws \RuntimeException on failure or if already finished
     */
    public function commit(): void
    {
        $this->assertActive();
        $this->client->commitTransaction($this->txId);
        $this->finished = true;
    }

    /**
     * Rollback the transaction.
     *
     * @throws \RuntimeException on failure or if already finished
     */
    public function rollback(): void
    {
        $this->assertActive();
        $this->client->rollbackTransaction($this->txId);
        $this->finished = true;
    }

    /**
     * Create a savepoint within this transaction.
     *
     * @param string $name Savepoint name
     */
    public function savepoint(string $name): void
    {
        $this->assertActive();
        $this->client->execute("/* tx:{$this->txId} */ SAVEPOINT $name");
    }

    /**
     * Rollback to a savepoint within this transaction.
     *
     * @param string $name Savepoint name
     */
    public function rollbackTo(string $name): void
    {
        $this->assertActive();
        $this->client->execute("/* tx:{$this->txId} */ ROLLBACK TO SAVEPOINT $name");
    }

    private function assertActive(): void
    {
        if ($this->finished) {
            throw new \RuntimeException("Transaction already finished (committed or rolled back)");
        }
    }
}

/**
 * A prepared statement handle.
 *
 * Created by Client::prepare(). Can be executed multiple times with
 * different parameters. Currently uses client-side interpolation;
 * server-side parameter binding is planned.
 */
class PreparedStatement
{
    private Client $client;
    private string $sql;

    public function __construct(Client $client, string $sql)
    {
        $this->client = $client;
        $this->sql = $sql;
    }

    /**
     * Execute the prepared statement as a query.
     *
     * @param array<mixed> $params Parameter values (currently unused, placeholder for future server-side binding)
     * @return array<string, mixed>
     */
    public function query(array $params = []): array
    {
        return $this->client->query($this->sql);
    }

    /**
     * Execute the prepared statement as a DML operation.
     *
     * @param array<mixed> $params Parameter values (currently unused, placeholder for future server-side binding)
     * @return int Rows affected
     */
    public function execute(array $params = []): int
    {
        return $this->client->execute($this->sql);
    }
}

/**
 * A client-side cursor for iterating query results row by row.
 *
 * v1 implementation: the full result set is fetched up-front and rows are
 * returned one at a time via next(). True server-side streaming cursors
 * are planned for v2.
 */
class Cursor
{
    /** @var array<string> */
    private array $columns;

    /** @var array<array<mixed>> */
    private array $rows;

    /** @var int */
    private int $index = 0;

    /**
     * @param array{columns: array<string>, rows: array<array<mixed>>} $result
     */
    public function __construct(array $result)
    {
        $this->columns = $result['columns'] ?? [];
        $this->rows = $result['rows'] ?? [];
    }

    /**
     * Get column names of the result set.
     *
     * @return array<string>
     */
    public function columns(): array
    {
        return $this->columns;
    }

    /**
     * Whether there are more rows to read.
     */
    public function hasNext(): bool
    {
        return $this->index < count($this->rows);
    }

    /**
     * Return the next row, or null if exhausted.
     *
     * @return array<mixed>|null
     */
    public function next(): ?array
    {
        if ($this->index >= count($this->rows)) {
            return null;
        }
        return $this->rows[$this->index++];
    }

    /**
     * Reset the cursor to the beginning.
     */
    public function reset(): void
    {
        $this->index = 0;
    }
}

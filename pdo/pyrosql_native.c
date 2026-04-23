/* pyrosql_native.c — Native PHP functions for advanced PyroSQL features.
 *
 * Provides pub/sub, COPY, live queries, CDC, cursor iteration, bulk insert,
 * and connection management functions that go beyond what PDO can express.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "pdo/php_pdo.h"
#include "pdo/php_pdo_driver.h"
#include "php_pyrosql.h"
#include "Zend/zend_exceptions.h"
#include "Zend/zend_interfaces.h"
#include "ext/json/php_json.h"
#include "ext/spl/spl_exceptions.h"

#include <string.h>

/* ── Class entries ─────────────────────────────────────────────────── */
zend_class_entry *pyrosql_connection_ce = NULL;
zend_class_entry *pyrosql_cursor_ce = NULL;

static zend_object_handlers pyrosql_connection_handlers;
static zend_object_handlers pyrosql_cursor_handlers;

/* ── Object structures ─────────────────────────────────────────────── */
typedef struct {
    pyrosql_conn *conn;       /* Borrowed pointer — do NOT free (owned by PDO) */
    int owns_handle;          /* 0 if borrowed from PDO, 1 if standalone */
    zend_object std;
} pyrosql_connection_obj;

typedef struct {
    pyrosql_conn *conn;       /* Borrowed pointer to connection */
    char *cursor_id;          /* Cursor identifier from FFI */
    int closed;
    zend_object std;
} pyrosql_cursor_obj;

/* ── Object offset helpers ─────────────────────────────────────────── */
static inline pyrosql_connection_obj *pyrosql_connection_from_obj(zend_object *obj) {
    return (pyrosql_connection_obj *)((char *)obj - XtOffsetOf(pyrosql_connection_obj, std));
}

static inline pyrosql_cursor_obj *pyrosql_cursor_from_obj(zend_object *obj) {
    return (pyrosql_cursor_obj *)((char *)obj - XtOffsetOf(pyrosql_cursor_obj, std));
}

#define Z_PYROSQL_CONN_P(zv) pyrosql_connection_from_obj(Z_OBJ_P(zv))
#define Z_PYROSQL_CURSOR_P(zv) pyrosql_cursor_from_obj(Z_OBJ_P(zv))

/* ── PyroSqlConnection object handlers ─────────────────────────────── */
static zend_object *pyrosql_connection_create(zend_class_entry *ce)
{
    pyrosql_connection_obj *intern = zend_object_alloc(sizeof(pyrosql_connection_obj), ce);
    intern->conn = NULL;
    intern->owns_handle = 0;

    zend_object_std_init(&intern->std, ce);
    object_properties_init(&intern->std, ce);
    intern->std.handlers = &pyrosql_connection_handlers;

    return &intern->std;
}

static void pyrosql_connection_free(zend_object *obj)
{
    pyrosql_connection_obj *intern = pyrosql_connection_from_obj(obj);

    if (intern->owns_handle && intern->conn) {
        if (intern->conn->handle && fn_close) {
            fn_close(intern->conn->handle);
        }
        if (intern->conn->dsn_host) efree(intern->conn->dsn_host);
        if (intern->conn->dsn_dbname) efree(intern->conn->dsn_dbname);
        efree(intern->conn);
    }
    intern->conn = NULL;

    zend_object_std_dtor(&intern->std);
}

/* ── PyroSqlCursor object handlers ─────────────────────────────────── */
static zend_object *pyrosql_cursor_create(zend_class_entry *ce)
{
    pyrosql_cursor_obj *intern = zend_object_alloc(sizeof(pyrosql_cursor_obj), ce);
    intern->conn = NULL;
    intern->cursor_id = NULL;
    intern->closed = 0;

    zend_object_std_init(&intern->std, ce);
    object_properties_init(&intern->std, ce);
    intern->std.handlers = &pyrosql_cursor_handlers;

    return &intern->std;
}

static void pyrosql_cursor_free(zend_object *obj)
{
    pyrosql_cursor_obj *intern = pyrosql_cursor_from_obj(obj);

    if (!intern->closed && intern->cursor_id && intern->conn && intern->conn->handle && fn_cursor_close) {
        char *result = fn_cursor_close(intern->conn->handle, intern->cursor_id);
        if (result) fn_free_string(result);
    }
    if (intern->cursor_id) efree(intern->cursor_id);
    intern->conn = NULL;

    zend_object_std_dtor(&intern->std);
}

/* ── Helper: validate connection object ────────────────────────────── */
static pyrosql_conn *get_conn_from_zval(zval *zconn, const char *func_name)
{
    if (Z_TYPE_P(zconn) != IS_OBJECT || !instanceof_function(Z_OBJCE_P(zconn), pyrosql_connection_ce)) {
        zend_throw_exception_ex(zend_ce_type_error, 0,
            "%s(): Argument #1 must be of type PyroSqlConnection, %s given",
            func_name, zend_zval_type_name(zconn));
        return NULL;
    }

    pyrosql_connection_obj *obj = Z_PYROSQL_CONN_P(zconn);
    if (!obj->conn || !obj->conn->handle) {
        zend_throw_exception_ex(spl_ce_RuntimeException, 0,
            "%s(): Connection is closed or invalid", func_name);
        return NULL;
    }

    return obj->conn;
}

/* ── Helper: parse JSON result and extract rows_affected count ─────── */
static int64_t parse_rows_affected_from_json(const char *json)
{
    if (!json) return -1;

    /* Look for "rows_affected": number */
    const char *p = strstr(json, "\"rows_affected\"");
    if (!p) return 0;
    p = strchr(p, ':');
    if (!p) return 0;
    p++;
    while (*p == ' ') p++;
    return (int64_t)strtoll(p, NULL, 10);
}

/* ══════════════════════════════════════════════════════════════════════
 * Native PHP functions
 * ══════════════════════════════════════════════════════════════════════ */

/* ── pyrosql_from_pdo(PDO $pdo): PyroSqlConnection ────────────────── */
PHP_FUNCTION(pyrosql_from_pdo)
{
    zval *zpdo;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_OBJECT_OF_CLASS(zpdo, php_pdo_get_dbh_ce())
    ZEND_PARSE_PARAMETERS_END();

    pdo_dbh_t *dbh = Z_PDO_DBH_P(zpdo);
    if (!dbh || !dbh->driver) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_from_pdo(): Invalid PDO object", 0);
        RETURN_THROWS();
    }

    /* Verify this is a pyrosql PDO connection */
    if (strcmp(dbh->driver->driver_name, "pyrosql") != 0) {
        zend_throw_exception_ex(spl_ce_RuntimeException, 0,
            "pyrosql_from_pdo(): PDO connection uses driver '%s', expected 'pyrosql'",
            dbh->driver->driver_name);
        RETURN_THROWS();
    }

    pyrosql_conn *conn = (pyrosql_conn *)dbh->driver_data;
    if (!conn || !conn->handle) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_from_pdo(): Connection is not established", 0);
        RETURN_THROWS();
    }

    /* Create PyroSqlConnection object wrapping the borrowed conn */
    object_init_ex(return_value, pyrosql_connection_ce);
    pyrosql_connection_obj *obj = Z_PYROSQL_CONN_P(return_value);
    obj->conn = conn;
    obj->owns_handle = 0; /* PDO owns the handle */
}

/* ── pyrosql_listen(PyroSqlConnection $conn, string $channel): bool ── */
PHP_FUNCTION(pyrosql_listen)
{
    zval *zconn;
    zend_string *channel;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
        Z_PARAM_STR(channel)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_conn *conn = get_conn_from_zval(zconn, "pyrosql_listen");
    if (!conn) RETURN_THROWS();

    if (!fn_listen) {
        /* Fallback: use SQL LISTEN */
        char sql[512];
        snprintf(sql, sizeof(sql), "LISTEN %s", ZSTR_VAL(channel));
        int64_t result = fn_execute(conn->handle, sql);
        RETURN_BOOL(result >= 0);
    }

    int32_t result = fn_listen(conn->handle, ZSTR_VAL(channel));
    RETURN_BOOL(result != 0);
}

/* ── pyrosql_unlisten(PyroSqlConnection $conn, string $channel): bool  */
PHP_FUNCTION(pyrosql_unlisten)
{
    zval *zconn;
    zend_string *channel;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
        Z_PARAM_STR(channel)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_conn *conn = get_conn_from_zval(zconn, "pyrosql_unlisten");
    if (!conn) RETURN_THROWS();

    if (!fn_unlisten) {
        char sql[512];
        snprintf(sql, sizeof(sql), "UNLISTEN %s", ZSTR_VAL(channel));
        int64_t result = fn_execute(conn->handle, sql);
        RETURN_BOOL(result >= 0);
    }

    int32_t result = fn_unlisten(conn->handle, ZSTR_VAL(channel));
    RETURN_BOOL(result != 0);
}

/* ── pyrosql_notify(PyroSqlConnection $conn, string $channel, string $payload): bool */
PHP_FUNCTION(pyrosql_notify)
{
    zval *zconn;
    zend_string *channel;
    zend_string *payload;

    ZEND_PARSE_PARAMETERS_START(3, 3)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
        Z_PARAM_STR(channel)
        Z_PARAM_STR(payload)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_conn *conn = get_conn_from_zval(zconn, "pyrosql_notify");
    if (!conn) RETURN_THROWS();

    if (!fn_notify) {
        /* Fallback: use SQL NOTIFY */
        size_t sql_len = ZSTR_LEN(channel) + ZSTR_LEN(payload) + 64;
        char *sql = emalloc(sql_len);
        snprintf(sql, sql_len, "NOTIFY %s, '%s'", ZSTR_VAL(channel), ZSTR_VAL(payload));
        int64_t result = fn_execute(conn->handle, sql);
        efree(sql);
        RETURN_BOOL(result >= 0);
    }

    int32_t result = fn_notify(conn->handle, ZSTR_VAL(channel), ZSTR_VAL(payload));
    RETURN_BOOL(result != 0);
}

/* ── pyrosql_on_notification(PyroSqlConnection $conn, callable $callback): void */
PHP_FUNCTION(pyrosql_on_notification)
{
    zval *zconn;
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
        Z_PARAM_FUNC(fci, fcc)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_conn *conn = get_conn_from_zval(zconn, "pyrosql_on_notification");
    if (!conn) RETURN_THROWS();

    if (!fn_get_notification) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_on_notification(): FFI function pyro_pwire_get_notification not available", 0);
        RETURN_THROWS();
    }

    /* Poll for a notification and invoke callback if one is available */
    char *notification_json = fn_get_notification(conn->handle);
    if (notification_json) {
        /* Parse the JSON notification: {"channel":"...","payload":"..."} */
        zval args[1];
        ZVAL_STRING(&args[0], notification_json);
        fn_free_string(notification_json);

        zval retval;
        fci.param_count = 1;
        fci.params = args;
        fci.retval = &retval;

        zend_call_function(&fci, &fcc);

        zval_ptr_dtor(&args[0]);
        zval_ptr_dtor(&retval);
    }
}

/* ── pyrosql_copy_in(conn, table, columns_json, csv_data): int ───── */
PHP_FUNCTION(pyrosql_copy_in)
{
    zval *zconn;
    zend_string *table;
    zend_string *columns_json;
    zend_string *csv_data;

    ZEND_PARSE_PARAMETERS_START(4, 4)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
        Z_PARAM_STR(table)
        Z_PARAM_STR(columns_json)
        Z_PARAM_STR(csv_data)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_conn *conn = get_conn_from_zval(zconn, "pyrosql_copy_in");
    if (!conn) RETURN_THROWS();

    if (!fn_copy_in) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_copy_in(): FFI function pyro_pwire_copy_in not available", 0);
        RETURN_THROWS();
    }

    char *result = fn_copy_in(conn->handle, ZSTR_VAL(table),
                               ZSTR_VAL(columns_json), ZSTR_VAL(csv_data));
    if (!result) {
        RETURN_LONG(-1);
    }

    /* Parse rows_affected from result JSON */
    int64_t affected = parse_rows_affected_from_json(result);

    /* Check for error */
    if (strstr(result, "\"error\"")) {
        const char *err_start = strstr(result, "\"error\":\"");
        if (err_start) {
            err_start += 9;
            const char *err_end = strchr(err_start, '"');
            if (err_end) {
                size_t err_len = err_end - err_start;
                char *err_msg = emalloc(err_len + 1);
                memcpy(err_msg, err_start, err_len);
                err_msg[err_len] = '\0';
                fn_free_string(result);
                zend_throw_exception(spl_ce_RuntimeException, err_msg, 0);
                efree(err_msg);
                RETURN_THROWS();
            }
        }
        fn_free_string(result);
        RETURN_LONG(-1);
    }

    fn_free_string(result);
    RETURN_LONG(affected);
}

/* ── pyrosql_copy_out(conn, sql): string ─────────────────────────── */
PHP_FUNCTION(pyrosql_copy_out)
{
    zval *zconn;
    zend_string *sql;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
        Z_PARAM_STR(sql)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_conn *conn = get_conn_from_zval(zconn, "pyrosql_copy_out");
    if (!conn) RETURN_THROWS();

    if (!fn_copy_out) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_copy_out(): FFI function pyro_pwire_copy_out not available", 0);
        RETURN_THROWS();
    }

    char *result = fn_copy_out(conn->handle, ZSTR_VAL(sql));
    if (!result) {
        RETURN_EMPTY_STRING();
    }

    zend_string *ret = zend_string_init(result, strlen(result), 0);
    fn_free_string(result);
    RETURN_STR(ret);
}

/* ── pyrosql_watch(conn, sql): string ────────────────────────────── */
PHP_FUNCTION(pyrosql_watch)
{
    zval *zconn;
    zend_string *sql;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
        Z_PARAM_STR(sql)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_conn *conn = get_conn_from_zval(zconn, "pyrosql_watch");
    if (!conn) RETURN_THROWS();

    if (!fn_watch) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_watch(): FFI function pyro_pwire_watch not available", 0);
        RETURN_THROWS();
    }

    char *result = fn_watch(conn->handle, ZSTR_VAL(sql));
    if (!result) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_watch(): Failed to create watch", 0);
        RETURN_THROWS();
    }

    zend_string *ret = zend_string_init(result, strlen(result), 0);
    fn_free_string(result);
    RETURN_STR(ret);
}

/* ── pyrosql_unwatch(conn, channel): bool ────────────────────────── */
PHP_FUNCTION(pyrosql_unwatch)
{
    zval *zconn;
    zend_string *channel;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
        Z_PARAM_STR(channel)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_conn *conn = get_conn_from_zval(zconn, "pyrosql_unwatch");
    if (!conn) RETURN_THROWS();

    if (!fn_unwatch) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_unwatch(): FFI function pyro_pwire_unwatch not available", 0);
        RETURN_THROWS();
    }

    int32_t result = fn_unwatch(conn->handle, ZSTR_VAL(channel));
    RETURN_BOOL(result != 0);
}

/* ── pyrosql_subscribe_cdc(conn, table): string ──────────────────── */
PHP_FUNCTION(pyrosql_subscribe_cdc)
{
    zval *zconn;
    zend_string *table;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
        Z_PARAM_STR(table)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_conn *conn = get_conn_from_zval(zconn, "pyrosql_subscribe_cdc");
    if (!conn) RETURN_THROWS();

    if (!fn_subscribe_cdc) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_subscribe_cdc(): FFI function pyro_pwire_subscribe_cdc not available", 0);
        RETURN_THROWS();
    }

    char *result = fn_subscribe_cdc(conn->handle, ZSTR_VAL(table));
    if (!result) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_subscribe_cdc(): Failed to subscribe to CDC", 0);
        RETURN_THROWS();
    }

    zend_string *ret = zend_string_init(result, strlen(result), 0);
    fn_free_string(result);
    RETURN_STR(ret);
}

/* ── pyrosql_query_cursor(conn, sql): PyroSqlCursor ──────────────── */
PHP_FUNCTION(pyrosql_query_cursor)
{
    zval *zconn;
    zend_string *sql;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
        Z_PARAM_STR(sql)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_conn *conn = get_conn_from_zval(zconn, "pyrosql_query_cursor");
    if (!conn) RETURN_THROWS();

    if (!fn_query_cursor) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_query_cursor(): FFI function pyro_pwire_query_cursor not available", 0);
        RETURN_THROWS();
    }

    char *cursor_id = fn_query_cursor(conn->handle, ZSTR_VAL(sql));
    if (!cursor_id) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_query_cursor(): Failed to create cursor", 0);
        RETURN_THROWS();
    }

    object_init_ex(return_value, pyrosql_cursor_ce);
    pyrosql_cursor_obj *cursor_obj = Z_PYROSQL_CURSOR_P(return_value);
    cursor_obj->conn = conn;
    cursor_obj->cursor_id = estrdup(cursor_id);
    cursor_obj->closed = 0;
    fn_free_string(cursor_id);
}

/* ── pyrosql_cursor_next(PyroSqlCursor $cursor): ?array ──────────── */
PHP_FUNCTION(pyrosql_cursor_next)
{
    zval *zcursor;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_OBJECT_OF_CLASS(zcursor, pyrosql_cursor_ce)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_cursor_obj *cursor_obj = Z_PYROSQL_CURSOR_P(zcursor);

    if (cursor_obj->closed) {
        RETURN_NULL();
    }

    if (!cursor_obj->conn || !cursor_obj->conn->handle) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_cursor_next(): Connection is closed", 0);
        RETURN_THROWS();
    }

    if (!fn_cursor_next) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_cursor_next(): FFI function pyro_pwire_cursor_next not available", 0);
        RETURN_THROWS();
    }

    char *row_json = fn_cursor_next(cursor_obj->conn->handle, cursor_obj->cursor_id);
    if (!row_json) {
        cursor_obj->closed = 1;
        RETURN_NULL();
    }

    /* Check for empty/end-of-cursor marker */
    if (strlen(row_json) == 0 || strcmp(row_json, "null") == 0) {
        fn_free_string(row_json);
        cursor_obj->closed = 1;
        RETURN_NULL();
    }

    /* Decode JSON row into PHP array */
    php_json_decode(return_value, row_json, strlen(row_json), 1, PHP_JSON_PARSER_DEFAULT_DEPTH);
    fn_free_string(row_json);

    if (Z_TYPE_P(return_value) == IS_NULL) {
        cursor_obj->closed = 1;
    }
}

/* ── pyrosql_bulk_insert(conn, table, json_rows): int ────────────── */
PHP_FUNCTION(pyrosql_bulk_insert)
{
    zval *zconn;
    zend_string *table;
    zend_string *json_rows;

    ZEND_PARSE_PARAMETERS_START(3, 3)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
        Z_PARAM_STR(table)
        Z_PARAM_STR(json_rows)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_conn *conn = get_conn_from_zval(zconn, "pyrosql_bulk_insert");
    if (!conn) RETURN_THROWS();

    if (!fn_bulk_insert) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_bulk_insert(): FFI function pyro_pwire_bulk_insert not available", 0);
        RETURN_THROWS();
    }

    char *result = fn_bulk_insert(conn->handle, ZSTR_VAL(table), ZSTR_VAL(json_rows));
    if (!result) {
        RETURN_LONG(-1);
    }

    /* Check for error */
    if (strstr(result, "\"error\"")) {
        const char *err_start = strstr(result, "\"error\":\"");
        if (err_start) {
            err_start += 9;
            const char *err_end = strchr(err_start, '"');
            if (err_end) {
                size_t err_len = err_end - err_start;
                char *err_msg = emalloc(err_len + 1);
                memcpy(err_msg, err_start, err_len);
                err_msg[err_len] = '\0';
                fn_free_string(result);
                zend_throw_exception(spl_ce_RuntimeException, err_msg, 0);
                efree(err_msg);
                RETURN_THROWS();
            }
        }
        fn_free_string(result);
        RETURN_LONG(-1);
    }

    int64_t affected = parse_rows_affected_from_json(result);
    fn_free_string(result);
    RETURN_LONG(affected);
}

/* ── pyrosql_batch_execute(conn, sql, params_json): string ──────────
 *
 * Packs N EXECUTEs of the same prepared `sql` template into ONE
 * `MSG_BATCH_EXECUTE` wire frame (opcode 0x07). Returns the server's
 * N `RESP_RESULT_SET` responses as a single JSON array.
 *
 * `params_json` must be a JSON array of arrays of scalars, one inner
 * array per invocation. Every parameter is passed as a string at the
 * wire level; the server decodes per the prepared template column
 * types (`"42"` → `INT 42`, etc.). The FFI also accepts JSON numbers
 * and booleans as a convenience and stringifies them before the wire
 * write — callers can skip the manual `"quote everything"` step.
 *
 * Return: JSON array string. Example with 2 invocations of
 * `SELECT v FROM t WHERE id = $1`:
 *
 *   [{"columns":["v"],"rows":[[42]],"rows_affected":0},
 *    {"columns":["v"],"rows":[[84]],"rows_affected":0}]
 *
 * On fatal error (PREPARE failed, invalid JSON, broken connection):
 * throws RuntimeException with the server/transport error message.
 */
PHP_FUNCTION(pyrosql_batch_execute)
{
    zval *zconn;
    zend_string *sql;
    zend_string *params_json;

    ZEND_PARSE_PARAMETERS_START(3, 3)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
        Z_PARAM_STR(sql)
        Z_PARAM_STR(params_json)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_conn *conn = get_conn_from_zval(zconn, "pyrosql_batch_execute");
    if (!conn) RETURN_THROWS();

    if (!fn_batch_execute) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_batch_execute(): FFI symbol pyro_pwire_batch_execute not "
            "available — rebuild libpyrosql_ffi_pwire.so (v1.4+) with the "
            "MSG_BATCH_EXECUTE implementation", 0);
        RETURN_THROWS();
    }

    char *result = fn_batch_execute(conn->handle, ZSTR_VAL(sql), ZSTR_VAL(params_json));
    if (!result) {
        zend_throw_exception(spl_ce_RuntimeException,
            "pyrosql_batch_execute: null return from FFI (connection broken?)", 0);
        RETURN_THROWS();
    }

    /* Fatal-error detection. The FFI layer returns a JSON OBJECT
     * ({"error":"..."}) on transport / prepare failure, but a JSON
     * ARRAY ([...]) on success. Peek at the first non-whitespace
     * character to decide. */
    const char *p = result;
    while (*p == ' ' || *p == '\t' || *p == '\n') p++;
    if (*p == '{') {
        /* Error object — extract message and throw. */
        const char *err_start = strstr(result, "\"error\":\"");
        if (err_start) {
            err_start += 9;
            const char *err_end = strchr(err_start, '"');
            if (err_end) {
                size_t err_len = err_end - err_start;
                char *err_msg = emalloc(err_len + 1);
                memcpy(err_msg, err_start, err_len);
                err_msg[err_len] = '\0';
                fn_free_string(result);
                zend_throw_exception(spl_ce_RuntimeException, err_msg, 0);
                efree(err_msg);
                RETURN_THROWS();
            }
        }
        /* Malformed error object — surface the raw JSON. */
        zend_string *ret = zend_string_init(result, strlen(result), 0);
        fn_free_string(result);
        zend_throw_exception(spl_ce_RuntimeException, ZSTR_VAL(ret), 0);
        zend_string_release(ret);
        RETURN_THROWS();
    }

    zend_string *ret = zend_string_init(result, strlen(result), 0);
    fn_free_string(result);
    RETURN_STR(ret);
}

/* ── pyrosql_ping(conn): bool ────────────────────────────────────── */
PHP_FUNCTION(pyrosql_ping)
{
    zval *zconn;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_conn *conn = get_conn_from_zval(zconn, "pyrosql_ping");
    if (!conn) RETURN_THROWS();

    if (!fn_ping) {
        RETURN_FALSE;
    }

    RETURN_BOOL(fn_ping(conn->handle) != 0);
}

/* ── pyrosql_close(conn): void ───────────────────────────────────── */
PHP_FUNCTION(pyrosql_close)
{
    zval *zconn;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_OBJECT_OF_CLASS(zconn, pyrosql_connection_ce)
    ZEND_PARSE_PARAMETERS_END();

    pyrosql_connection_obj *obj = Z_PYROSQL_CONN_P(zconn);

    if (!obj->conn || !obj->conn->handle) {
        return; /* Already closed */
    }

    if (obj->owns_handle) {
        /* We own it, close the FFI handle */
        if (fn_close) {
            fn_close(obj->conn->handle);
        }
        obj->conn->handle = NULL;
    } else {
        /* Borrowed from PDO — just detach, let PDO handle cleanup */
        obj->conn = NULL;
    }
}

/* ── Argument info for native functions ────────────────────────────── */
ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_pyrosql_from_pdo, 0, 1, PyroSqlConnection, 0)
    ZEND_ARG_OBJ_INFO(0, pdo, PDO, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_listen, 0, 2, _IS_BOOL, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
    ZEND_ARG_TYPE_INFO(0, channel, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_unlisten, 0, 2, _IS_BOOL, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
    ZEND_ARG_TYPE_INFO(0, channel, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_notify, 0, 3, _IS_BOOL, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
    ZEND_ARG_TYPE_INFO(0, channel, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, payload, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_on_notification, 0, 2, IS_VOID, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
    ZEND_ARG_CALLABLE_INFO(0, callback, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_copy_in, 0, 4, IS_LONG, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
    ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, columns_json, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, csv_data, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_copy_out, 0, 2, IS_STRING, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_watch, 0, 2, IS_STRING, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_unwatch, 0, 2, _IS_BOOL, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
    ZEND_ARG_TYPE_INFO(0, channel, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_subscribe_cdc, 0, 2, IS_STRING, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
    ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_pyrosql_query_cursor, 0, 2, PyroSqlCursor, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_cursor_next, 0, 1, IS_ARRAY, 1)
    ZEND_ARG_OBJ_INFO(0, cursor, PyroSqlCursor, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_bulk_insert, 0, 3, IS_LONG, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
    ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, json_rows, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_batch_execute, 0, 3, IS_STRING, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, params_json, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_ping, 0, 1, _IS_BOOL, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pyrosql_close, 0, 1, IS_VOID, 0)
    ZEND_ARG_OBJ_INFO(0, conn, PyroSqlConnection, 0)
ZEND_END_ARG_INFO()

/* ── Function entry table ──────────────────────────────────────────── */
const zend_function_entry pyrosql_native_functions[] = {
    PHP_FE(pyrosql_from_pdo,         arginfo_pyrosql_from_pdo)
    PHP_FE(pyrosql_listen,           arginfo_pyrosql_listen)
    PHP_FE(pyrosql_unlisten,         arginfo_pyrosql_unlisten)
    PHP_FE(pyrosql_notify,           arginfo_pyrosql_notify)
    PHP_FE(pyrosql_on_notification,  arginfo_pyrosql_on_notification)
    PHP_FE(pyrosql_copy_in,          arginfo_pyrosql_copy_in)
    PHP_FE(pyrosql_copy_out,         arginfo_pyrosql_copy_out)
    PHP_FE(pyrosql_watch,            arginfo_pyrosql_watch)
    PHP_FE(pyrosql_unwatch,          arginfo_pyrosql_unwatch)
    PHP_FE(pyrosql_subscribe_cdc,    arginfo_pyrosql_subscribe_cdc)
    PHP_FE(pyrosql_query_cursor,     arginfo_pyrosql_query_cursor)
    PHP_FE(pyrosql_cursor_next,      arginfo_pyrosql_cursor_next)
    PHP_FE(pyrosql_bulk_insert,      arginfo_pyrosql_bulk_insert)
    PHP_FE(pyrosql_batch_execute,    arginfo_pyrosql_batch_execute)
    PHP_FE(pyrosql_ping,             arginfo_pyrosql_ping)
    PHP_FE(pyrosql_close,            arginfo_pyrosql_close)
    PHP_FE_END
};

/* ── Register native classes ───────────────────────────────────────── */
void pyrosql_register_native_classes(void)
{
    zend_class_entry ce;

    /* PyroSqlConnection */
    INIT_CLASS_ENTRY(ce, "PyroSqlConnection", NULL);
    pyrosql_connection_ce = zend_register_internal_class(&ce);
    pyrosql_connection_ce->create_object = pyrosql_connection_create;
    pyrosql_connection_ce->ce_flags |= ZEND_ACC_FINAL | ZEND_ACC_NO_DYNAMIC_PROPERTIES;

    memcpy(&pyrosql_connection_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    pyrosql_connection_handlers.offset = XtOffsetOf(pyrosql_connection_obj, std);
    pyrosql_connection_handlers.free_obj = pyrosql_connection_free;
    pyrosql_connection_handlers.clone_obj = NULL;

    /* PyroSqlCursor */
    INIT_CLASS_ENTRY(ce, "PyroSqlCursor", NULL);
    pyrosql_cursor_ce = zend_register_internal_class(&ce);
    pyrosql_cursor_ce->create_object = pyrosql_cursor_create;
    pyrosql_cursor_ce->ce_flags |= ZEND_ACC_FINAL | ZEND_ACC_NO_DYNAMIC_PROPERTIES;

    memcpy(&pyrosql_cursor_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    pyrosql_cursor_handlers.offset = XtOffsetOf(pyrosql_cursor_obj, std);
    pyrosql_cursor_handlers.free_obj = pyrosql_cursor_free;
    pyrosql_cursor_handlers.clone_obj = NULL;
}

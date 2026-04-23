/* pyrosql — Unified PHP extension providing both PDO driver and native functions
 * for PyroSQL via PWire protocol.
 *
 * Links against libpyrosql_ffi_pwire.so which implements the binary
 * PWire protocol over TCP.
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

#include <dlfcn.h>
#include <string.h>

/* ── Function pointers loaded from libpyrosql_ffi_pwire.so ──────────── */
void  (*fn_init)(void) = NULL;
void* (*fn_connect)(const char *host, uint16_t port) = NULL;
char* (*fn_query)(void *h, const char *sql) = NULL;
int64_t (*fn_execute)(void *h, const char *sql) = NULL;
char* (*fn_begin)(void *h) = NULL;
int32_t (*fn_commit)(void *h, const char *tx_id) = NULL;
int32_t (*fn_rollback)(void *h, const char *tx_id) = NULL;
char* (*fn_prepare)(void *h, const char *sql) = NULL;
char* (*fn_execute_prepared)(void *h, const char *prepared_json, const char *params_json) = NULL;
void  (*fn_free_string)(char *s) = NULL;
void  (*fn_close)(void *h) = NULL;
void  (*fn_shutdown)(void) = NULL;
int32_t (*fn_ping)(void *h) = NULL;

/* Native FFI function pointers */
int32_t (*fn_listen)(void *h, const char *channel) = NULL;
int32_t (*fn_unlisten)(void *h, const char *channel) = NULL;
int32_t (*fn_notify)(void *h, const char *channel, const char *payload) = NULL;
char*   (*fn_get_notification)(void *h) = NULL;
char*   (*fn_copy_in)(void *h, const char *table, const char *columns_json, const char *csv_data) = NULL;
char*   (*fn_copy_out)(void *h, const char *sql) = NULL;
char*   (*fn_watch)(void *h, const char *sql) = NULL;
int32_t (*fn_unwatch)(void *h, const char *channel) = NULL;
char*   (*fn_subscribe_cdc)(void *h, const char *table) = NULL;
char*   (*fn_query_cursor)(void *h, const char *sql) = NULL;
char*   (*fn_cursor_next)(void *h, const char *cursor_id) = NULL;
char*   (*fn_cursor_close)(void *h, const char *cursor_id) = NULL;
char*   (*fn_bulk_insert)(void *h, const char *table, const char *json_rows) = NULL;
char*   (*fn_batch_execute)(void *h, const char *sql, const char *params_json) = NULL;

static void *lib_handle = NULL;

/* ── Load the shared library ────────────────────────────────────────── */
int pyrosql_load_lib(void)
{
    if (lib_handle) return 1;

    const char *paths[] = {
        "libpyrosql_ffi_pwire.so",
        "/usr/lib/libpyrosql_ffi_pwire.so",
        "/usr/local/lib/libpyrosql_ffi_pwire.so",
        NULL
    };

    for (int i = 0; paths[i]; i++) {
        lib_handle = dlopen(paths[i], RTLD_LAZY);
        if (lib_handle) break;
    }

    if (!lib_handle) {
        php_error_docref(NULL, E_WARNING, "pyrosql: cannot load libpyrosql_ffi_pwire.so: %s", dlerror());
        return 0;
    }

#define LOAD_SYM(name, type) \
    fn_##name = (type)dlsym(lib_handle, "pyro_pwire_" #name); \
    if (!fn_##name) { php_error_docref(NULL, E_WARNING, "pyrosql: symbol pyro_pwire_" #name " not found"); return 0; }

    LOAD_SYM(init, void(*)(void));
    LOAD_SYM(connect, void*(*)(const char*, uint16_t));
    LOAD_SYM(query, char*(*)(void*, const char*));
    LOAD_SYM(execute, int64_t(*)(void*, const char*));
    LOAD_SYM(free_string, void(*)(char*));
    LOAD_SYM(close, void(*)(void*));

#undef LOAD_SYM

    /* Load optional native symbols — these may not exist in all builds */
#define LOAD_SYM_OPT(name, type) \
    fn_##name = (type)dlsym(lib_handle, "pyro_pwire_" #name);

    LOAD_SYM_OPT(begin, char*(*)(void*));
    LOAD_SYM_OPT(commit, int32_t(*)(void*, const char*));
    LOAD_SYM_OPT(rollback, int32_t(*)(void*, const char*));
    LOAD_SYM_OPT(prepare, char*(*)(void*, const char*));
    LOAD_SYM_OPT(execute_prepared, char*(*)(void*, const char*, const char*));
    LOAD_SYM_OPT(shutdown, void(*)(void));
    LOAD_SYM_OPT(ping, int32_t(*)(void*));
    LOAD_SYM_OPT(listen, int32_t(*)(void*, const char*));
    LOAD_SYM_OPT(unlisten, int32_t(*)(void*, const char*));
    LOAD_SYM_OPT(notify, int32_t(*)(void*, const char*, const char*));
    LOAD_SYM_OPT(get_notification, char*(*)(void*));
    LOAD_SYM_OPT(copy_in, char*(*)(void*, const char*, const char*, const char*));
    LOAD_SYM_OPT(copy_out, char*(*)(void*, const char*));
    LOAD_SYM_OPT(watch, char*(*)(void*, const char*));
    LOAD_SYM_OPT(unwatch, int32_t(*)(void*, const char*));
    LOAD_SYM_OPT(subscribe_cdc, char*(*)(void*, const char*));
    LOAD_SYM_OPT(query_cursor, char*(*)(void*, const char*));
    LOAD_SYM_OPT(cursor_next, char*(*)(void*, const char*));
    LOAD_SYM_OPT(cursor_close, char*(*)(void*, const char*));
    LOAD_SYM_OPT(bulk_insert, char*(*)(void*, const char*, const char*));
    LOAD_SYM_OPT(batch_execute, char*(*)(void*, const char*, const char*));

#undef LOAD_SYM_OPT

    fn_init();
    return 1;
}

/* ── PDO driver: handle closer ──────────────────────────────────────── */
static void pyrosql_handle_closer(pdo_dbh_t *dbh)
{
    pyrosql_conn *conn = (pyrosql_conn *)dbh->driver_data;
    if (conn) {
        if (conn->handle && fn_close) {
            fn_close(conn->handle);
        }
        if (conn->dsn_host) efree(conn->dsn_host);
        if (conn->dsn_dbname) efree(conn->dsn_dbname);
        efree(conn);
        dbh->driver_data = NULL;
    }
}

/* ── Statement methods (defined in pyrosql_stmt.c) ──────────────────── */
extern const struct pdo_stmt_methods pyrosql_pdo_stmt_methods;

/* ── PDO driver: prepare statement ──────────────────────────────────── */
static bool pyrosql_handle_preparer(pdo_dbh_t *dbh, zend_string *sql,
                                     pdo_stmt_t *stmt, zval *driver_options)
{
    pyrosql_conn *conn = (pyrosql_conn *)dbh->driver_data;
    pyrosql_stmt *s = ecalloc(1, sizeof(pyrosql_stmt));
    s->conn = conn;
    s->sql = estrndup(ZSTR_VAL(sql), ZSTR_LEN(sql));

    stmt->driver_data = s;
    stmt->methods = &pyrosql_pdo_stmt_methods;
    stmt->supports_placeholders = PDO_PLACEHOLDER_POSITIONAL;

    return true;
}

/* ── PDO driver: direct execute ─────────────────────────────────────── */
static zend_long pyrosql_handle_doer(pdo_dbh_t *dbh, const zend_string *sql)
{
    pyrosql_conn *conn = (pyrosql_conn *)dbh->driver_data;
    int64_t affected = fn_execute(conn->handle, ZSTR_VAL(sql));
    return (zend_long)affected;
}

/* ── PDO driver: last insert id ─────────────────────────────────────── */
static zend_string *pyrosql_handle_last_id(pdo_dbh_t *dbh, const zend_string *name)
{
    pyrosql_conn *conn = (pyrosql_conn *)dbh->driver_data;

    /* Try to retrieve last inserted id via lastval() */
    char *json = fn_query(conn->handle, "SELECT lastval()");
    if (json) {
        /* Parse minimal JSON: {"columns":["lastval"],"rows":[[<id>]]} */
        const char *rows_key = strstr(json, "\"rows\"");
        if (rows_key) {
            const char *p = strchr(rows_key, '[');
            if (p) {
                p++; /* skip outer [ */
                p = strchr(p, '[');
                if (p) {
                    p++; /* skip inner [ */
                    char buf[64];
                    int i = 0;
                    if (*p == '"') {
                        p++;
                        while (*p && *p != '"' && i < 63) buf[i++] = *p++;
                    } else {
                        while (*p && *p != ']' && *p != ',' && i < 63) buf[i++] = *p++;
                    }
                    buf[i] = '\0';
                    fn_free_string(json);
                    if (i > 0) {
                        return zend_string_init(buf, i, 0);
                    }
                }
            }
        }
        fn_free_string(json);
    }

    return zend_string_init("0", 1, 0);
}

/* ── PDO driver: begin/commit/rollback ──────────────────────────────── */
static bool pyrosql_handle_begin(pdo_dbh_t *dbh)
{
    pyrosql_conn *conn = (pyrosql_conn *)dbh->driver_data;
    if (fn_begin) {
        char *tx_id = fn_begin(conn->handle);
        if (tx_id) fn_free_string(tx_id);
    } else {
        char *r = fn_query(conn->handle, "BEGIN");
        if (r) fn_free_string(r);
    }
    conn->in_transaction = 1;
    return true;
}

static bool pyrosql_handle_commit(pdo_dbh_t *dbh)
{
    pyrosql_conn *conn = (pyrosql_conn *)dbh->driver_data;
    if (fn_commit) {
        fn_commit(conn->handle, "");
    } else {
        char *r = fn_query(conn->handle, "COMMIT");
        if (r) fn_free_string(r);
    }
    conn->in_transaction = 0;
    return true;
}

static bool pyrosql_handle_rollback(pdo_dbh_t *dbh)
{
    pyrosql_conn *conn = (pyrosql_conn *)dbh->driver_data;
    if (fn_rollback) {
        fn_rollback(conn->handle, "");
    } else {
        char *r = fn_query(conn->handle, "ROLLBACK");
        if (r) fn_free_string(r);
    }
    conn->in_transaction = 0;
    return true;
}

/* ── PDO driver: set/get attribute ──────────────────────────────────── */
static bool pyrosql_handle_set_attr(pdo_dbh_t *dbh, zend_long attr, zval *val)
{
    return false;
}

static int pyrosql_handle_get_attr(pdo_dbh_t *dbh, zend_long attr, zval *return_value)
{
    switch (attr) {
        case PDO_ATTR_SERVER_VERSION:
            ZVAL_STRING(return_value, "PyroSQL 1.0 (PWire)");
            return 1;
        case PDO_ATTR_CLIENT_VERSION:
            ZVAL_STRING(return_value, PYROSQL_VERSION);
            return 1;
        case PDO_ATTR_DRIVER_NAME:
            ZVAL_STRING(return_value, "pyrosql");
            return 1;
    }
    return 0;
}

/* ── PDO driver: quoter ─────────────────────────────────────────────── */
static zend_string *pyrosql_handle_quoter(pdo_dbh_t *dbh, const zend_string *unquoted,
                                           enum pdo_param_type paramtype)
{
    const char *s = ZSTR_VAL(unquoted);
    size_t len = ZSTR_LEN(unquoted);

    char *quoted = emalloc(2 * len + 3);
    char *p = quoted;
    *p++ = '\'';
    for (size_t i = 0; i < len; i++) {
        if (s[i] == '\0') {
            *p++ = '\\';
            *p++ = '0';
        } else if (s[i] == '\\') {
            *p++ = '\\';
            *p++ = '\\';
        } else if (s[i] == '\'') {
            *p++ = '\'';
            *p++ = '\'';
        } else {
            *p++ = s[i];
        }
    }
    *p++ = '\'';
    *p = '\0';

    zend_string *result = zend_string_init(quoted, p - quoted, 0);
    efree(quoted);
    return result;
}

/* ── PDO driver: fetch_err ─────────────────────────────────────────── */
static void pyrosql_handle_fetch_err(pdo_dbh_t *dbh, pdo_stmt_t *stmt, zval *info)
{
    pyrosql_conn *conn = dbh ? (pyrosql_conn *)dbh->driver_data : NULL;
    if (!conn) return;

    if (conn->last_error_msg[0]) {
        /* PDO expects exactly 2 elements: native error code (long) and message (string).
         * PDO itself prepends the SQLSTATE as element [0]. */
        add_next_index_long(info, (zend_long)conn->last_error_code);
        add_next_index_string(info, conn->last_error_msg);
    }
}

/* ── PDO driver: check_liveness ────────────────────────────────────── */
static int pyrosql_handle_check_liveness(pdo_dbh_t *dbh)
{
    pyrosql_conn *conn = (pyrosql_conn *)dbh->driver_data;
    if (!conn || !conn->handle) {
        return FAILURE;
    }
    if (fn_ping) {
        return fn_ping(conn->handle) ? SUCCESS : FAILURE;
    }
    /* Fallback: try a simple query */
    char *r = fn_query(conn->handle, "SELECT 1");
    if (r) { fn_free_string(r); return SUCCESS; }
    return FAILURE;
}

/* ── PDO driver: in_transaction ────────────────────────────────────── */
static bool pyrosql_handle_in_transaction(pdo_dbh_t *dbh)
{
    pyrosql_conn *conn = (pyrosql_conn *)dbh->driver_data;
    return conn->in_transaction ? true : false;
}

/* ── PDO driver method table ────────────────────────────────────────── */
static const struct pdo_dbh_methods pyrosql_methods = {
    pyrosql_handle_closer,
    pyrosql_handle_preparer,
    pyrosql_handle_doer,
    pyrosql_handle_quoter,
    pyrosql_handle_begin,
    pyrosql_handle_commit,
    pyrosql_handle_rollback,
    pyrosql_handle_set_attr,
    pyrosql_handle_last_id,
    pyrosql_handle_fetch_err,
    pyrosql_handle_get_attr,
    pyrosql_handle_check_liveness,
    NULL, /* get_driver_methods */
    NULL, /* request_shutdown */
    pyrosql_handle_in_transaction,
    NULL  /* get_gc */
};

/* ── PDO driver: factory (handles DSN parsing + connect) ────────────── */
static int pyrosql_handle_factory(pdo_dbh_t *dbh, zval *driver_options)
{
    if (!pyrosql_load_lib()) {
        return 0;
    }

    pyrosql_conn *conn = ecalloc(1, sizeof(pyrosql_conn));

    /* Parse DSN: pyrosql:host=xxx;port=12520;dbname=yyy */
    const char *dsn = dbh->data_source;
    char host[256] = "127.0.0.1";
    int port = 12520;
    char dbname[256] = "";

    if (dsn) {
        const char *p;
        if ((p = strstr(dsn, "host=")) != NULL) {
            sscanf(p + 5, "%255[^;]", host);
        }
        if ((p = strstr(dsn, "port=")) != NULL) {
            sscanf(p + 5, "%d", &port);
        }
        if ((p = strstr(dsn, "dbname=")) != NULL) {
            sscanf(p + 7, "%255[^;]", dbname);
        }
    }

    conn->dsn_host = estrdup(host);
    conn->dsn_port = port;
    conn->dsn_dbname = estrdup(dbname);
    conn->in_transaction = 0;
    conn->last_error_msg[0] = '\0';
    conn->last_error_sqlstate[0] = '\0';
    conn->last_error_code = 0;

    conn->handle = fn_connect(host, (uint16_t)port);
    if (!conn->handle) {
        efree(conn->dsn_host);
        efree(conn->dsn_dbname);
        efree(conn);
        pdo_throw_exception(0, "pyrosql: connection failed", NULL);
        return 0;
    }

    dbh->driver_data = conn;
    dbh->methods = &pyrosql_methods;
    dbh->alloc_own_columns = 1;

    return 1;
}

/* ── PDO driver definition ──────────────────────────────────────────── */
static const pdo_driver_t pyrosql_pdo_driver = {
    PDO_DRIVER_HEADER(pyrosql),
    pyrosql_handle_factory
};

/* ── Native function entries (defined in pyrosql_native.c) ─────────── */
extern const zend_function_entry pyrosql_native_functions[];

/* ── Module init/shutdown ───────────────────────────────────────────── */
PHP_MINIT_FUNCTION(pyrosql)
{
    /* Register PDO driver */
    if (php_pdo_register_driver(&pyrosql_pdo_driver) != SUCCESS) {
        return FAILURE;
    }

    /* Register native classes (PyroSqlConnection, PyroSqlCursor) */
    pyrosql_register_native_classes();

    return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(pyrosql)
{
    php_pdo_unregister_driver(&pyrosql_pdo_driver);
    if (fn_shutdown) fn_shutdown();
    if (lib_handle) { dlclose(lib_handle); lib_handle = NULL; }
    return SUCCESS;
}

PHP_MINFO_FUNCTION(pyrosql)
{
    php_info_print_table_start();
    php_info_print_table_header(2, "PyroSQL Extension", "enabled");
    php_info_print_table_row(2, "Version", PYROSQL_VERSION);
    php_info_print_table_row(2, "PDO Driver", "pyrosql");
    php_info_print_table_row(2, "Native Functions", "enabled");
    php_info_print_table_row(2, "Protocol", "PWire binary TCP");
    php_info_print_table_end();
}

/* ── Module entry ───────────────────────────────────────────────────── */
zend_module_entry pyrosql_module_entry = {
    STANDARD_MODULE_HEADER,
    "pyrosql",
    pyrosql_native_functions,
    PHP_MINIT(pyrosql),
    PHP_MSHUTDOWN(pyrosql),
    NULL,
    NULL,
    PHP_MINFO(pyrosql),
    PYROSQL_VERSION,
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_PYROSQL
ZEND_GET_MODULE(pyrosql)
#endif

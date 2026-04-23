#ifndef PHP_PYROSQL_H
#define PHP_PYROSQL_H

#define PYROSQL_VERSION "1.1.0"

extern zend_module_entry pyrosql_module_entry;
#define phpext_pyrosql_ptr &pyrosql_module_entry

/* FFI handle from libpyrosql_ffi_pwire */
typedef struct {
    void *handle;       /* pyro_pwire_connect() handle */
    char *dsn_host;
    int dsn_port;
    char *dsn_dbname;
    int in_transaction;
    char last_error_msg[1024];
    char last_error_sqlstate[6];
    int last_error_code;
} pyrosql_conn;

typedef struct {
    pyrosql_conn *conn;
    char *sql;
    char *result_json;   /* JSON result from pyro_pwire_query */
    char *prepared_json; /* JSON from pyro_pwire_prepare */
    int col_count;
    char **col_names;
    int row_count;
    int current_row;
    char ***rows;        /* rows[row][col] as strings */
    int **nulls;         /* nulls[row][col] */
} pyrosql_stmt;

/* ── FFI function pointers (defined in pyrosql_ext.c) ──────────────── */
extern void  (*fn_init)(void);
extern void* (*fn_connect)(const char *host, uint16_t port);
extern char* (*fn_query)(void *h, const char *sql);
extern int64_t (*fn_execute)(void *h, const char *sql);
extern char* (*fn_begin)(void *h);
extern int32_t (*fn_commit)(void *h, const char *tx_id);
extern int32_t (*fn_rollback)(void *h, const char *tx_id);
extern char* (*fn_prepare)(void *h, const char *sql);
extern char* (*fn_execute_prepared)(void *h, const char *prepared_json, const char *params_json);
extern void  (*fn_free_string)(char *s);
extern void  (*fn_close)(void *h);
extern void  (*fn_shutdown)(void);
extern int32_t (*fn_ping)(void *h);

/* Native FFI function pointers (also in pyrosql_ext.c) */
extern int32_t (*fn_listen)(void *h, const char *channel);
extern int32_t (*fn_unlisten)(void *h, const char *channel);
extern int32_t (*fn_notify)(void *h, const char *channel, const char *payload);
extern char*   (*fn_get_notification)(void *h);
extern char*   (*fn_copy_in)(void *h, const char *table, const char *columns_json, const char *csv_data);
extern char*   (*fn_copy_out)(void *h, const char *sql);
extern char*   (*fn_watch)(void *h, const char *sql);
extern int32_t (*fn_unwatch)(void *h, const char *channel);
extern char*   (*fn_subscribe_cdc)(void *h, const char *table);
extern char*   (*fn_query_cursor)(void *h, const char *sql);
extern char*   (*fn_cursor_next)(void *h, const char *cursor_id);
extern char*   (*fn_cursor_close)(void *h, const char *cursor_id);
extern char*   (*fn_bulk_insert)(void *h, const char *table, const char *json_rows);
extern char*   (*fn_batch_execute)(void *h, const char *sql, const char *params_json);

/* Load shared library (defined in pyrosql_ext.c) */
extern int pyrosql_load_lib(void);

/* Native class entries (defined in pyrosql_native.c) */
extern zend_class_entry *pyrosql_connection_ce;
extern zend_class_entry *pyrosql_cursor_ce;

/* Registration functions (defined in pyrosql_native.c) */
void pyrosql_register_native_classes(void);
void pyrosql_register_native_functions(void);

#endif

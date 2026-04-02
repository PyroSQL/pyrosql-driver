/* pdo_pyrosql — Statement implementation.
 *
 * Executes queries via libpyrosql_ffi_pwire and parses the JSON
 * results into PDO rows.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "pdo/php_pdo.h"
#include "pdo/php_pdo_driver.h"
#include "php_pyrosql.h"
#include "ext/json/php_json.h"

#include <string.h>
#include <stdlib.h>

/* FFI function pointers are declared in php_pyrosql.h and defined in pyrosql_ext.c */

/* ── Free result data from a previous execution ────────────────────── */
static void pyrosql_stmt_free_result(pyrosql_stmt *s)
{
    for (int r = 0; r < s->row_count; r++) {
        if (s->rows && s->rows[r]) {
            for (int c = 0; c < s->col_count; c++) {
                if (s->rows[r][c]) efree(s->rows[r][c]);
            }
            efree(s->rows[r]);
        }
        if (s->nulls && s->nulls[r]) efree(s->nulls[r]);
    }
    if (s->rows) { efree(s->rows); s->rows = NULL; }
    if (s->nulls) { efree(s->nulls); s->nulls = NULL; }

    if (s->col_names) {
        for (int c = 0; c < s->col_count; c++) {
            if (s->col_names[c]) efree(s->col_names[c]);
        }
        efree(s->col_names);
        s->col_names = NULL;
    }

    s->col_count = 0;
    s->row_count = 0;
    s->current_row = -1;
}

/* ── Helper: set error on connection from JSON ─────────────────────── */
static void pyrosql_set_error(pyrosql_conn *conn, pdo_dbh_t *dbh,
                               const char *sqlstate, int code, const char *msg)
{
    if (sqlstate) {
        strncpy(conn->last_error_sqlstate, sqlstate, 5);
        conn->last_error_sqlstate[5] = '\0';
        memcpy(dbh->error_code, sqlstate, 5);
        dbh->error_code[5] = '\0';
    } else {
        strcpy(conn->last_error_sqlstate, "HY000");
        memcpy(dbh->error_code, "HY000", 6);
    }
    conn->last_error_code = code;
    if (msg) {
        strncpy(conn->last_error_msg, msg, sizeof(conn->last_error_msg) - 1);
        conn->last_error_msg[sizeof(conn->last_error_msg) - 1] = '\0';
    } else {
        conn->last_error_msg[0] = '\0';
    }
}

/* ── Dynamic string buffer ──────────────────────────────────────────── */
typedef struct {
    char *buf;
    size_t len;
    size_t cap;
} dyn_str;

static void dyn_init(dyn_str *d, size_t cap) {
    d->buf = emalloc(cap);
    d->len = 0;
    d->cap = cap;
}

static void dyn_ensure(dyn_str *d, size_t extra) {
    if (d->len + extra >= d->cap) {
        d->cap = (d->len + extra) * 2;
        d->buf = erealloc(d->buf, d->cap);
    }
}

static void dyn_appendc(dyn_str *d, char c) {
    dyn_ensure(d, 1);
    d->buf[d->len++] = c;
}

static void dyn_appends(dyn_str *d, const char *s) {
    size_t slen = strlen(s);
    dyn_ensure(d, slen);
    memcpy(d->buf + d->len, s, slen);
    d->len += slen;
}

static void dyn_terminate(dyn_str *d) {
    dyn_ensure(d, 1);
    d->buf[d->len] = '\0';
}

static void dyn_free(dyn_str *d) {
    if (d->buf) efree(d->buf);
    d->buf = NULL;
    d->len = d->cap = 0;
}

/* ── JSON-escape a string into a buffer ────────────────────────────── */
static void json_escape_to(dyn_str *buf, const char *s, size_t len)
{
    dyn_appendc(buf, '"');
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)s[i];
        switch (c) {
            case '"':  dyn_appends(buf, "\\\""); break;
            case '\\': dyn_appends(buf, "\\\\"); break;
            case '\b': dyn_appends(buf, "\\b"); break;
            case '\f': dyn_appends(buf, "\\f"); break;
            case '\n': dyn_appends(buf, "\\n"); break;
            case '\r': dyn_appends(buf, "\\r"); break;
            case '\t': dyn_appends(buf, "\\t"); break;
            default:
                if (c < 0x20) {
                    char esc[8];
                    snprintf(esc, sizeof(esc), "\\u%04x", c);
                    dyn_appends(buf, esc);
                } else {
                    dyn_appendc(buf, c);
                }
        }
    }
    dyn_appendc(buf, '"');
}

/* ── Parse JSON result into stmt arrays ─────────────────────────────── */
static int parse_json_result(pyrosql_stmt *s, pdo_stmt_t *stmt, const char *json)
{
    /* The FFI library returns JSON like:
     * {"columns":["id","name"],"rows":[[1,"foo"],[2,"bar"]]}
     * or for errors:
     * {"error":"message","sqlstate":"42601"}
     */
    zval parsed;
    php_json_decode(&parsed, json, strlen(json), 1, PHP_JSON_PARSER_DEFAULT_DEPTH);

    if (Z_TYPE(parsed) != IS_ARRAY) {
        zval_ptr_dtor(&parsed);
        return 0;
    }

    /* Check for error */
    zval *err = zend_hash_str_find(Z_ARRVAL(parsed), "error", 5);
    if (err && Z_TYPE_P(err) == IS_STRING) {
        const char *sqlstate = NULL;
        int code = 0;
        zval *zstate = zend_hash_str_find(Z_ARRVAL(parsed), "sqlstate", 8);
        if (zstate && Z_TYPE_P(zstate) == IS_STRING) {
            sqlstate = Z_STRVAL_P(zstate);
        }
        zval *zcode = zend_hash_str_find(Z_ARRVAL(parsed), "code", 4);
        if (zcode) {
            if (Z_TYPE_P(zcode) == IS_LONG) {
                code = (int)Z_LVAL_P(zcode);
            } else if (Z_TYPE_P(zcode) == IS_STRING) {
                code = atoi(Z_STRVAL_P(zcode));
            }
        }

        {
            const char *state = sqlstate ? sqlstate : "HY000";
            pyrosql_set_error(s->conn, stmt->dbh, state, code, Z_STRVAL_P(err));
            memcpy(stmt->error_code, state, 5);
            stmt->error_code[5] = '\0';
        }
        zval_ptr_dtor(&parsed);
        return -1;
    }

    /* Extract columns */
    zval *columns = zend_hash_str_find(Z_ARRVAL(parsed), "columns", 7);
    zval *rows = zend_hash_str_find(Z_ARRVAL(parsed), "rows", 4);

    if (!columns || Z_TYPE_P(columns) != IS_ARRAY) {
        zval_ptr_dtor(&parsed);
        return 0;
    }

    s->col_count = zend_hash_num_elements(Z_ARRVAL_P(columns));
    s->col_names = ecalloc(s->col_count, sizeof(char *));

    int ci = 0;
    zval *col;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(columns), col) {
        if (Z_TYPE_P(col) == IS_STRING) {
            s->col_names[ci] = estrndup(Z_STRVAL_P(col), Z_STRLEN_P(col));
        } else {
            s->col_names[ci] = estrdup("?");
        }
        ci++;
    } ZEND_HASH_FOREACH_END();

    /* Extract rows */
    if (rows && Z_TYPE_P(rows) == IS_ARRAY) {
        s->row_count = zend_hash_num_elements(Z_ARRVAL_P(rows));
        s->rows = ecalloc(s->row_count, sizeof(char **));
        s->nulls = ecalloc(s->row_count, sizeof(int *));

        int ri = 0;
        zval *row;
        ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(rows), row) {
            if (Z_TYPE_P(row) != IS_ARRAY) { ri++; continue; }
            s->rows[ri] = ecalloc(s->col_count, sizeof(char *));
            s->nulls[ri] = ecalloc(s->col_count, sizeof(int));

            int vi = 0;
            zval *val;
            ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(row), val) {
                if (vi >= s->col_count) break;
                if (Z_TYPE_P(val) == IS_NULL) {
                    s->nulls[ri][vi] = 1;
                    s->rows[ri][vi] = NULL;
                } else {
                    s->nulls[ri][vi] = 0;
                    zend_string *str = zval_get_string(val);
                    s->rows[ri][vi] = estrndup(ZSTR_VAL(str), ZSTR_LEN(str));
                    zend_string_release(str);
                }
                vi++;
            } ZEND_HASH_FOREACH_END();
            ri++;
        } ZEND_HASH_FOREACH_END();
    } else {
        s->row_count = 0;
    }

    s->current_row = -1;
    zval_ptr_dtor(&parsed);
    return 1;
}

/* ── Build JSON params array from bound parameters ─────────────────── */
static char *build_params_json(pdo_stmt_t *stmt)
{
    if (!stmt->bound_params) {
        return NULL;
    }

    uint32_t num_params = zend_hash_num_elements(stmt->bound_params);
    if (num_params == 0) {
        return NULL;
    }

    dyn_str buf;
    dyn_init(&buf, 256);
    dyn_appendc(&buf, '[');

    int first = 1;
    zend_ulong idx;
    zend_string *key;
    zval *param_zv;

    ZEND_HASH_FOREACH_KEY_VAL(stmt->bound_params, idx, key, param_zv) {
        struct pdo_bound_param_data *param = (struct pdo_bound_param_data *)Z_PTR_P(param_zv);
        (void)idx;
        (void)key;

        if (!first) {
            dyn_appendc(&buf, ',');
        }
        first = 0;

        zval *value = &param->parameter;
        if (Z_ISREF_P(value)) {
            value = Z_REFVAL_P(value);
        }

        switch (Z_TYPE_P(value)) {
            case IS_NULL:
                dyn_appends(&buf, "null");
                break;
            case IS_LONG: {
                char numbuf[32];
                snprintf(numbuf, sizeof(numbuf), "%ld", Z_LVAL_P(value));
                dyn_appends(&buf, numbuf);
                break;
            }
            case IS_TRUE:
                dyn_appends(&buf, "true");
                break;
            case IS_FALSE:
                dyn_appends(&buf, "false");
                break;
            case IS_DOUBLE: {
                char numbuf[64];
                snprintf(numbuf, sizeof(numbuf), "%.*G", 14, Z_DVAL_P(value));
                dyn_appends(&buf, numbuf);
                break;
            }
            default: {
                zend_string *str = zval_get_string(value);
                json_escape_to(&buf, ZSTR_VAL(str), ZSTR_LEN(str));
                zend_string_release(str);
                break;
            }
        }
    } ZEND_HASH_FOREACH_END();

    dyn_appendc(&buf, ']');
    dyn_terminate(&buf);

    char *result = estrndup(buf.buf, buf.len);
    dyn_free(&buf);
    return result;
}

/* ── Interpolate bound params into SQL (replace ? with quoted values) ── */
static char *interpolate_params(const char *sql, pdo_stmt_t *stmt)
{
    dyn_str buf;
    dyn_init(&buf, strlen(sql) * 2);

    /* Collect params into an ordered array */
    uint32_t num_params = zend_hash_num_elements(stmt->bound_params);
    zval **param_values = ecalloc(num_params, sizeof(zval *));
    uint32_t max_idx = 0;

    zend_ulong idx;
    zend_string *key;
    zval *param_zv;
    ZEND_HASH_FOREACH_KEY_VAL(stmt->bound_params, idx, key, param_zv) {
        (void)key;
        struct pdo_bound_param_data *param = (struct pdo_bound_param_data *)Z_PTR_P(param_zv);
        if (idx < num_params) {
            param_values[idx] = &param->parameter;
            if (idx + 1 > max_idx) max_idx = idx + 1;
        }
    } ZEND_HASH_FOREACH_END();

    uint32_t param_idx = 0;
    int in_string = 0;
    char quote_char = 0;

    for (const char *p = sql; *p; p++) {
        if (in_string) {
            dyn_appendc(&buf, *p);
            if (*p == quote_char) {
                /* Check for escaped quote (double quote) */
                if (*(p + 1) == quote_char) {
                    dyn_appendc(&buf, *(++p));
                } else {
                    in_string = 0;
                }
            }
            continue;
        }

        if (*p == '\'' || *p == '"') {
            in_string = 1;
            quote_char = *p;
            dyn_appendc(&buf, *p);
            continue;
        }

        if (*p == '?' && param_idx < max_idx && param_values[param_idx]) {
            zval *value = param_values[param_idx++];
            if (Z_ISREF_P(value)) {
                value = Z_REFVAL_P(value);
            }

            switch (Z_TYPE_P(value)) {
                case IS_NULL:
                    dyn_appends(&buf, "NULL");
                    break;
                case IS_LONG: {
                    char numbuf[32];
                    snprintf(numbuf, sizeof(numbuf), "%ld", Z_LVAL_P(value));
                    dyn_appends(&buf, numbuf);
                    break;
                }
                case IS_TRUE:
                    dyn_appends(&buf, "true");
                    break;
                case IS_FALSE:
                    dyn_appends(&buf, "false");
                    break;
                case IS_DOUBLE: {
                    char numbuf[64];
                    snprintf(numbuf, sizeof(numbuf), "%.*G", 14, Z_DVAL_P(value));
                    dyn_appends(&buf, numbuf);
                    break;
                }
                default: {
                    /* String value — quote and escape */
                    zend_string *str = zval_get_string(value);
                    dyn_appendc(&buf, '\'');
                    for (size_t i = 0; i < ZSTR_LEN(str); i++) {
                        char c = ZSTR_VAL(str)[i];
                        if (c == '\'') dyn_appendc(&buf, '\''); /* escape ' as '' */
                        if (c == '\\') dyn_appendc(&buf, '\\');
                        dyn_appendc(&buf, c);
                    }
                    dyn_appendc(&buf, '\'');
                    zend_string_release(str);
                    break;
                }
            }
        } else {
            dyn_appendc(&buf, *p);
        }
    }

    dyn_terminate(&buf);
    efree(param_values);

    char *result = estrndup(buf.buf, buf.len);
    dyn_free(&buf);
    return result;
}

/* ── Statement: execute (server-side binding preferred) ─────────────── */
static int pyrosql_stmt_execute(pdo_stmt_t *stmt)
{
    pyrosql_stmt *s = (pyrosql_stmt *)stmt->driver_data;

    /* Free previous results */
    pyrosql_stmt_free_result(s);

    if (s->result_json) {
        fn_free_string(s->result_json);
        s->result_json = NULL;
    }
    if (s->prepared_json) {
        fn_free_string(s->prepared_json);
        s->prepared_json = NULL;
    }

    int has_params = stmt->bound_params && zend_hash_num_elements(stmt->bound_params) > 0;

    /* Prefer server-side binding via fn_prepare + fn_execute_prepared when
     * the FFI symbols are available and we have bound parameters. */
    if (has_params && fn_prepare && fn_execute_prepared) {
        /* PREPARE */
        s->prepared_json = fn_prepare(s->conn->handle, s->sql);
        if (!s->prepared_json) {
            pyrosql_set_error(s->conn, stmt->dbh, "HY000", 0, "PREPARE returned NULL");
            return 0;
        }

        /* Check for error in prepare response */
        if (strstr(s->prepared_json, "\"error\"") != NULL) {
            int rc = parse_json_result(s, stmt, s->prepared_json);
            if (rc < 0) {
                stmt->column_count = 0;
                return 0;
            }
        }

        /* Build params JSON array */
        char *params_json = build_params_json(stmt);
        if (!params_json) {
            params_json = estrndup("[]", 2);
        }

        /* EXECUTE PREPARED */
        s->result_json = fn_execute_prepared(s->conn->handle, s->prepared_json, params_json);
        efree(params_json);

        if (!s->result_json) {
            pyrosql_set_error(s->conn, stmt->dbh, "HY000", 0, "EXECUTE PREPARED returned NULL");
            return 0;
        }
    } else {
        /* Fallback: client-side interpolation */
        char *final_sql = NULL;

        if (has_params) {
            final_sql = interpolate_params(s->sql, stmt);
            if (!final_sql) {
                pyrosql_set_error(s->conn, stmt->dbh, "HY000", 0, "Failed to interpolate parameters");
                return 0;
            }
        }

        s->result_json = fn_query(s->conn->handle, final_sql ? final_sql : s->sql);

        if (final_sql) {
            efree(final_sql);
        }
    }

    if (!s->result_json) {
        pyrosql_set_error(s->conn, stmt->dbh, "HY000", 0, "Query returned NULL");
        return 0;
    }

    int rc = parse_json_result(s, stmt, s->result_json);
    if (rc < 0) {
        stmt->column_count = 0;
        return 0;
    }
    if (rc == 0) {
        stmt->column_count = 0;
        return 0;
    }

    stmt->column_count = s->col_count;
    stmt->row_count = s->row_count;

    return 1;
}

/* ── Statement: fetch ───────────────────────────────────────────────── */
static int pyrosql_stmt_fetch(pdo_stmt_t *stmt, enum pdo_fetch_orientation ori, zend_long offset)
{
    pyrosql_stmt *s = (pyrosql_stmt *)stmt->driver_data;

    s->current_row++;
    if (s->current_row >= s->row_count) {
        return 0;
    }
    return 1;
}

/* ── Statement: describe column ─────────────────────────────────────── */
static int pyrosql_stmt_describe(pdo_stmt_t *stmt, int colno)
{
    pyrosql_stmt *s = (pyrosql_stmt *)stmt->driver_data;

    if (colno >= s->col_count) return 0;

    stmt->columns[colno] = (struct pdo_column_data){
        .name = zend_string_init(s->col_names[colno], strlen(s->col_names[colno]), 0),
        .maxlen = SIZE_MAX,
        .precision = 0,
    };

    return 1;
}

/* ── Statement: get column value ────────────────────────────────────── */
static int pyrosql_stmt_get_col(pdo_stmt_t *stmt, int colno, zval *result, enum pdo_param_type *type)
{
    pyrosql_stmt *s = (pyrosql_stmt *)stmt->driver_data;

    if (s->current_row < 0 || s->current_row >= s->row_count || colno >= s->col_count) {
        return 0;
    }

    if (s->nulls[s->current_row][colno]) {
        ZVAL_NULL(result);
    } else {
        char *val = s->rows[s->current_row][colno];
        if (val) {
            ZVAL_STRING(result, val);
        } else {
            ZVAL_NULL(result);
        }
    }

    return 1;
}

/* ── Statement: destructor ──────────────────────────────────────────── */
static int pyrosql_stmt_dtor(pdo_stmt_t *stmt)
{
    pyrosql_stmt *s = (pyrosql_stmt *)stmt->driver_data;
    if (!s) return 1;

    if (s->sql) efree(s->sql);
    if (s->result_json && fn_free_string) fn_free_string(s->result_json);
    if (s->prepared_json && fn_free_string) fn_free_string(s->prepared_json);

    pyrosql_stmt_free_result(s);

    efree(s);
    stmt->driver_data = NULL;
    return 1;
}

/* ── Statement: param hook (for bound params) ───────────────────────── */
static int pyrosql_stmt_param_hook(pdo_stmt_t *stmt, struct pdo_bound_param_data *param,
                                    enum pdo_param_event event_type)
{
    return 1;
}

/* ── Statement method table ─────────────────────────────────────────── */
const struct pdo_stmt_methods pyrosql_pdo_stmt_methods = {
    pyrosql_stmt_dtor,
    pyrosql_stmt_execute,
    pyrosql_stmt_fetch,
    pyrosql_stmt_describe,
    pyrosql_stmt_get_col,
    pyrosql_stmt_param_hook,
    NULL, /* set_attr */
    NULL, /* get_attr */
    NULL, /* get_column_meta */
    NULL, /* next_rowset */
    NULL  /* cursor_closer */
};

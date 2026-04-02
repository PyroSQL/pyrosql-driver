PHP_ARG_ENABLE(pyrosql, whether to enable PyroSQL extension,
[  --enable-pyrosql   Enable PyroSQL extension (PDO driver + native functions)])

if test "$PHP_PYROSQL" != "no"; then
  ifdef([PHP_CHECK_PDO_INCLUDES],[
    PHP_CHECK_PDO_INCLUDES
  ],[
    AC_MSG_CHECKING([for PDO includes])
    if test -f $abs_srcdir/include/php/ext/pdo/php_pdo_driver.h; then
      pdo_cv_inc_path=$abs_srcdir/ext
    elif test -f $abs_srcdir/ext/pdo/php_pdo_driver.h; then
      pdo_cv_inc_path=$abs_srcdir/ext
    elif test -f $phpincludedir/ext/pdo/php_pdo_driver.h; then
      pdo_cv_inc_path=$phpincludedir/ext
    else
      AC_MSG_ERROR([Cannot find php_pdo_driver.h.])
    fi
    AC_MSG_RESULT($pdo_cv_inc_path)
  ])

  PHP_NEW_EXTENSION(pyrosql, pyrosql_ext.c pyrosql_stmt.c pyrosql_native.c, $ext_shared,,-I$pdo_cv_inc_path)
  PHP_SUBST(PYROSQL_SHARED_LIBADD)
  PHP_ADD_EXTENSION_DEP(pyrosql, pdo)
fi

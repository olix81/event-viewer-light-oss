AC_PREREQ([2.69])
AC_INIT([event-viewer-light], [0.2.0], [https://github.com/olix81/event-viewer-light/issues])
AC_CONFIG_SRCDIR([main.c])
AC_CONFIG_AUX_DIR([build-aux])
AM_INIT_AUTOMAKE([foreign subdir-objects])

AC_USE_SYSTEM_EXTENSIONS
AC_PROG_CC
AC_CANONICAL_HOST
AC_PATH_PROG([PKG_CONFIG], [pkg-config])

AC_ARG_ENABLE([sqlite],
  [AS_HELP_STRING([--disable-sqlite], [Build without SQLite persistence])],
  [],
  [enable_sqlite=yes])

AC_ARG_WITH([ssl],
  [AS_HELP_STRING([--with-ssl], [Link OpenSSL libraries (off by default; OpenSSL 3 is not GPL-2.0 compatible)])],
  [],
  [with_ssl=no])


have_sqlite=no
SQLITE_LIBS=
if test "x$enable_sqlite" = "xyes"; then
  AC_CHECK_HEADERS([sqlite3.h], [],
    [AC_MSG_ERROR([sqlite3.h is required when SQLite support is enabled. Install the development headers or re-run with --disable-sqlite.])])

  AC_CHECK_LIB([sqlite3], [sqlite3_open],
    [SQLITE_LIBS="-lsqlite3"],
    [AC_MSG_ERROR([sqlite3 library not found; re-run with --disable-sqlite or adjust LDFLAGS.])])

  have_sqlite=yes
fi
AM_CONDITIONAL([WITH_SQLITE], [test "x$have_sqlite" = "xyes"])
AC_SUBST([SQLITE_LIBS])

SSL_LIBS=
have_ssl=no
if test "x$with_ssl" != "xno"; then
  AC_CHECK_LIB([ssl], [SSL_new], [have_ssl=yes], [have_ssl=no])
  if test "x$have_ssl" = "xyes"; then
    AC_CHECK_LIB([crypto], [EVP_sha256], [],
      [have_ssl=no])
  fi

  if test "x$have_ssl" = "xyes"; then
    SSL_LIBS="-lssl -lcrypto"
  else
    AC_MSG_WARN([OpenSSL libraries not found; continuing without SSL linkage.])
  fi
fi
AC_SUBST([SSL_LIBS])

# The vendored mongoose.h defaults MG_TLS to OpenSSL: select the TLS backend
# explicitly so that a build without OpenSSL compiles Mongoose without TLS.
if test "x$have_ssl" = "xyes"; then
  TLS_CPPFLAGS="-DMG_TLS=MG_TLS_OPENSSL"
else
  TLS_CPPFLAGS="-DMG_TLS=MG_TLS_NONE"
fi
AC_SUBST([TLS_CPPFLAGS])

PLATFORM_LIBS=
case "$host_os" in
  mingw*|msys*|cygwin*)
    PLATFORM_LIBS="-lws2_32 -lcrypt32 -lpthread"
    ;;
  *)
    AC_CHECK_LIB([pthread], [pthread_create],
      [PLATFORM_LIBS="$PLATFORM_LIBS -lpthread"],
      [AC_MSG_ERROR([libpthread is required.])])

    AC_CHECK_LIB([m], [cos],
      [PLATFORM_LIBS="$PLATFORM_LIBS -lm"])

    AC_CHECK_LIB([rt], [clock_gettime],
      [PLATFORM_LIBS="$PLATFORM_LIBS -lrt"], [])
    ;;
esac
AC_SUBST([PLATFORM_LIBS])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT

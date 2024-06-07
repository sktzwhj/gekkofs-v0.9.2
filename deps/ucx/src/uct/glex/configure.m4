#
# Copyright (C) Mellanox Technologies Ltd. 2001-2017.  ALL RIGHTS RESERVED.
# See file LICENSE for terms.
#

#
# Check for GLEX support
#
glex_happy="no"

AC_ARG_WITH([glex],
           [AS_HELP_STRING([--with-glex=(DIR)], [Enable the use of GLEX (default is guess).])],
           [], [with_glex=guess])

AS_IF([test "x$with_glex" != "xno"],
    [save_CPPFLAGS="$CPPFLAGS"
     save_CFLAGS="$CFLAGS"
     save_LDFLAGS="$LDFLAGS"

     AS_IF([test ! -z "$with_glex" -a "x$with_glex" != "xyes" -a "x$with_glex" != "xguess"],
            [
            ucx_check_glex_dir="$with_glex"
            AS_IF([test -d "$with_glex/lib"],[libsuff=""],[libsuff="64"])
            ucx_check_glex_libdir="$with_glex/lib$libsuff"
            CPPFLAGS="-I$with_glex/include $save_CPPFLAGS"
            LDFLAGS="-L$ucx_check_glex_libdir $save_LDFLAGS"
            ])
        AS_IF([test ! -z "$with_glex_libdir" -a "x$with_glex_libdir" != "xyes"],
            [ucx_check_glex_libdir="$with_nccl_libdir"
            LDFLAGS="-L$ucx_check_glex_libdir $save_LDFLAGS"])

        AC_CHECK_HEADERS([glex.h],
            [AC_CHECK_LIB([glex] , [glex_open_device],
                           [glex_happy="yes"],
                           [AC_MSG_WARN([GLEX runtime not detected. Disable.])
                            glex_happy="no"])
            ], [glex_happy="no"])

        AS_IF([test "x$glex_happy" == "xyes"],
            [
                AC_DEFINE([HAVE_GLEX], 1, [Enable GLEX support])
                AC_SUBST(GLEX_CPPFLAGS, "-I$ucx_check_glex_dir/include/ ")
                AC_SUBST(GLEX_LDFLAGS, "-L$ucx_check_glex_dir/lib -lglex")
                CFLAGS="$save_CFLAGS $GLEX_CFLAGS"
                CPPFLAGS="$save_CPPFLAGS $GLEX_CPPFLAGS"
                LDFLAGS="$save_LDFLAGS $GLEX_LDFLAGS"
                uct_modules+=":glex"
            ],
            [
                AS_IF([test "x$with_glex" != "xguess"],
                    [AC_MSG_ERROR([glex support is requested but glex packages can't found])],
                    [AC_MSG_WARN([GLEX not found])
                    AC_DEFINE([HAVE_GLEX], [0], [Disable the use of GLEX])])
            ])
    ],
    [AC_MSG_WARN([GLEX was explicitly disabled])
    AC_DEFINE([HAVE_GLEX], [0], [Disable the use of GLEX])])

AM_CONDITIONAL([HAVE_GLEX], [test "x$glex_happy" != xno])
AC_CONFIG_FILES([src/uct/glex/Makefile])

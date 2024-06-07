#
# Copyright (C) Mellanox Technologies Ltd. 2001-2018.  ALL RIGHTS RESERVED.
# See file LICENSE for terms.
#

if HAVE_PMI
EXTRA_CFLAGS		= @PMI_CFLAGS@
EXTRA_LDFLAGS		= @PMI_LIBS@
endif

if HAVE_PMIX
EXTRA_CFLAGS		= @PMIX_CFLAGS@
EXTRA_LDFLAGS		= @PMIX_LIBS@
endif

if HAVE_GLEX

module_LTLIBRARIES      = libuct_glex.la
libuct_glex_la_CFLAGS   = $(BASE_CFLAGS) $(EXTRA_CFLAGS)
libuct_glex_la_CPPFLAGS = $(BASE_CPPFLAGS)
libuct_glex_la_LIBADD   = $(top_builddir)/src/ucs/libucs.la \
                          $(top_builddir)/src/uct/libuct.la
libuct_glex_la_LDFLAGS  = -version-info $(SOVERSION) $(UCT_MODULE_LDFLAGS) $(EXTRA_LDFLAGS)

noinst_HEADERS = \
	glex_md.h \
	glex_iface.h \
	glex_ep.h \
	glex_channel.h \
	glex_def.h

libuct_glex_la_SOURCES = \
    glex_md.c \
    glex_iface.c \
    glex_ep.c \
    glex_channel.c

include $(top_srcdir)/config/module.am

endif

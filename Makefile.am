bin_PROGRAMS = viewer



viewer_SOURCES = \
	main.c \
	cJSON.c \
	mongoose.c

AM_CPPFLAGS = $(TLS_CPPFLAGS)

if WITH_SQLITE
AM_CPPFLAGS += -DUSE_SQLITE=1
endif

AM_CFLAGS = -Wall -Wextra -pedantic -Wno-unused-parameter

viewer_LDADD = \
	$(SSL_LIBS) \
	$(SQLITE_LIBS) \
	$(PLATFORM_LIBS)

all: test-prereqs build

.PHONY: all test-prereqs build builddir clean install

# How verbose shall we be?

V=

# Installation location variables.  These will be overridden by luarocks
# if you install that way.

LUA_DIR=/usr/local
LUA_LIBDIR=$(LUA_DIR)/lib/lua/5.1
LUA_SHAREDIR=$(LUA_DIR)/share/lua/5.1

# Other configuration variables.  These will also be set by luarocks.

#LIBFLAG=-shared
LIBFLAG=-bundle -undefined dynamic_lookup -all_load
BUILD_DIR=build
CFLAGS= -g -O2 -Wall -Werror
LDFLAGS=

# Prerequisites.

test-prereqs:
	@echo Checking for GLIB...
	@pkg-config glib-2.0 --exists --print-errors
	@echo Checking for Gavro...
	@pkg-config gavro --exists --print-errors

GLIB_CFLAGS := $(shell pkg-config glib-2.0 gobject-2.0 --cflags)
GLIB_LDFLAGS := $(shell pkg-config glib-2.0 gobject-2.0 --libs)

GAVRO_CFLAGS := $(shell pkg-config gavro --cflags)
GAVRO_LDFLAGS := $(shell pkg-config gavro --libs)

# Build rules

ifeq (,$(V))
QUIET_CC   = @echo '   ' CC $@;
QUIET_LINK = @echo '   ' LINK $@;
else
QUIET_CC   =
QUIET_LINK =
endif


build: builddir build/gavro.so

builddir:
	@mkdir -p $(BUILD_DIR)

build/gavro.o: src/gavro.c
	$(QUIET_CC)$(CC) -o $@ $(CFLAGS) $(GLIB_CFLAGS) -c $(GAVRO_CFLAGS) $<

build/gavro.so: build/gavro.o
	$(QUIET_LINK)$(CC) -o $@ $(LIBFLAG) $(GLIB_LDFLAGS) $(GAVRO_LDFLAGS) $<

clean:
	@echo Cleaning...
	@rm -rf build

install:
	@echo Installing...
	@install -d -m 0755 $(LUA_LIBDIR)
	@install build/gavro.so $(LUA_LIBDIR)

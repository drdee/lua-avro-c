all: test-prereqs build

.PHONY: all test-prereqs build builddir build-lua clean install

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
	@echo Checking for Avro C library...
	@pkg-config 'avro-c >= 1.5.0' --exists --print-errors

GLIB_CFLAGS := $(shell pkg-config glib-2.0 gobject-2.0 --cflags)
GLIB_LDFLAGS := $(shell pkg-config glib-2.0 gobject-2.0 --libs)

AVRO_CFLAGS := $(shell pkg-config avro-c --cflags)
AVRO_LDFLAGS := $(shell pkg-config avro-c --libs)

# Build rules

ifeq (,$(V))
QUIET_CC   = @echo '   ' CC $@;
QUIET_CP   = @echo '   ' CP $@;
QUIET_LINK = @echo '   ' LINK $@;
else
QUIET_CC   =
QUIET_CP   =
QUIET_LINK =
endif


build: builddir build-lua build/avro/c/legacy.so

build-lua: build/avro.lua build/avro/test.lua build/avro/c/ffi.lua

build/avro.lua: src/avro.lua
	$(QUIET_CP)cp src/avro.lua $(BUILD_DIR)

build/avro/test.lua: src/avro/test.lua
	$(QUIET_CP)cp src/avro/test.lua $(BUILD_DIR)/avro

build/avro/c/ffi.lua: src/avro/c/ffi.lua
	$(QUIET_CP)cp src/avro/c/ffi.lua $(BUILD_DIR)/avro/c

builddir:
	@mkdir -p $(BUILD_DIR)/avro/c

build/avro/c/legacy.o: src/avro/c/legacy.c
	$(QUIET_CC)$(CC) -o $@ $(CFLAGS) $(GLIB_CFLAGS) -c $(AVRO_CFLAGS) $<

build/avro/c/legacy.so: build/avro/c/legacy.o
	$(QUIET_LINK)$(CC) -o $@ $(LIBFLAG) $(GLIB_LDFLAGS) $(AVRO_LDFLAGS) $<

test: build
	@echo Testing in Lua...
	@cd $(BUILD_DIR) && lua avro/test.lua
	@echo Testing in LuaJIT...
	@cd $(BUILD_DIR) && luajit avro/test.lua

clean:
	@echo Cleaning...
	@rm -rf build

install:
	@echo Installing...
	@install -d -m 0755 $(LUA_SHAREDIR)
	@install -d -m 0755 $(LUA_SHAREDIR)/avro
	@install -d -m 0755 $(LUA_SHAREDIR)/avro/c
	@install build/avro.lua $(LUA_SHAREDIR)
	@install build/avro/test.lua $(LUA_SHAREDIR)/avro
	@install build/avro/c/ffi.lua $(LUA_SHAREDIR)/avro/c
	@install -d -m 0755 $(LUA_LIBDIR)
	@install build/avro/c/legacy.so $(LUA_LIBDIR)

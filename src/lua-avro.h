/* -*- coding: utf-8 -*-
 * ----------------------------------------------------------------------
 * Copyright © 2010, RedJack, LLC.
 * All rights reserved.
 *
 * Please see the LICENSE.txt file in this distribution for license
 * details.
 * ----------------------------------------------------------------------
 */

#ifndef LUA_AVRO_H
#define LUA_AVRO_H

#include <avro.h>
#include <lua.h>


/**
 * Push an avro_datum_t onto the given Lua stack.
 */

int
lua_gavro_push_datum(lua_State *L,
                     avro_datum_t datum,
                     avro_schema_t schema);


/**
 * Extract an avro_datum_t from the given index on the Lua stack.  Calls
 * lua_error if that index doesn't contain an Avro Datum instance.
 */

avro_datum_t
lua_gavro_get_datum(lua_State *L, int index);


/**
 * Push an avro_schema_t onto the given Lua stack.
 */

int
lua_gavro_push_resolver(lua_State *L, avro_schema_t schema);


/**
 * Extract an avro_schema_t from the given index on the Lua stack.
 * Calls lua_error if that index doesn't contain an Avro Schema
 * instance.
 */

avro_schema_t
lua_gavro_get_schema(lua_State *L, int index);


#endif  /* LUA_AVRO_H */

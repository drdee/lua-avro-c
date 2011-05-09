/* -*- coding: utf-8 -*-
 * ----------------------------------------------------------------------
 * Copyright © 2010, RedJack, LLC.
 * All rights reserved.
 *
 * Please see the LICENSE.txt file in this distribution for license
 * details.
 * ----------------------------------------------------------------------
 */

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include <avro.h>
#include <avro/consumer.h>
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>


/*-----------------------------------------------------------------------
 * Lua access — data
 */

/**
 * The string used to identify the AvroDatum class's metatable in the
 * Lua registry.
 */

#define MT_AVRO_DATUM "avro:AvroDatum"
#define MT_AVRO_DATUM_METHODS "avro:AvroDatum:methods"


typedef struct _LuaAvroDatum
{
    avro_datum_t  datum;
    avro_schema_t  schema;
} LuaAvroDatum;


int
lua_avro_push_datum(lua_State *L,
                    avro_datum_t datum,
                    avro_schema_t schema)
{
    LuaAvroDatum  *l_datum;

    l_datum = lua_newuserdata(L, sizeof(LuaAvroDatum));
    l_datum->datum = avro_datum_incref(datum);
    l_datum->schema = avro_schema_incref(schema);
    luaL_getmetatable(L, MT_AVRO_DATUM);
    lua_setmetatable(L, -2);
    return 1;
}


avro_datum_t
lua_avro_get_datum(lua_State *L, int index)
{
    LuaAvroDatum  *l_datum = luaL_checkudata(L, index, MT_AVRO_DATUM);
    return l_datum->datum;
}


/**
 * Returns the type of an AvroDatum instance.
 */

static int
l_datum_type(lua_State *L)
{
    avro_datum_t  datum = lua_avro_get_datum(L, 1);
    lua_pushnumber(L, avro_typeof(datum));
    return 1;
}


/**
 * Returns the name of the current union branch.
 */

static int
l_datum_discriminant(lua_State *L)
{
    LuaAvroDatum  *l_datum = luaL_checkudata(L, 1, MT_AVRO_DATUM);

    if (!is_avro_union(l_datum->datum))
    {
        lua_pushstring(L, "Can't get discriminant of a non-union datum");
        return lua_error(L);
    }

    int  discriminant = avro_union_discriminant(l_datum->datum);
    avro_schema_t  branch =
        avro_schema_union_branch(l_datum->schema, discriminant);
    lua_pushstring(L, avro_schema_type_name(branch));
    return 1;
}


/**
 * Returns a JSON-encoded string representing the datum.
 */

static int
l_datum_tostring(lua_State *L)
{
    LuaAvroDatum  *l_datum = luaL_checkudata(L, 1, MT_AVRO_DATUM);
    char  *json_str = NULL;

    if (avro_datum_to_json(l_datum->datum, 1, &json_str))
    {
        lua_pushliteral(L, "Error retrieving JSON encoding for datum");
        return lua_error(L);
    }

    lua_pushstring(L, json_str);
    free(json_str);
    return 1;
}


/**
 * If @ref datum is an Avro scalar, we push the Lua equivalent onto
 * the stack.  If the datum is not a scalar, and @ref require_scalar
 * is true, we raise a Lua error.  Otherwise, we push a new AvroDatum
 * wrapper onto the stack.
 */

static int
lua_avro_push_scalar_or_datum(lua_State *L,
                              avro_datum_t datum,
                              avro_schema_t schema,
                              bool require_scalar)
{
    switch (avro_typeof(datum))
    {
      case AVRO_STRING:
        {
            char  *val = NULL;
            avro_string_get(datum, &val);
            lua_pushstring(L, val);
            return 1;
        }

      case AVRO_BYTES:
        {
            char  *val = NULL;
            int64_t  size = 0;
            avro_bytes_get(datum, &val, &size);
            lua_pushlstring(L, val, size);
            return 1;
        }

      case AVRO_INT32:
        {
            int32_t  val = 0;
            avro_int32_get(datum, &val);
            lua_pushnumber(L, val);
            return 1;
        }

      case AVRO_INT64:
        {
            int64_t  val = 0;
            avro_int64_get(datum, &val);
            lua_pushnumber(L, val);
            return 1;
        }

      case AVRO_FLOAT:
        {
            float  val = 0;
            avro_float_get(datum, &val);
            lua_pushnumber(L, val);
            return 1;
        }

      case AVRO_DOUBLE:
        {
            double  val = 0;
            avro_double_get(datum, &val);
            lua_pushnumber(L, val);
            return 1;
        }

      case AVRO_BOOLEAN:
        {
            int8_t  val = 0;
            avro_boolean_get(datum, &val);
            lua_pushboolean(L, val);
            return 1;
        }

      case AVRO_NULL:
        {
            lua_pushnil(L);
            return 1;
        }

      case AVRO_ENUM:
        {
            const char  *name = avro_enum_get_name(datum);
            lua_pushstring(L, name);
            return 1;
        }

      case AVRO_FIXED:
        {
            char  *val = NULL;
            int64_t  size = 0;
            avro_fixed_get(datum, &val, &size);
            lua_pushlstring(L, val, size);
            return 1;
        }

      default:
        if (require_scalar)
        {
            lua_pushstring(L, "Avro datum isn't a scalar");
            return lua_error(L);
        }

        else
        {
            lua_avro_push_datum(L, datum, schema);
            return 1;
        }
    }
}


/**
 * Returns the datum of a scalar AvroDatum instance.  If the datum
 * isn't a scalar, we raise an error.
 */

static int
l_datum_scalar(lua_State *L)
{
    LuaAvroDatum  *l_datum = luaL_checkudata(L, 1, MT_AVRO_DATUM);
    return lua_avro_push_scalar_or_datum
        (L, l_datum->datum, l_datum->schema, true);
}


/**
 * Extract the given element from an Avro map datum, and push it onto
 * the stack.  If the datum is a scalar, push the Lua equivalent of
 * the scalar datum onto the stack, rather than a new AvroDatum
 * wrapper.
 *
 * We follow the Lua convention that @ref index is 1-based.
 */

static int
get_array_element(lua_State *L,
                  LuaAvroDatum *l_datum,
                  unsigned int index,
                  bool coerce_scalar)
{
    if ((index < 1) || (index > avro_array_size(l_datum->datum)))
    {
        lua_pushnil(L);
        lua_pushliteral(L, "Index out of bounds");
        return 2;
    }

    avro_datum_t  element_datum = NULL;
    avro_array_get(l_datum->datum, index-1, &element_datum);

    avro_schema_t  element_schema =
        avro_schema_array_items(l_datum->schema);

    if (coerce_scalar)
    {
        return lua_avro_push_scalar_or_datum
            (L, element_datum, element_schema, false);
    }

    else
    {
        lua_avro_push_datum(L, element_datum, element_schema);
        return 1;
    }
}


/**
 * Extract the named datum from an Avro map datum, and push it onto
 * the stack.  If the datum is a scalar, push the Lua equivalent of
 * the scalar datum onto the stack, rather than a new AvroDatum
 * wrapper.
 */

static int
get_map_datum(lua_State *L,
              LuaAvroDatum *l_datum,
              const char *key,
              bool can_create,
              bool coerce_scalar)
{
    avro_schema_t  element_schema = avro_schema_map_values(l_datum->schema);
    avro_datum_t  element_datum = NULL;
    avro_map_get(l_datum->datum, key, &element_datum);

    if (element_datum == NULL)
    {
        if (can_create)
        {
            element_datum = avro_datum_from_schema(element_schema);
            avro_map_set(l_datum->datum, key, element_datum);
            avro_datum_decref(element_datum);
        }

        else
        {
            lua_pushnil(L);
            lua_pushliteral(L, "Map element doesn't exist");
            return 2;
        }
    }

    if (coerce_scalar)
    {
        return lua_avro_push_scalar_or_datum
            (L, element_datum, element_schema, false);
    }

    else
    {
        lua_avro_push_datum(L, element_datum, element_schema);
        return 1;
    }
}


/**
 * Extract the named field from an Avro record datum, and push it onto
 * the stack.  If the field is a scalar, push the Lua equivalent of
 * the scalar datum onto the stack, rather than a new AvroDatum
 * wrapper.
 */

static int
get_record_field(lua_State *L,
                 LuaAvroDatum *l_datum,
                 const char *field_name,
                 bool coerce_scalar)
{
    avro_datum_t  field_datum = NULL;
    avro_record_get(l_datum->datum, field_name, &field_datum);

    if (field_datum == NULL)
    {
        lua_pushnil(L);
        lua_pushliteral(L, "Record field doesn't exist");
        return 2;
    }

    avro_schema_t  field_schema =
        avro_schema_record_field_get(l_datum->schema, field_name);

    if (coerce_scalar)
    {
        return lua_avro_push_scalar_or_datum
            (L, field_datum, field_schema, false);
    }

    else
    {
        lua_avro_push_datum(L, field_datum, field_schema);
        return 1;
    }
}


/**
 * Extract the branch datum from an Avro union datum, and push it onto
 * the stack.  If the field is a scalar, push the Lua equivalent of
 * the scalar datum onto the stack, rather than a new AvroDatum
 * wrapper.
 *
 * The “field_name” for a union branch must be “_”.  All other field
 * names result in a nil result.
 */

static int
get_union_branch(lua_State *L,
                 LuaAvroDatum *l_datum,
                 const char *field_name,
                 bool coerce_scalar)
{
    int  discriminant;
    avro_datum_t  branch;
    avro_datum_t  branch_schema;

    if (strcmp(field_name, "_") == 0)
    {
        discriminant = avro_union_discriminant(l_datum->datum);
        branch = avro_union_current_branch(l_datum->datum);
        branch_schema = avro_schema_union_branch
            (l_datum->schema, discriminant);

    }

    else
    {
        branch_schema = avro_schema_union_branch_by_name
            (l_datum->schema, &discriminant, field_name);

        if (branch_schema == NULL)
        {
            lua_pushnil(L);
            lua_pushliteral(L, "Union branch doesn't exist");
            return 2;
        }

        avro_union_set_discriminant(l_datum->datum, discriminant, &branch);
    }

    if (branch == NULL)
    {
        lua_pushnil(L);
        lua_pushstring(L, avro_strerror());
        return 2;
    }

    if (coerce_scalar)
    {
        return lua_avro_push_scalar_or_datum(L, branch, branch_schema, false);
    }

    else
    {
        lua_avro_push_datum(L, branch, branch_schema);
        return 1;
    }
}


/**
 * Extracts the given subdatum from an AvroDatum instance.  If @ref
 * extract_scalar is true, and the result is a scalar Avro datum, then
 * we extract out scalar datum and push the Lua equivalent onto the
 * stack.  Otherwise, we push an AvroDatum wrapper onto the stack.
 */

static int
get_subdatum(lua_State *L,
             LuaAvroDatum *l_datum,
             int index_index,
             bool can_create,
             bool coerce_datum)
{
    if (lua_isnumber(L, index_index))
    {
        /*
         * We have an integer index.  If this is an array, look for
         * the element with the given index.
         */

        lua_Integer  index = lua_tointeger(L, index_index);

        if (is_avro_array(l_datum->datum))
        {
            return get_array_element(L, l_datum, index, coerce_datum);
        }
    }

    const char  *index_str = luaL_optstring(L, index_index, NULL);
    if (index_str != NULL)
    {
        /*
         * We have a string index.  If this is a map, look for the
         * datum with the given key.  If this is a record, look for a
         * field with that name.
         */

        if (is_avro_map(l_datum->datum))
        {
            return get_map_datum(L, l_datum, index_str, can_create, coerce_datum);
        }

        else if (is_avro_record(l_datum->datum))
        {
            return get_record_field(L, l_datum, index_str, coerce_datum);
        }

        else if (is_avro_union(l_datum->datum))
        {
            return get_union_branch(L, l_datum, index_str, coerce_datum);
        }
    }

    /*
     * If we fall through to here, we don't know how to handle this
     * kind of index against this kind of datum.
     */

    return 0;
}


/**
 * Returns the given subdatum in an AvroDatum instance.
 */

static int
l_datum_get(lua_State *L)
{
    LuaAvroDatum  *l_datum = luaL_checkudata(L, 1, MT_AVRO_DATUM);
    return get_subdatum(L, l_datum, 2, false, true);
}


/**
 * An implementation of the AvroDatum class's __index metamethod.  It
 * first checks the MT_AVRO_DATUM_METHODS table to see if
 * there's an AvroDatum method with the given name.  If not, then we
 * fall back to see if the AvroDatum contains a subfield with that
 * name.
 */

static int
l_datum_index(lua_State *L)
{
    /*
     * First see if the METHODS table contains a method function for
     * the given key (which is at stack index 2).
     */

    luaL_getmetatable(L, MT_AVRO_DATUM_METHODS);
    lua_pushvalue(L, 2);
    lua_rawget(L, -2);

    if (!lua_isnil(L, -1))
    {
        return 1;
    }

    /*
     * Otherwise fall back on the AvroDatum:get() method, which looks
     * for a subdatum with the given name.  Pop off the METHODS table
     * and nil datum first.
     */

    lua_pop(L, 2);
    return l_datum_get(L);
}


/**
 * Sets the datum datum of an Avro scalar.  If the datum is not a
 * scalar, we raise a Lua error.
 */

static int
set_scalar_datum(lua_State *L, int self_index, int val_index)
{
    LuaAvroDatum  *l_datum = luaL_checkudata(L, self_index, MT_AVRO_DATUM);

    switch (avro_typeof(l_datum->datum))
    {
      case AVRO_STRING:
        {
            const char  *str = luaL_checkstring(L, val_index);
            avro_string_set(l_datum->datum, str);
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_BYTES:
        {
            size_t  len = 0;
            const char  *buf = luaL_checklstring(L, val_index, &len);
            avro_bytes_set(l_datum->datum, buf, len);
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_INT32:
        {
            lua_Integer  i = luaL_checkinteger(L, val_index);
            avro_int32_set(l_datum->datum, i);
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_INT64:
        {
            long  l = luaL_checklong(L, val_index);
            avro_int64_set(l_datum->datum, l);
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_FLOAT:
        {
            lua_Number  n = luaL_checknumber(L, val_index);
            avro_float_set(l_datum->datum, (float) n);
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_DOUBLE:
        {
            lua_Number  n = luaL_checknumber(L, val_index);
            avro_double_set(l_datum->datum, (double) n);
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_BOOLEAN:
        {
            int  b = lua_toboolean(L, val_index);
            avro_boolean_set(l_datum->datum, b);
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_NULL:
        {
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_ENUM:
        {
            const char  *symbol = luaL_checkstring(L, val_index);
            avro_enum_set_name(l_datum->datum, symbol);
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_FIXED:
        {
            size_t  len = 0;
            const char  *buf = luaL_checklstring(L, val_index, &len);
            avro_fixed_set(l_datum->datum, buf, len);
            lua_pushvalue(L, self_index);
            return 1;
        }

      default:
        {
            lua_pushstring(L, "Avro datum isn't a scalar");
            return lua_error(L);
        }
    }
}


/**
 * Sets the datum of a scalar (if called with one parameter), or the
 * given subdatum in a compound AvroDatum (if called with two).
 */

static int
l_datum_set(lua_State *L)
{
    int  nargs = lua_gettop(L);

    /*
     * If there are two arguments (including self), then the caller is
     * trying to set the datum of a scalar.
     */

    if (nargs == 2)
    {
        return set_scalar_datum(L, 1, 2);
    }

    /*
     * If there are three arguments, then the caller is trying to set
     * the datum of a field/element/branch, which should be a scalar.
     */

    if (nargs == 3)
    {
        LuaAvroDatum  *l_datum = luaL_checkudata(L, 1, MT_AVRO_DATUM);
        if (!get_subdatum(L, l_datum, 2, true, false))
        {
            lua_pushstring(L, "Nonexistent subdatum");
            return lua_error(L);
        }

        /*
         * The new datum will be pushed onto the top of the stack.
         */

        return set_scalar_datum(L, -1, 3);
    }

    /*
     * Bad number of arguments!
     */

    lua_pushstring(L, "Bad number of arguments to AvroDatum:set");
    return lua_error(L);
}


/**
 * An implementation of the AvroDatum class's __newindex metamethod.  It
 * first checks the MT_AVRO_DATUM_METHODS table to see if there's an
 * AvroDatum method with the given name.  If so, you can't use this
 * syntax to set the field; you must use the set method, instead.
 */

static int
l_datum_newindex(lua_State *L)
{
    /*
     * First see if the METHODS table contains a method function for
     * the given key (which is at stack index 2).
     */

    luaL_getmetatable(L, MT_AVRO_DATUM_METHODS);
    lua_pushvalue(L, 2);
    lua_rawget(L, -2);

    if (!lua_isnil(L, -1))
    {
        lua_pushstring(L, "Cannot set field with [] syntax");
        return lua_error(L);
    }

    /*
     * Otherwise fall back on the AvroDatum:set() method, which looks
     * for a subdatum with the given name.  Pop off the METHODS table
     * and nil datum first.
     */

    lua_pop(L, 2);
    return l_datum_set(L);
}


/**
 * Appends a new element to an Avro array.  If called with one
 * parameter, then the array must contain scalars, and the parameter
 * is used as the datum of the new element.  If called with no
 * parameters, then the array can contain any kind of element.  In
 * both cases, we return the AvroDatum for the new element.
 */

static int
l_datum_append(lua_State *L)
{
    int  nargs = lua_gettop(L);
    LuaAvroDatum  *l_datum = luaL_checkudata(L, 1, MT_AVRO_DATUM);

    if (!is_avro_array(l_datum->datum))
    {
        lua_pushstring(L, "Can only append to an array");
        return lua_error(L);
    }

    if (nargs > 2)
    {
        lua_pushstring(L, "Bad number of arguments to AvroDatum:append");
        return lua_error(L);
    }

    avro_schema_t  element_schema = avro_schema_array_items(l_datum->schema);
    avro_datum_t  element = avro_datum_from_schema(element_schema);
    avro_array_append_datum(l_datum->datum, element);
    lua_avro_push_datum(L, element, element_schema);
    avro_datum_decref(element);

    if (nargs == 2)
    {
        /*
         * If the caller provided a datum, then the new element must
         * be a scalar.
         */

        return set_scalar_datum(L, -1, 2);
    }

    /*
     * Otherwise just return the new element datum.
     */

    return 1;
}


/**
 * Iterates through the elements of an Avro array or map.  The result
 * of this function can be used as a for loop iterator.  For arrays,
 * the iterator behaves like the builtin ipairs function, returning
 * [i, element] pairs during each iteration.  For maps, it behaves
 * like the builtin pairs function, returning [key, element] pairs.
 * In both cases, if the elements are scalars, these will be
 * translated into the Lua equivalent; if they're compound datum
 * objects, you'll get an AvroDatum instance.
 */

typedef struct _Iterator
{
    avro_datum_t  datum;
    avro_schema_t  element_schema;
    unsigned int  next_index;
} Iterator;

#define MT_ITERATOR "sawmill:AvroDatum:iterator"

static void
create_iterator(lua_State *L, avro_datum_t datum, avro_schema_t element_schema)
{
    lua_newuserdata(L, sizeof(Iterator));
    Iterator  *state = lua_touserdata(L, -1);
    state->datum = avro_datum_incref(datum);
    state->element_schema = avro_schema_incref(element_schema);
    state->next_index = 0;
    luaL_getmetatable(L, MT_ITERATOR);
    lua_setmetatable(L, -2);
}

static int
iterator_gc(lua_State *L)
{
    Iterator  *state = luaL_checkudata(L, 1, MT_ITERATOR);
    if (state->datum != NULL)
    {
        avro_datum_decref(state->datum);
        state->datum = NULL;
    }
    if (state->element_schema != NULL)
    {
        avro_schema_decref(state->element_schema);
        state->element_schema = NULL;
    }
    return 0;
}

static int
iterate_array(lua_State *L)
{
    Iterator  *state = luaL_checkudata(L, 1, MT_ITERATOR);
    unsigned int  length = avro_array_size(state->datum);

    /*
     * next_index is the 0-based avro index, not the 1-based Lua
     * index.
     */

    if (state->next_index >= length)
    {
        return 0;
    }

    avro_datum_t  element = NULL;
    avro_array_get(state->datum, state->next_index, &element);
    lua_pushinteger(L, state->next_index+1);
    lua_avro_push_scalar_or_datum(L, element, state->element_schema, false);

    state->next_index++;
    return 2;
}

static int
iterate_map(lua_State *L)
{
    Iterator  *state = luaL_checkudata(L, 1, MT_ITERATOR);
    unsigned int  length = avro_map_size(state->datum);

    /*
     * next_index is the 0-based avro index, not the 1-based Lua
     * index.
     */

    if (state->next_index >= length)
    {
        return 0;
    }

    const char  *key = NULL;
    avro_datum_t  element = NULL;
    avro_map_get_key(state->datum, state->next_index, &key);
    avro_map_get(state->datum, key, &element);

    lua_pushstring(L, key);
    lua_avro_push_scalar_or_datum(L, element, state->element_schema, false);

    state->next_index++;
    return 2;
}

static int
l_datum_iterate(lua_State *L)
{
    LuaAvroDatum  *l_datum = luaL_checkudata(L, 1, MT_AVRO_DATUM);

    if (is_avro_array(l_datum->datum))
    {
        lua_pushcfunction(L, iterate_array);
        create_iterator(L, l_datum->datum, l_datum->schema);
        lua_pushnil(L);
        return 3;
    }

    if (is_avro_map(l_datum->datum))
    {
        lua_pushcfunction(L, iterate_map);
        create_iterator(L, l_datum->datum, l_datum->schema);
        lua_pushnil(L);
        return 3;
    }

    lua_pushstring(L, "Can only iterate through arrays and maps");
    return lua_error(L);
}


/**
 * Encode an Avro value using the given resolver.
 */

static int
l_datum_encode(lua_State *L)
{
    static avro_consumer_t  *encoding_consumer = NULL;
    static avro_consumer_t  *sizeof_consumer = NULL;
    static char  static_buf[65536];

    LuaAvroDatum  *l_datum = luaL_checkudata(L, 1, MT_AVRO_DATUM);

    if (encoding_consumer == NULL) {
        encoding_consumer = avro_encoding_consumer_new();
    }

    if (sizeof_consumer == NULL) {
        sizeof_consumer = avro_sizeof_consumer_new();
    }

    size_t  size = 0;
    avro_consume_datum(l_datum->datum, sizeof_consumer, &size);

    int  result;
    char  *buf;
    bool  free_buf;

    if (size <= sizeof(static_buf)) {
        buf = static_buf;
        free_buf = false;
    } else {
        const char  *buf = malloc(size);
        if (buf == NULL) {
            lua_pushnil(L);
            lua_pushstring(L, "Out of memory");
            return 2;
        }
        free_buf = true;
    }

    avro_writer_t  writer = avro_writer_memory(buf, size);
    result = avro_consume_datum(l_datum->datum, encoding_consumer, writer);
    avro_writer_free(writer);

    if (result) {
        if (free_buf) {
            free(buf);
        }
        lua_pushnil(L);
        lua_pushstring(L, avro_strerror());
        return 2;
    }

    lua_pushlstring(L, buf, size);
    if (free_buf) {
        free(buf);
    }
    return 1;
}


/**
 * Finalizes an AvroDatum instance.
 */

static int
l_datum_gc(lua_State *L)
{
    LuaAvroDatum  *l_datum = luaL_checkudata(L, 1, MT_AVRO_DATUM);
    if (l_datum->datum != NULL)
    {
        avro_datum_decref(l_datum->datum);
        l_datum->datum = NULL;
    }
    if (l_datum->schema != NULL)
    {
        avro_datum_decref(l_datum->schema);
        l_datum->schema = NULL;
    }
    return 0;
}


/**
 * Creates a new AvroDatum instance from a JSON string.
 */

static int
l_datum_new(lua_State *L)
{
    /* TODO */
    lua_pushnil(L);
    return 1;
}


/*-----------------------------------------------------------------------
 * Lua access — schemas
 */

/**
 * The string used to identify the AvroSchema class's metatable in the
 * Lua registry.
 */

#define MT_AVRO_SCHEMA "avro:AvroSchema"

typedef struct _LuaAvroSchema
{
    avro_schema_t  schema;
} LuaAvroSchema;


int
lua_avro_push_schema(lua_State *L, avro_schema_t schema)
{
    LuaAvroSchema  *l_schema;

    l_schema = lua_newuserdata(L, sizeof(LuaAvroSchema));
    l_schema->schema = avro_schema_incref(schema);
    luaL_getmetatable(L, MT_AVRO_SCHEMA);
    lua_setmetatable(L, -2);
    return 1;
}


avro_schema_t
lua_avro_get_schema(lua_State *L, int index)
{
    LuaAvroSchema  *l_schema = luaL_checkudata(L, index, MT_AVRO_SCHEMA);
    return l_schema->schema;
}


/**
 * Creates a new AvroDatum for the given schema.
 */

static int
l_schema_new_datum(lua_State *L)
{
    avro_schema_t  schema = lua_avro_get_schema(L, 1);
    avro_datum_t  datum = avro_datum_from_schema(schema);
    lua_avro_push_datum(L, datum, schema);
    avro_datum_decref(datum);
    return 1;
}


/**
 * Returns the type of an AvroSchema instance.
 */

static int
l_schema_type(lua_State *L)
{
    avro_schema_t  schema = lua_avro_get_schema(L, 1);
    lua_pushnumber(L, avro_typeof(schema));
    return 1;
}


/**
 * Finalizes an AvroSchema instance.
 */

static int
l_schema_gc(lua_State *L)
{
    LuaAvroSchema  *l_schema = luaL_checkudata(L, 1, MT_AVRO_SCHEMA);
    if (l_schema->schema != NULL)
    {
        avro_schema_decref(l_schema->schema);
        l_schema->schema = NULL;
    }
    return 0;
}


/**
 * Creates a new AvroSchema instance from a JSON schema string.
 */

static int
l_schema_new(lua_State *L)
{
    size_t  json_len;
    const char  *json_str = luaL_checklstring(L, 1, &json_len);

    avro_schema_error_t  schema_error;
    avro_schema_t  schema;

    int  rc = avro_schema_from_json(json_str, json_len, &schema, &schema_error);
    if (rc != 0)
    {
        lua_pushstring(L, "Error parsing JSON schema");
        return lua_error(L);
    }

    lua_avro_push_schema(L, schema);
    avro_schema_decref(schema);
    return 1;
}


/*-----------------------------------------------------------------------
 * Lua access — resolvers
 */

/**
 * The string used to identify the AvroResolver class's metatable in the
 * Lua registry.
 */

#define MT_AVRO_RESOLVER "avro:AvroResolver"

typedef struct _LuaAvroResolver
{
    avro_consumer_t  *resolver;
} LuaAvroResolver;


int
lua_avro_push_resolver(lua_State *L, avro_consumer_t *resolver)
{
    LuaAvroResolver  *l_resolver;

    l_resolver = lua_newuserdata(L, sizeof(LuaAvroResolver));
    l_resolver->resolver = resolver;
    luaL_getmetatable(L, MT_AVRO_RESOLVER);
    lua_setmetatable(L, -2);
    return 1;
}


avro_consumer_t *
lua_avro_get_resolver(lua_State *L, int index)
{
    LuaAvroResolver  *l_resolver = luaL_checkudata(L, index, MT_AVRO_RESOLVER);
    return l_resolver->resolver;
}


/**
 * Creates a new AvroResolver for the given schemas.
 */

static int
l_resolver_new(lua_State *L)
{
    avro_schema_t  writer_schema = lua_avro_get_schema(L, 1);
    avro_schema_t  reader_schema = lua_avro_get_schema(L, 2);
    avro_consumer_t  *resolver = avro_resolver_new(writer_schema, reader_schema);
    if (resolver == NULL) {
        lua_pushnil(L);
        lua_pushstring(L, avro_strerror());
        return 2;
    } else {
        lua_avro_push_resolver(L, resolver);
        return 1;
    }
}


/**
 * Finalizes an AvroResolver instance.
 */

static int
l_resolver_gc(lua_State *L)
{
    LuaAvroResolver  *l_resolver = luaL_checkudata(L, 1, MT_AVRO_RESOLVER);
    if (l_resolver->resolver != NULL)
    {
        avro_consumer_free(l_resolver->resolver);
        l_resolver->resolver = NULL;
    }
    return 0;
}


/**
 * Decode an Avro value using the given resolver.
 */

static int
l_resolver_decode(lua_State *L)
{
    LuaAvroResolver  *l_resolver = luaL_checkudata(L, 1, MT_AVRO_RESOLVER);
    size_t  size = 0;
    const char  *buf = luaL_checklstring(L, 2, &size);
    LuaAvroDatum  *l_datum = luaL_checkudata(L, 3, MT_AVRO_DATUM);

    avro_reader_t  reader = avro_reader_memory(buf, size);
    if (avro_consume_binary(reader, l_resolver->resolver, l_datum->datum)) {
        lua_pushnil(L);
        lua_pushstring(L, avro_strerror());
        return 2;
    }

    avro_reader_free(reader);
    lua_pushboolean(L, true);
    return 1;
}


/*-----------------------------------------------------------------------
 * Lua access — module
 */

static const luaL_Reg  datum_methods[] =
{
    {"append", l_datum_append},
    {"discriminant", l_datum_discriminant},
    {"encode", l_datum_encode},
    {"get", l_datum_get},
    {"iterate", l_datum_iterate},
    {"scalar", l_datum_scalar},
    {"set", l_datum_set},
    {"type", l_datum_type},
    {NULL, NULL}
};


static const luaL_Reg  schema_methods[] =
{
    {"new_value", l_schema_new_datum},
    {"type", l_schema_type},
    {NULL, NULL}
};


static const luaL_Reg  resolver_methods[] =
{
    {"decode", l_resolver_decode},
    {NULL, NULL}
};


static const luaL_Reg  mod_methods[] =
{
    {"Resolver", l_resolver_new},
    {"Schema", l_schema_new},
    {"Value", l_datum_new},
    {NULL, NULL}
};


#define set_avro_const2(s1, s2)    \
    lua_pushinteger(L, AVRO_##s1); \
    lua_setfield(L, -2, #s2);

#define set_avro_const(s)         \
    lua_pushinteger(L, AVRO_##s); \
    lua_setfield(L, -2, #s);


int
luaopen_avro(lua_State *L)
{
    /* AvroSchema metatable */

    luaL_newmetatable(L, MT_AVRO_SCHEMA);
    lua_createtable(L, 0, sizeof(schema_methods) / sizeof(luaL_reg) - 1);
    luaL_register(L, NULL, schema_methods);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_schema_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    /* AvroDatum metatables */

    luaL_newmetatable(L, MT_AVRO_DATUM_METHODS);
    luaL_register(L, NULL, datum_methods);
    lua_pop(L, 1);

    luaL_newmetatable(L, MT_AVRO_DATUM);
    lua_pushcfunction(L, l_datum_tostring);
    lua_setfield(L, -2, "__tostring");
    lua_pushcfunction(L, l_datum_index);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_datum_newindex);
    lua_setfield(L, -2, "__newindex");
    lua_pushcfunction(L, l_datum_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    luaL_newmetatable(L, MT_ITERATOR);
    lua_pushcfunction(L, iterator_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    /* AvroResolver metatable */

    luaL_newmetatable(L, MT_AVRO_RESOLVER);
    lua_createtable(L, 0, sizeof(resolver_methods) / sizeof(luaL_reg) - 1);
    luaL_register(L, NULL, resolver_methods);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_resolver_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    luaL_register(L, "avro", mod_methods);

    set_avro_const(BOOLEAN);
    set_avro_const(BYTES);
    set_avro_const(DOUBLE);
    set_avro_const(FLOAT);
    set_avro_const2(INT32, INT);
    set_avro_const2(INT64, LONG);
    set_avro_const(NULL);
    set_avro_const(STRING);

    set_avro_const(ARRAY);
    set_avro_const(ENUM);
    set_avro_const(FIXED);
    set_avro_const(MAP);
    set_avro_const(RECORD);
    set_avro_const(UNION);

    return 1;
}

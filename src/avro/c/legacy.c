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
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <avro.h>
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>


/*-----------------------------------------------------------------------
 * Lua access — data
 */

/**
 * The string used to identify the AvroValue class's metatable in the
 * Lua registry.
 */

#define MT_AVRO_VALUE "avro:AvroValue"
#define MT_AVRO_VALUE_METHODS "avro:AvroValue:methods"

static int
lua_return_avro_error(lua_State *L)
{
    lua_pushnil(L);
    lua_pushstring(L, avro_strerror());
    return 2;
}

static int
lua_avro_error(lua_State *L)
{
    lua_pushstring(L, avro_strerror());
    return lua_error(L);
}

#define check(call) \
    do { \
        int __rc; \
        __rc = call; \
        if (__rc != 0) { \
            return lua_avro_error(L); \
        } \
    } while (0)


#define NO_DESTRUCTOR 0
#define GENERIC_DESTRUCTOR 1
#define RESOLVED_READER_DESTRUCTOR 2

typedef struct _LuaAvroValue
{
    avro_value_t  value;
    int  destructor;
} LuaAvroValue;


int
lua_avro_push_value(lua_State *L, avro_value_t *value, int destructor)
{
    LuaAvroValue  *l_value;

    l_value = lua_newuserdata(L, sizeof(LuaAvroValue));
    l_value->value = *value;
    l_value->destructor = destructor;
    luaL_getmetatable(L, MT_AVRO_VALUE);
    lua_setmetatable(L, -2);
    return 1;
}


avro_value_t *
lua_avro_get_value(lua_State *L, int index)
{
    LuaAvroValue  *l_value = luaL_checkudata(L, index, MT_AVRO_VALUE);
    return &l_value->value;
}


/**
 * Returns the type of an AvroValue instance.
 */

static int
l_value_type(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    lua_pushnumber(L, avro_value_get_type(value));
    return 1;
}


/**
 * Returns the name of the current union branch.
 */

static int
l_value_discriminant(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);

    if (avro_value_get_type(value) != AVRO_UNION) {
        lua_pushliteral(L, "Can't get discriminant of a non-union value");
        return lua_error(L);
    }

    int  discriminant;
    check(avro_value_get_discriminant(value, &discriminant));

    avro_schema_t  union_schema = avro_value_get_schema(value);
    avro_schema_t  branch =
        avro_schema_union_branch(union_schema, discriminant);
    lua_pushstring(L, avro_schema_type_name(branch));
    return 1;
}


/**
 * Returns a JSON-encoded string representing the value.
 */

static int
l_value_tostring(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    char  *json_str = NULL;

    if (avro_value_to_json(value, 1, &json_str))
    {
        lua_pushliteral(L, "Error retrieving JSON encoding for value");
        return lua_error(L);
    }

    lua_pushstring(L, json_str);
    free(json_str);
    return 1;
}


/**
 * Compares two values for equality.
 */

static int
l_value_eq(lua_State *L)
{
    avro_value_t  *value1 = lua_avro_get_value(L, 1);
    avro_value_t  *value2 = lua_avro_get_value(L, 2);
    lua_pushboolean(L, avro_value_equal(value1, value2));
    return 1;
}


/**
 * Sets the source of a resolved reader value.
 */

static int
l_value_set_source(lua_State *L)
{
    LuaAvroValue  *l_value1 = luaL_checkudata(L, 1, MT_AVRO_VALUE);
    avro_value_t  *value2 = lua_avro_get_value(L, 2);

    if (l_value1->destructor != RESOLVED_READER_DESTRUCTOR) {
        lua_pushliteral(L, "Can only call set_source on a resolved reader value");
        return lua_error(L);
    }

    avro_resolved_reader_set_source(&l_value1->value, value2);
    return 0;
}


/**
 * If @ref value is an Avro scalar, we push the Lua equivalent onto
 * the stack.  If the value is not a scalar, and @ref require_scalar
 * is true, we raise a Lua error.  Otherwise, we push a new AvroValue
 * wrapper onto the stack.
 */

static int
lua_avro_push_scalar_or_value(lua_State *L, avro_value_t *value,
                              bool require_scalar, int destructor)
{
    switch (avro_value_get_type(value))
    {
      case AVRO_STRING:
        {
            const char  *val = NULL;
            size_t  size = 0;
            check(avro_value_get_string(value, &val, &size));
            /* size contains the NUL terminator */
            lua_pushlstring(L, val, size-1);
            return 1;
        }

      case AVRO_BYTES:
        {
            const void  *val = NULL;
            size_t  size = 0;
            check(avro_value_get_bytes(value, &val, &size));
            lua_pushlstring(L, val, size);
            return 1;
        }

      case AVRO_INT32:
        {
            int32_t  val = 0;
            check(avro_value_get_int(value, &val));
            lua_pushnumber(L, val);
            return 1;
        }

      case AVRO_INT64:
        {
            int64_t  val = 0;
            check(avro_value_get_long(value, &val));
            lua_pushnumber(L, val);
            return 1;
        }

      case AVRO_FLOAT:
        {
            float  val = 0;
            check(avro_value_get_float(value, &val));
            lua_pushnumber(L, val);
            return 1;
        }

      case AVRO_DOUBLE:
        {
            double  val = 0;
            check(avro_value_get_double(value, &val));
            lua_pushnumber(L, val);
            return 1;
        }

      case AVRO_BOOLEAN:
        {
            int  val = 0;
            check(avro_value_get_boolean(value, &val));
            lua_pushboolean(L, val);
            return 1;
        }

      case AVRO_NULL:
        {
            check(avro_value_get_null(value));
            lua_pushnil(L);
            return 1;
        }

      case AVRO_ENUM:
        {
            int  val = 0;
            check(avro_value_get_enum(value, &val));
            avro_schema_t  enum_schema = avro_value_get_schema(value);
            const char  *name = avro_schema_enum_get(enum_schema, val);
            lua_pushstring(L, name);
            return 1;
        }

      case AVRO_FIXED:
        {
            const void  *val = NULL;
            size_t  size = 0;
            check(avro_value_get_fixed(value, &val, &size));
            lua_pushlstring(L, val, size);
            return 1;
        }

      default:
        if (require_scalar) {
            return luaL_error(L, "Avro value isn't a scalar");
        }

        else {
            lua_avro_push_value(L, value, destructor);
            return 1;
        }
    }
}


/**
 * Returns the hash of an AvroValue instance.
 */

static int
l_value_hash(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    uint32_t  hash = avro_value_hash(value);
    lua_pushinteger(L, hash);
    return 1;
}


/**
 * Returns the value of a scalar AvroValue instance.  If the value
 * isn't a scalar, we raise an error.
 */

static int
l_value_scalar(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    return lua_avro_push_scalar_or_value(L, value, true, NO_DESTRUCTOR);
}


/**
 * Extract the given element from an Avro array value, and push it onto
 * the stack.  If the value is a scalar, push the Lua equivalent of the
 * scalar value onto the stack, rather than a new AvroValue wrapper.
 *
 * We follow the Lua convention that @ref index is 1-based.
 */

static int
get_array_element(lua_State *L, avro_value_t *value,
                  unsigned int index, bool coerce_scalar)
{
    size_t  array_size;
    check(avro_value_get_size(value, &array_size));

    if ((index < 1) || (index > array_size)) {
        lua_pushnil(L);
        lua_pushliteral(L, "Index out of bounds");
        return 2;
    }

    avro_value_t  element_value;
    check(avro_value_get_by_index(value, index-1, &element_value, NULL));

    if (coerce_scalar) {
        return lua_avro_push_scalar_or_value(L, &element_value, false, NO_DESTRUCTOR);
    }

    else {
        lua_avro_push_value(L, &element_value, NO_DESTRUCTOR);
        return 1;
    }
}


/**
 * Extract the named value from an Avro map value, and push it onto the
 * stack.  If the value is a scalar, push the Lua equivalent of the
 * scalar value onto the stack, rather than a new AvroValue wrapper.
 */

static int
get_map_value(lua_State *L, avro_value_t *value,
              const char *key, bool can_create, bool coerce_scalar)
{
    avro_value_t  element_value;

    if (can_create) {
        check(avro_value_add(value, key, &element_value, NULL, NULL));
    } else {
        check(avro_value_get_by_name(value, key, &element_value, NULL));
        if (element_value.self == NULL) {
            lua_pushnil(L);
            lua_pushliteral(L, "Map element doesn't exist");
            return 2;
        }
    }

    if (coerce_scalar) {
        return lua_avro_push_scalar_or_value(L, &element_value, false, NO_DESTRUCTOR);
    }

    else {
        lua_avro_push_value(L, &element_value, NO_DESTRUCTOR);
        return 1;
    }
}


/**
 * Extract the named field from an Avro record value, and push it onto
 * the stack.  If the field is a scalar, push the Lua equivalent of the
 * scalar value onto the stack, rather than a new AvroValue wrapper.
 */

static int
get_record_field(lua_State *L, avro_value_t *value,
                 const char *field_name, bool coerce_scalar)
{
    avro_value_t  field_value;
    check(avro_value_get_by_name(value, field_name, &field_value, NULL));

    if (field_value.self == NULL) {
        lua_pushnil(L);
        lua_pushliteral(L, "Record field doesn't exist");
        return 2;
    }

    if (coerce_scalar) {
        return lua_avro_push_scalar_or_value(L, &field_value, false, NO_DESTRUCTOR);
    }

    else {
        lua_avro_push_value(L, &field_value, NO_DESTRUCTOR);
        return 1;
    }
}


/**
 * Extract the branch value from an Avro union value, and push it onto
 * the stack.  If the field is a scalar, push the Lua equivalent of the
 * scalar value onto the stack, rather than a new AvroValue wrapper.
 *
 * If the “field_name” for a union branch is “_”, then we return the
 * current branch.  Otherwise the “field_name” must match the name of
 * one of the schemas of the union.
 */

static int
get_union_branch(lua_State *L, avro_value_t *value,
                 const char *field_name, bool coerce_scalar)
{
    avro_value_t  branch;

    if (field_name[0] == '_' && field_name[1] == '\0') {
        check(avro_value_get_current_branch(value, &branch));
    }

    else {
        int  discriminant;
        avro_schema_t  union_schema = avro_value_get_schema(value);
        avro_schema_t  branch_schema =
            avro_schema_union_branch_by_name
            (union_schema, &discriminant, field_name);

        if (branch_schema == NULL) {
            return lua_return_avro_error(L);
        }

        check(avro_value_set_branch(value, discriminant, &branch));
    }

    if (coerce_scalar) {
        return lua_avro_push_scalar_or_value(L, &branch, false, NO_DESTRUCTOR);
    }

    else {
        lua_avro_push_value(L, &branch, NO_DESTRUCTOR);
        return 1;
    }
}


/**
 * Extracts the given subvalue from an AvroValue instance.  If @ref
 * extract_scalar is true, and the result is a scalar Avro value, then
 * we extract out scalar value and push the Lua equivalent onto the
 * stack.  Otherwise, we push an AvroValue wrapper onto the stack.
 */

static int
get_subvalue(lua_State *L, avro_value_t *value,
             int index_index, bool can_create, bool coerce_value)
{
    if (lua_isnumber(L, index_index)) {
        /*
         * We have an integer index.  If this is an array, look for the
         * element with the given index.
         */

        lua_Integer  index = lua_tointeger(L, index_index);

        if (avro_value_get_type(value) == AVRO_ARRAY) {
            return get_array_element(L, value, index, coerce_value);
        }
    }

    const char  *index_str = luaL_optstring(L, index_index, NULL);
    if (index_str != NULL) {
        /*
         * We have a string index.  If this is a map, look for the value
         * with the given key.  If this is a record, look for a field
         * with that name.  If this is a union, activate the given
         * branch and return it.
         */

        if (avro_value_get_type(value) == AVRO_MAP) {
            return get_map_value(L, value, index_str, can_create, coerce_value);
        }

        if (avro_value_get_type(value) == AVRO_RECORD) {
            return get_record_field(L, value, index_str, coerce_value);
        }

        if (avro_value_get_type(value) == AVRO_UNION) {
            return get_union_branch(L, value, index_str, coerce_value);
        }
    }

    /*
     * If we fall through to here, we don't know how to handle this
     * kind of index against this kind of value.
     */

    return 0;
}


/**
 * Returns the given subvalue in an AvroValue instance.
 */

static int
l_value_get(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    return get_subvalue(L, value, 2, false, true);
}


/**
 * An implementation of the AvroValue class's __index metamethod.  It
 * first checks the MT_AVRO_VALUE_METHODS table to see if there's an
 * AvroValue method with the given name.  If not, then we fall back to
 * see if the AvroValue contains a subfield with that name.
 */

static int
l_value_index(lua_State *L)
{
    /*
     * First see if the METHODS table contains a method function for
     * the given key (which is at stack index 2).
     */

    luaL_getmetatable(L, MT_AVRO_VALUE_METHODS);
    lua_pushvalue(L, 2);
    lua_rawget(L, -2);

    if (!lua_isnil(L, -1)) {
        return 1;
    }

    /*
     * Otherwise fall back on the AvroValue:get() method, which looks
     * for a subvalue with the given name.  Pop off the METHODS table
     * and nil value first.
     */

    lua_pop(L, 2);
    return l_value_get(L);
}


/**
 * Sets the value value of an Avro scalar.  If the value is not a
 * scalar, we raise a Lua error.
 */

static int
set_scalar_value(lua_State *L, int self_index, int val_index)
{
    avro_value_t  *value = lua_avro_get_value(L, self_index);

    switch (avro_value_get_type(value))
    {
      case AVRO_STRING:
        {
            size_t  str_len;
            const char  *str = luaL_checklstring(L, val_index, &str_len);
            /* value length must include NUL terminatory */
            check(avro_value_set_string_len(value, (char *) str, str_len+1));
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_BYTES:
        {
            size_t  len;
            const char  *buf = luaL_checklstring(L, val_index, &len);
            check(avro_value_set_bytes(value, (void *) buf, len));
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_INT32:
        {
            lua_Integer  i = luaL_checkinteger(L, val_index);
            check(avro_value_set_int(value, i));
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_INT64:
        {
            long  l = luaL_checklong(L, val_index);
            check(avro_value_set_long(value, l));
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_FLOAT:
        {
            lua_Number  n = luaL_checknumber(L, val_index);
            check(avro_value_set_float(value, (float) n));
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_DOUBLE:
        {
            lua_Number  n = luaL_checknumber(L, val_index);
            check(avro_value_set_double(value, (double) n));
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_BOOLEAN:
        {
            int  b = lua_toboolean(L, val_index);
            check(avro_value_set_boolean(value, b));
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_NULL:
        {
            check(avro_value_set_null(value));
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_ENUM:
        {
            const char  *symbol = luaL_checkstring(L, val_index);
            avro_schema_t  enum_schema = avro_value_get_schema(value);
            int  symbol_value = avro_schema_enum_get_by_name(enum_schema, symbol);
            if (symbol_value < 0) {
                return luaL_error(L, "No symbol named %s", symbol);
            }
            check(avro_value_set_enum(value, symbol_value));
            lua_pushvalue(L, self_index);
            return 1;
        }

      case AVRO_FIXED:
        {
            size_t  len = 0;
            const char  *buf = luaL_checklstring(L, val_index, &len);
            check(avro_value_set_fixed(value, (void *) buf, len));
            lua_pushvalue(L, self_index);
            return 1;
        }

      default:
        {
            lua_pushliteral(L, "Avro value isn't a scalar");
            return lua_error(L);
        }
    }
}


/**
 * Sets the value of a scalar (if called with one parameter), or the
 * given subvalue in a compound AvroValue (if called with two).
 */

static int
l_value_set(lua_State *L)
{
    int  nargs = lua_gettop(L);

    /*
     * If there are two arguments (including self), then the caller is
     * trying to set the value of a scalar.
     */

    if (nargs == 2) {
        return set_scalar_value(L, 1, 2);
    }

    /*
     * If there are three arguments, then the caller is trying to set
     * the value of a field/element/branch, which should be a scalar.
     */

    if (nargs == 3) {
        avro_value_t  *value = lua_avro_get_value(L, 1);
        if (!get_subvalue(L, value, 2, true, false)) {
            lua_pushliteral(L, "Nonexistent subvalue");
            return lua_error(L);
        }

        /*
         * The new value will be pushed onto the top of the stack.
         */

        return set_scalar_value(L, -1, 3);
    }

    /*
     * Bad number of arguments!
     */

    lua_pushliteral(L, "Bad number of arguments to AvroValue:set");
    return lua_error(L);
}


/**
 * An implementation of the AvroValue class's __newindex metamethod.  It
 * first checks the MT_AVRO_VALUE_METHODS table to see if there's an
 * AvroValue method with the given name.  If so, you can't use this
 * syntax to set the field; you must use the set method, instead.
 */

static int
l_value_newindex(lua_State *L)
{
    /*
     * First see if the METHODS table contains a method function for
     * the given key (which is at stack index 2).
     */

    luaL_getmetatable(L, MT_AVRO_VALUE_METHODS);
    lua_pushvalue(L, 2);
    lua_rawget(L, -2);

    if (!lua_isnil(L, -1)) {
        lua_pushliteral(L, "Cannot set field with [] syntax");
        return lua_error(L);
    }

    /*
     * Otherwise fall back on the AvroValue:set() method, which looks
     * for a subvalue with the given name.  Pop off the METHODS table
     * and nil value first.
     */

    lua_pop(L, 2);
    return l_value_set(L);
}


/**
 * Appends a new element to an Avro array.  If called with one
 * parameter, then the array must contain scalars, and the parameter is
 * used as the value of the new element.  If called with no parameters,
 * then the array can contain any kind of element.  In both cases, we
 * return the AvroValue for the new element.
 */

static int
l_value_append(lua_State *L)
{
    int  nargs = lua_gettop(L);
    avro_value_t  *value = lua_avro_get_value(L, 1);

    if (avro_value_get_type(value) != AVRO_ARRAY) {
        lua_pushliteral(L, "Can only append to an array");
        return lua_error(L);
    }

    if (nargs > 2) {
        lua_pushliteral(L, "Bad number of arguments to AvroValue:append");
        return lua_error(L);
    }

    avro_value_t  element;
    check(avro_value_append(value, &element, NULL));
    lua_avro_push_value(L, &element, NO_DESTRUCTOR);

    if (nargs == 2) {
        /*
         * If the caller provided a value, then the new element must be
         * a scalar.
         */

        return set_scalar_value(L, -1, 2);
    }

    /*
     * Otherwise just return the new element value.
     */

    return 1;
}


/**
 * Iterates through the elements of an Avro array or map.  The result of
 * this function can be used as a for loop iterator.  For arrays, the
 * iterator behaves like the builtin ipairs function, returning [i,
 * element] pairs during each iteration.  For maps, it behaves like the
 * builtin pairs function, returning [key, element] pairs.  In both
 * cases, if the elements are scalars, these will be translated into the
 * Lua equivalent; if they're compound value objects, you'll get an
 * AvroValue instance.
 */

typedef struct _Iterator
{
    avro_value_t  *value;
    size_t  next_index;
} Iterator;

#define MT_ITERATOR "sawmill:AvroValue:iterator"

static void
create_iterator(lua_State *L, avro_value_t *value)
{
    lua_newuserdata(L, sizeof(Iterator));
    Iterator  *state = lua_touserdata(L, -1);
    state->value = value;
    state->next_index = 0;
    luaL_getmetatable(L, MT_ITERATOR);
    lua_setmetatable(L, -2);
}

static int
iterator_gc(lua_State *L)
{
    Iterator  *state = luaL_checkudata(L, 1, MT_ITERATOR);
    state->value = NULL;
    return 0;
}

static int
iterate_array(lua_State *L)
{
    Iterator  *state = luaL_checkudata(L, 1, MT_ITERATOR);
    size_t  length;
    check(avro_value_get_size(state->value, &length));

    /*
     * next_index is the 0-based avro index, not the 1-based Lua index.
     */

    if (state->next_index >= length) {
        return 0;
    }

    avro_value_t  element;
    check(avro_value_get_by_index(state->value, state->next_index, &element, NULL));
    lua_pushinteger(L, state->next_index+1);
    lua_avro_push_scalar_or_value(L, &element, false, NO_DESTRUCTOR);

    state->next_index++;
    return 2;
}

static int
iterate_map(lua_State *L)
{
    Iterator  *state = luaL_checkudata(L, 1, MT_ITERATOR);
    size_t  length;
    check(avro_value_get_size(state->value, &length));

    /*
     * next_index is the 0-based avro index, not the 1-based Lua index.
     */

    if (state->next_index >= length) {
        return 0;
    }

    const char  *key = NULL;
    avro_value_t  element;
    check(avro_value_get_by_index(state->value, state->next_index, &element, &key));

    lua_pushstring(L, key);
    lua_avro_push_scalar_or_value(L, &element, false, NO_DESTRUCTOR);

    state->next_index++;
    return 2;
}

static int
l_value_iterate(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    avro_type_t  value_type = avro_value_get_type(value);

    if (value_type == AVRO_ARRAY) {
        lua_pushcfunction(L, iterate_array);
        create_iterator(L, value);
        lua_pushnil(L);
        return 3;
    }

    if (value_type == AVRO_MAP) {
        lua_pushcfunction(L, iterate_map);
        create_iterator(L, value);
        lua_pushnil(L);
        return 3;
    }

    lua_pushliteral(L, "Can only iterate through arrays and maps");
    return lua_error(L);
}


/**
 * Encode an Avro value using the binary encoding.  Returns the result
 * as a Lua string.
 */

static int
l_value_encode(lua_State *L)
{
    static char  static_buf[65536];

    avro_value_t  *value = lua_avro_get_value(L, 1);

    size_t  size = 0;
    check(avro_value_sizeof(value, &size));

    int  result;
    char  *buf;
    bool  free_buf;

    if (size <= sizeof(static_buf)) {
        buf = static_buf;
        free_buf = false;
    } else {
        buf = malloc(size);
        if (buf == NULL) {
            lua_pushnil(L);
            lua_pushliteral(L, "Out of memory");
            return 2;
        }
        free_buf = true;
    }

    avro_writer_t  writer = avro_writer_memory(buf, size);
    result = avro_value_write(writer, value);
    avro_writer_free(writer);

    if (result) {
        if (free_buf) {
            free(buf);
        }
        return lua_return_avro_error(L);
    }

    lua_pushlstring(L, buf, size);
    if (free_buf) {
        free(buf);
    }
    return 1;
}


/**
 * Return the length of the binary encoding of the value.
 */

static int
l_value_encoded_size(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);

    size_t  size = 0;
    check(avro_value_sizeof(value, &size));

    lua_pushinteger(L, size);
    return 1;
}


/**
 * Encode an Avro value using the binary encoding.  The result is placed
 * into the given memory region, which is provided as a light user data
 * and a size.  There's no safety checking here; to make it easier to
 * not include this function in sandboxes, it's exposed as a global
 * function in the "avro" package, and not as a method of the AvroValue
 * class.
 */

static int
l_value_encode_raw(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    if (!lua_islightuserdata(L, 2)) {
        return luaL_error(L, "Destination buffer should be a light userdata");
    }
    void  *buf = lua_touserdata(L, 2);
    size_t  size = luaL_checkinteger(L, 3);

    avro_writer_t  writer = avro_writer_memory(buf, size);
    int  result = avro_value_write(writer, value);
    avro_writer_free(writer);

    if (result) {
        lua_pushboolean(L, false);
        lua_pushstring(L, avro_strerror());
        return 2;
    }

    lua_pushboolean(L, true);
    return 1;
}


/**
 * Finalizes an AvroValue instance.
 */

static int
l_value_gc(lua_State *L)
{
    LuaAvroValue  *l_value = luaL_checkudata(L, 1, MT_AVRO_VALUE);
    if (l_value->destructor == GENERIC_DESTRUCTOR &&
        l_value->value.self != NULL) {
        avro_generic_value_free(&l_value->value);
    }
    if (l_value->destructor == RESOLVED_READER_DESTRUCTOR &&
        l_value->value.self != NULL) {
        avro_resolved_reader_free_value(&l_value->value);
    }
    l_value->value.iface = NULL;
    l_value->value.self = NULL;
    l_value->destructor = false;
    return 0;
}


/**
 * Creates a new AvroValue instance from a JSON string.
 */

static int
l_value_new(lua_State *L)
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
    avro_value_iface_t  *iface;
} LuaAvroSchema;


int
lua_avro_push_schema(lua_State *L, avro_schema_t schema)
{
    LuaAvroSchema  *l_schema;

    l_schema = lua_newuserdata(L, sizeof(LuaAvroSchema));
    l_schema->schema = avro_schema_incref(schema);
    l_schema->iface = avro_generic_class_from_schema(schema);
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
 * Creates a new AvroValue for the given schema.
 */

static int
l_schema_new_value(lua_State *L)
{
    LuaAvroSchema  *l_schema = luaL_checkudata(L, 1, MT_AVRO_SCHEMA);
    avro_value_t  value;
    check(avro_generic_value_new(l_schema->iface, &value));
    lua_avro_push_value(L, &value, GENERIC_DESTRUCTOR);
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
    if (l_schema->schema != NULL) {
        avro_schema_decref(l_schema->schema);
        l_schema->schema = NULL;
    }
    if (l_schema->iface != NULL) {
        avro_value_iface_decref(l_schema->iface);
        l_schema->iface = NULL;
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
        lua_pushliteral(L, "Error parsing JSON schema");
        return lua_error(L);
    }

    lua_avro_push_schema(L, schema);
    avro_schema_decref(schema);
    return 1;
}


/**
 * Creates a new array schema from the given items schema.
 */

static int
l_schema_new_array(lua_State *L)
{
    avro_schema_t  items_schema = lua_avro_get_schema(L, 1);
    avro_schema_t  schema = avro_schema_array(items_schema);
    if (schema == NULL) {
        return lua_avro_error(L);
    }
    lua_avro_push_schema(L, schema);
    avro_schema_decref(schema);
    return 1;
}


/*-----------------------------------------------------------------------
 * Lua access — resolved readers
 */

/**
 * The string used to identify the AvroResolvedReader class's metatable
 * in the Lua registry.
 */

#define MT_AVRO_RESOLVED_READER "avro:AvroResolvedReader"

typedef struct _LuaAvroResolvedReader
{
    avro_value_iface_t  *resolver;
} LuaAvroResolvedReader;


int
lua_avro_push_resolved_reader(lua_State *L, avro_value_iface_t *resolver)
{
    LuaAvroResolvedReader  *l_resolver;

    l_resolver = lua_newuserdata(L, sizeof(LuaAvroResolvedReader));
    l_resolver->resolver = resolver;
    luaL_getmetatable(L, MT_AVRO_RESOLVED_READER);
    lua_setmetatable(L, -2);
    return 1;
}


avro_value_iface_t *
lua_avro_get_resolved_reader(lua_State *L, int index)
{
    LuaAvroResolvedReader  *l_resolver =
        luaL_checkudata(L, index, MT_AVRO_RESOLVED_READER);
    return l_resolver->resolver;
}


/**
 * Creates a new AvroResolvedReader for the given schemas.
 */

static int
l_resolved_reader_new(lua_State *L)
{
    avro_schema_t  writer_schema = lua_avro_get_schema(L, 1);
    avro_schema_t  reader_schema = lua_avro_get_schema(L, 2);
    avro_value_iface_t  *resolver =
        avro_resolved_reader_new(writer_schema, reader_schema);
    if (resolver == NULL) {
        return lua_return_avro_error(L);
    } else {
        lua_avro_push_resolved_reader(L, resolver);
        return 1;
    }
}


/**
 * Finalizes an AvroResolvedReader instance.
 */

static int
l_resolved_reader_gc(lua_State *L)
{
    LuaAvroResolvedReader  *l_resolver =
        luaL_checkudata(L, 1, MT_AVRO_RESOLVED_READER);
    if (l_resolver->resolver != NULL) {
        avro_value_iface_decref(l_resolver->resolver);
        l_resolver->resolver = NULL;
    }
    return 0;
}


/**
 * Creates a new AvroValue for the given resolved reader.
 */

static int
l_resolved_reader_new_value(lua_State *L)
{
    avro_value_iface_t  *resolver = lua_avro_get_resolved_reader(L, 1);
    avro_value_t  value;
    check(avro_resolved_reader_new_value(resolver, &value));
    lua_avro_push_value(L, &value, RESOLVED_READER_DESTRUCTOR);
    return 1;
}


/*-----------------------------------------------------------------------
 * Lua access — resolved writers
 */

/**
 * The string used to identify the AvroResolvedWriter class's metatable
 * in the Lua registry.
 */

#define MT_AVRO_RESOLVED_WRITER "avro:AvroResolvedWriter"

typedef struct _LuaAvroResolvedWriter
{
    avro_value_iface_t  *resolver;
    avro_value_t  value;
} LuaAvroResolvedWriter;


int
lua_avro_push_resolved_writer(lua_State *L, avro_value_iface_t *resolver)
{
    LuaAvroResolvedWriter  *l_resolver;

    l_resolver = lua_newuserdata(L, sizeof(LuaAvroResolvedWriter));
    l_resolver->resolver = resolver;
    avro_resolved_writer_new_value(resolver, &l_resolver->value);
    luaL_getmetatable(L, MT_AVRO_RESOLVED_WRITER);
    lua_setmetatable(L, -2);
    return 1;
}


avro_value_iface_t *
lua_avro_get_resolved_writer(lua_State *L, int index)
{
    LuaAvroResolvedWriter  *l_resolver =
        luaL_checkudata(L, index, MT_AVRO_RESOLVED_WRITER);
    return l_resolver->resolver;
}


/**
 * Creates a new AvroResolvedWriter for the given schemas.
 */

static int
l_resolved_writer_new(lua_State *L)
{
    avro_schema_t  writer_schema = lua_avro_get_schema(L, 1);
    avro_schema_t  reader_schema = lua_avro_get_schema(L, 2);
    avro_value_iface_t  *resolver =
        avro_resolved_writer_new(writer_schema, reader_schema);
    if (resolver == NULL) {
        return lua_return_avro_error(L);
    } else {
        lua_avro_push_resolved_writer(L, resolver);
        return 1;
    }
}


/**
 * Finalizes an AvroResolvedWriter instance.
 */

static int
l_resolved_writer_gc(lua_State *L)
{
    LuaAvroResolvedWriter  *l_resolver =
        luaL_checkudata(L, 1, MT_AVRO_RESOLVED_WRITER);
    if (l_resolver->value.self != NULL) {
        avro_resolved_writer_free_value(&l_resolver->value);
        l_resolver->value.iface = NULL;
        l_resolver->value.self = NULL;
    }
    if (l_resolver->resolver != NULL) {
        avro_value_iface_decref(l_resolver->resolver);
        l_resolver->resolver = NULL;
    }
    return 0;
}


/**
 * Decode an Avro value using the given resolver.
 */

static int
l_resolved_writer_decode(lua_State *L)
{
    LuaAvroResolvedWriter  *l_resolver =
        luaL_checkudata(L, 1, MT_AVRO_RESOLVED_WRITER);
    size_t  size = 0;
    const char  *buf = luaL_checklstring(L, 2, &size);
    avro_value_t  *value = lua_avro_get_value(L, 3);

    avro_reader_t  reader = avro_reader_memory(buf, size);
    avro_resolved_writer_set_dest(&l_resolver->value, value);
    int rc = avro_value_read(reader, &l_resolver->value);
    avro_reader_free(reader);

    if (rc != 0) {
        return lua_return_avro_error(L);
    }

    lua_pushboolean(L, true);
    return 1;
}


/**
 * Decode an Avro value, using the binary encoding, from the given
 * memory region, which is provided as a light user data and a size.
 * There's no safety checking here; to make it easier to not include
 * this function in sandboxes, it's exposed as a global function in the
 * "avro" package, and not as a method of the AvroValue class.
 */

static int
l_value_decode_raw(lua_State *L)
{
    LuaAvroResolvedWriter  *l_resolver =
        luaL_checkudata(L, 1, MT_AVRO_RESOLVED_WRITER);
    if (!lua_islightuserdata(L, 2)) {
        return luaL_error(L, "Destination buffer should be a light userdata");
    }
    void  *buf = lua_touserdata(L, 2);
    size_t  size = luaL_checkinteger(L, 3);
    avro_value_t  *value = lua_avro_get_value(L, 4);

    avro_reader_t  reader = avro_reader_memory(buf, size);
    avro_resolved_writer_set_dest(&l_resolver->value, value);
    int rc = avro_value_read(reader, &l_resolver->value);
    avro_reader_free(reader);

    if (rc != 0) {
        return lua_return_avro_error(L);
    }

    lua_pushboolean(L, true);
    return 1;
}


/*-----------------------------------------------------------------------
 * Lua access — data files
 */

/**
 * The string used to identify the AvroDataInputFile class's metatable
 * in the Lua registry.
 */

#define MT_AVRO_DATA_INPUT_FILE "avro:AvroDataInputFile"


typedef struct _LuaAvroDataInputFile
{
    avro_file_reader_t  reader;
    avro_schema_t  wschema;
    avro_value_iface_t  *iface;
} LuaAvroDataInputFile;

int
lua_avro_push_file_reader(lua_State *L, avro_file_reader_t reader)
{
    LuaAvroDataInputFile  *l_file;

    l_file = lua_newuserdata(L, sizeof(LuaAvroDataInputFile));
    l_file->reader = reader;
    l_file->wschema = avro_file_reader_get_writer_schema(reader);
    l_file->iface = avro_generic_class_from_schema(l_file->wschema);
    luaL_getmetatable(L, MT_AVRO_DATA_INPUT_FILE);
    lua_setmetatable(L, -2);
    return 1;
}


avro_file_reader_t
lua_avro_get_file_reader(lua_State *L, int index)
{
    LuaAvroDataInputFile  *l_file =
        luaL_checkudata(L, index, MT_AVRO_DATA_INPUT_FILE);
    return l_file->reader;
}


/**
 * Closes a file reader.
 */

static int
l_input_file_close(lua_State *L)
{
    LuaAvroDataInputFile  *l_file =
        luaL_checkudata(L, 1, MT_AVRO_DATA_INPUT_FILE);
    if (l_file->reader != NULL) {
        avro_file_reader_close(l_file->reader);
        l_file->reader = NULL;
    }
    l_file->wschema = NULL;
    if (l_file->iface != NULL) {
        avro_value_iface_decref(l_file->iface);
        l_file->iface = NULL;
    }
    return 0;
}

/**
 * Reads a value from a file reader.
 */

static int
l_input_file_read(lua_State *L)
{
    int  nargs = lua_gettop(L);
    LuaAvroDataInputFile  *l_file =
        luaL_checkudata(L, 1, MT_AVRO_DATA_INPUT_FILE);

    if (nargs == 1) {
        /* No Value instance given, so create one. */
        avro_value_t  value;
        check(avro_generic_value_new(l_file->iface, &value));
        int  rc = avro_file_reader_read_value(l_file->reader, &value);
        if (rc != 0) {
            return lua_return_avro_error(L);
        }
        lua_avro_push_value(L, &value, GENERIC_DESTRUCTOR);
        return 1;
    }

    else {
        /* Otherwise read into the given value. */
        avro_value_t  *value = lua_avro_get_value(L, 2);
        int  rc = avro_file_reader_read_value(l_file->reader, value);
        if (rc != 0) {
            return lua_return_avro_error(L);
        }
        lua_pushvalue(L, 2);
        return 1;
    }
}


/**
 * The string used to identify the AvroDataOutputFile class's metatable
 * in the Lua registry.
 */

#define MT_AVRO_DATA_OUTPUT_FILE "avro:AvroDataOutputFile"


typedef struct _LuaAvroDataOutputFile
{
    avro_file_writer_t  writer;
} LuaAvroDataOutputFile;


int
lua_avro_push_file_writer(lua_State *L, avro_file_writer_t writer)
{
    LuaAvroDataOutputFile  *l_file;

    l_file = lua_newuserdata(L, sizeof(LuaAvroDataOutputFile));
    l_file->writer = writer;
    luaL_getmetatable(L, MT_AVRO_DATA_OUTPUT_FILE);
    lua_setmetatable(L, -2);
    return 1;
}


avro_file_writer_t
lua_avro_get_file_writer(lua_State *L, int index)
{
    LuaAvroDataOutputFile  *l_file =
        luaL_checkudata(L, index, MT_AVRO_DATA_OUTPUT_FILE);
    return l_file->writer;
}


/**
 * Closes a file writer.
 */

static int
l_output_file_close(lua_State *L)
{
    LuaAvroDataOutputFile  *l_file =
        luaL_checkudata(L, 1, MT_AVRO_DATA_OUTPUT_FILE);
    if (l_file->writer != NULL) {
        avro_file_writer_close(l_file->writer);
        l_file->writer = NULL;
    }
    return 0;
}

/**
 * Writes a value to a file writer.
 */

static int
l_output_file_write(lua_State *L)
{
    avro_file_writer_t  writer = lua_avro_get_file_writer(L, 1);

    avro_value_t  *value = lua_avro_get_value(L, 2);
    check(avro_file_writer_append_value(writer, value));
    return 0;
}


/**
 * Opens a new input or output file.
 */

static int
l_file_open(lua_State *L)
{
    static const char  *MODES[] = { "r", "w", NULL };

    const char  *path = luaL_checkstring(L, 1);
    int  mode = luaL_checkoption(L, 2, "r", MODES);

    if (mode == 0) {
        /* mode == "r" */
        avro_file_reader_t  reader;
        int  rc = avro_file_reader(path, &reader);
        if (rc != 0) {
            return lua_return_avro_error(L);
        }
        lua_avro_push_file_reader(L, reader);
        return 1;

    } else if (mode == 1) {
        /* mode == "w" */
        avro_schema_t  schema = lua_avro_get_schema(L, 3);
        avro_file_writer_t  writer;
        int  rc = avro_file_writer_create(path, schema, &writer);
        if (rc != 0) {
            return lua_return_avro_error(L);
        }
        lua_avro_push_file_writer(L, writer);
        return 1;
    }

    return 0;
}


/*-----------------------------------------------------------------------
 * Lua access — module
 */

static const luaL_Reg  value_methods[] =
{
    {"append", l_value_append},
    {"discriminant", l_value_discriminant},
    {"encode", l_value_encode},
    {"encoded_size", l_value_encoded_size},
    {"get", l_value_get},
    {"hash", l_value_hash},
    {"iterate", l_value_iterate},
    {"scalar", l_value_scalar},
    {"set", l_value_set},
    {"set_source", l_value_set_source},
    {"type", l_value_type},
    {NULL, NULL}
};


static const luaL_Reg  schema_methods[] =
{
    {"new_value", l_schema_new_value},
    {"type", l_schema_type},
    {NULL, NULL}
};


static const luaL_Reg  resolved_reader_methods[] =
{
    {"new_value", l_resolved_reader_new_value},
    {NULL, NULL}
};


static const luaL_Reg  resolved_writer_methods[] =
{
    {"decode", l_resolved_writer_decode},
    {NULL, NULL}
};


static const luaL_Reg  input_file_methods[] =
{
    {"close", l_input_file_close},
    {"read", l_input_file_read},
    {NULL, NULL}
};


static const luaL_Reg  output_file_methods[] =
{
    {"close", l_output_file_close},
    {"write", l_output_file_write},
    {NULL, NULL}
};


static const luaL_Reg  mod_methods[] =
{
    {"ArraySchema", l_schema_new_array},
    {"ResolvedReader", l_resolved_reader_new},
    {"ResolvedWriter", l_resolved_writer_new},
    {"Schema", l_schema_new},
    {"Value", l_value_new},
    {"open", l_file_open},
    {"raw_decode_value", l_value_decode_raw},
    {"raw_encode_value", l_value_encode_raw},
    {NULL, NULL}
};


#define set_avro_const2(s1, s2)    \
    lua_pushinteger(L, AVRO_##s1); \
    lua_setfield(L, -2, #s2);

#define set_avro_const(s)         \
    lua_pushinteger(L, AVRO_##s); \
    lua_setfield(L, -2, #s);


int
luaopen_avro_c_legacy(lua_State *L)
{
    /* AvroSchema metatable */

    luaL_newmetatable(L, MT_AVRO_SCHEMA);
    lua_createtable(L, 0, sizeof(schema_methods) / sizeof(luaL_reg) - 1);
    luaL_register(L, NULL, schema_methods);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_schema_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    /* AvroValue metatables */

    luaL_newmetatable(L, MT_AVRO_VALUE_METHODS);
    luaL_register(L, NULL, value_methods);
    lua_pop(L, 1);

    luaL_newmetatable(L, MT_AVRO_VALUE);
    lua_pushcfunction(L, l_value_eq);
    lua_setfield(L, -2, "__eq");
    lua_pushcfunction(L, l_value_tostring);
    lua_setfield(L, -2, "__tostring");
    lua_pushcfunction(L, l_value_index);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_value_newindex);
    lua_setfield(L, -2, "__newindex");
    lua_pushcfunction(L, l_value_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    luaL_newmetatable(L, MT_ITERATOR);
    lua_pushcfunction(L, iterator_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    /* AvroResolvedReader metatable */

    luaL_newmetatable(L, MT_AVRO_RESOLVED_READER);
    lua_createtable(L, 0, sizeof(resolved_reader_methods) / sizeof(luaL_reg) - 1);
    luaL_register(L, NULL, resolved_reader_methods);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_resolved_reader_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    /* AvroResolvedWriter metatable */

    luaL_newmetatable(L, MT_AVRO_RESOLVED_WRITER);
    lua_createtable(L, 0, sizeof(resolved_writer_methods) / sizeof(luaL_reg) - 1);
    luaL_register(L, NULL, resolved_writer_methods);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_resolved_writer_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    /* AvroInputFile metatable */

    luaL_newmetatable(L, MT_AVRO_DATA_INPUT_FILE);
    lua_createtable(L, 0, sizeof(input_file_methods) / sizeof(luaL_reg) - 1);
    luaL_register(L, NULL, input_file_methods);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_input_file_close);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    /* AvroOutputFile metatable */

    luaL_newmetatable(L, MT_AVRO_DATA_OUTPUT_FILE);
    lua_createtable(L, 0, sizeof(output_file_methods) / sizeof(luaL_reg) - 1);
    luaL_register(L, NULL, output_file_methods);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_output_file_close);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    luaL_register(L, "avro.c.legacy", mod_methods);

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

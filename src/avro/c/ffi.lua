-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011, RedJack, LLC.
-- All rights reserved.
--
-- Please see the LICENSE.txt file in this distribution for license
-- details.
------------------------------------------------------------------------

-- A LuaJIT FFI implementation of the Avro C bindings.

-- NOTE: This module assumes that the FFI is available.  It will raise
-- an error if it's not.  The avro.c module checks for its availability,
-- and loads in this module, or avro.c.legacy, as appropriate.

local ffi = require "ffi"

local error = error
local print = print
local type = type

module "avro.c.ffi"

local avro = ffi.load("avro")

ffi.cdef [[
typedef int  avro_type_t;
typedef int  avro_class_t;

typedef struct avro_obj_t {
    avro_type_t  type;
    avro_class_t  class_type;
    volatile int  refcount;
} avro_obj_t;

const char *
avro_strerror(void);
]]

local function avro_error()
   error(ffi.string(avro.avro_strerror()))
end

------------------------------------------------------------------------
-- Forward declarations

ffi.cdef [[
typedef avro_obj_t  *avro_schema_t;

typedef struct LuaAvroSchema {
    avro_schema_t  schema;
} LuaAvroSchema;

typedef avro_obj_t  *avro_datum_t;

typedef struct LuaAvroValue {
    avro_datum_t  datum;
} LuaAvroValue;

typedef struct avro_consumer_t  avro_consumer_t;

typedef struct LuaAvroResolver {
    avro_consumer_t  *resolver;
} LuaAvroResolver;
]]

local avro_schema_t = ffi.typeof([[avro_schema_t]])
local LuaAvroSchema

local avro_datum_t = ffi.typeof([[avro_datum_t]])
local LuaAvroValue

local avro_consumer_t = ffi.typeof([[avro_consumer_t *]])
local LuaAvroConsumer


------------------------------------------------------------------------
-- Constants

STRING  =  0
BYTES   =  1
INT     =  2
LONG    =  3
FLOAT   =  4
DOUBLE  =  5
BOOLEAN =  6
NULL    =  7
RECORD  =  8
ENUM    =  9
FIXED   = 10
MAP     = 11
ARRAY   = 12
UNION   = 13
LINK    = 14


------------------------------------------------------------------------
-- C type pointers

ffi.cdef [[
typedef struct avro_schema_error_t  *avro_schema_error_t;
typedef struct avro_reader_t  *avro_reader_t;
typedef struct avro_writer_t  *avro_writer_t;
]]

local char_p_ptr = ffi.typeof([=[ char *[1] ]=])
local const_char_p_ptr = ffi.typeof([=[ const char *[1] ]=])
local double_ptr = ffi.typeof([=[ double[1] ]=])
local float_ptr = ffi.typeof([=[ float[1] ]=])
local int_ptr = ffi.typeof([=[ int[1] ]=])
local int8_t_ptr = ffi.typeof([=[ int8_t[1] ]=])
local int32_t_ptr = ffi.typeof([=[ int32_t[1] ]=])
local int64_t_ptr = ffi.typeof([=[ int64_t[1] ]=])
local size_t_ptr = ffi.typeof([=[ size_t[1] ]=])

local avro_datum_t_ptr = ffi.typeof([=[ avro_datum_t[1] ]=])
local avro_schema_t_ptr = ffi.typeof([=[ avro_schema_t[1] ]=])
local avro_schema_error_t_ptr = ffi.typeof([=[ avro_schema_error_t[1] ]=])


------------------------------------------------------------------------
-- Function declarations

-- avro.h

ffi.cdef [[
void *malloc(size_t size);
void free(void *ptr);

int
avro_array_append_datum(avro_datum_t array_datum, avro_datum_t datum);

int
avro_array_get(const avro_datum_t datum, int64_t index, avro_datum_t *value);

size_t
avro_array_size(const avro_datum_t datum);

int
avro_boolean_get(avro_datum_t datum, int8_t *val);

int
avro_boolean_set(avro_datum_t datum, int8_t val);

int
avro_bytes_get(avro_datum_t datum, char **val, int64_t *size);

int
avro_bytes_set(avro_datum_t datum, const char *val, const int64_t size);

void
avro_datum_decref(avro_datum_t schema);

avro_datum_t
avro_datum_from_schema(const avro_schema_t schema);

avro_schema_t
avro_datum_get_schema(const avro_datum_t datum);

int
avro_datum_to_json(const avro_datum_t datum,
                   int one_line, char **json_str);

int
avro_double_get(avro_datum_t datum, double *val);

int
avro_double_set(avro_datum_t datum, double val);

const char *
avro_enum_get_name(const avro_datum_t datum);

int
avro_enum_set_name(avro_datum_t datum, const char *val);

int
avro_fixed_get(avro_datum_t datum, char **val, int64_t *size);

int
avro_fixed_set(avro_datum_t datum, const char *val, int64_t size);

int
avro_float_get(avro_datum_t datum, float *val);

int
avro_float_set(avro_datum_t datum, float val);

int
avro_int32_get(avro_datum_t datum, int32_t *val);

int
avro_int32_set(avro_datum_t datum, int32_t val);

int
avro_int64_get(avro_datum_t datum, int64_t *val);

int
avro_int64_set(avro_datum_t datum, int64_t val);

int
avro_map_get(const avro_datum_t datum, const char *key, avro_datum_t *value);

int
avro_map_get_key(const avro_datum_t datum, int index, const char **key);

int
avro_map_set(avro_datum_t datum, const char *key, avro_datum_t value);

size_t
avro_map_size(const avro_datum_t datum);

avro_reader_t
avro_reader_memory(const char *buf, int64_t len);

void
avro_reader_free(avro_reader_t reader);

int
avro_record_get(const avro_datum_t datum, const char *name, avro_datum_t *value);

avro_schema_t
avro_schema_array_items(avro_schema_t schema);

void
avro_schema_decref(avro_schema_t schema);

int
avro_schema_from_json(const char *json_str, const int32_t json_len,
                      avro_schema_t *schema, avro_schema_error_t *err);

avro_schema_t
avro_schema_map_values(avro_schema_t schema);

const char *
avro_schema_type_name(const avro_schema_t schema);

avro_schema_t
avro_schema_union_branch(avro_schema_t schema, int discriminant);

int
avro_string_get(avro_datum_t datum, char **val);

int
avro_string_set(avro_datum_t datum, const char *val);

avro_datum_t
avro_union_current_branch(avro_datum_t datum);

int64_t
avro_union_discriminant(const avro_datum_t datum);

int
avro_union_set_discriminant(avro_datum_t datum, int discriminant,
                            avro_datum_t *branch);

avro_writer_t
avro_writer_memory(char *buf, int64_t len);

void
avro_writer_free(avro_writer_t writer);
]]

-- avro/consumer.h

ffi.cdef [[
void
avro_consumer_free(avro_consumer_t *consumer);

int
avro_consume_binary(avro_reader_t reader, avro_consumer_t *consumer, void *ud);

int
avro_consume_datum(const avro_datum_t datum, avro_consumer_t *consumer, void *ud);

avro_consumer_t *
avro_encoding_consumer_new(void);

avro_consumer_t *
avro_resolver_new(avro_schema_t writer, avro_schema_t reader);

avro_consumer_t *
avro_sizeof_consumer_new(void);
]]

------------------------------------------------------------------------
-- Schemas

local Schema_class = {}
local Schema_mt = { __index = Schema_class }

function Schema_class:new_value()
   local datum = avro.avro_datum_from_schema(self.schema)
   if datum == nil then avro_error() end
   return LuaAvroValue(datum)
end

function Schema_class:type()
   return self.schema[0].type
end

function Schema_mt:__gc()
   if self.schema ~= nil then
      avro.avro_schema_decref(self.schema)
      self.schema = nil
   end
end

function Schema(json)
   local json_len = #json
   local schema = ffi.new(avro_schema_t_ptr)
   local schema_error = ffi.new(avro_schema_error_t_ptr)
   local rc = avro.avro_schema_from_json(json, json_len, schema, schema_error)
   if rc ~= 0 then avro_error() end
   return LuaAvroSchema(schema[0])
end

LuaAvroSchema = ffi.metatype([[LuaAvroSchema]], Schema_mt)

------------------------------------------------------------------------
-- Values

local Value_class = {}
local Value_mt = {}

-- A helper method that returns the Lua equivalent for scalar values.
local function lua_scalar(datum)
   local datum_type = datum[0].type
   if datum_type == BOOLEAN then
      local val = ffi.new(int8_t_ptr)
      avro.avro_boolean_get(datum, val)
      return true, val[0] ~= 0
   elseif datum_type == BYTES then
      local val = ffi.new(char_p_ptr)
      local size = ffi.new(int64_t_ptr)
      avro.avro_bytes_get(datum, val, size)
      return true, ffi.string(val[0], size[0])
   elseif datum_type == DOUBLE then
      local val = ffi.new(double_ptr)
      avro.avro_double_get(datum, val)
      return true, val[0]
   elseif datum_type == FLOAT then
      local val = ffi.new(float_ptr)
      avro.avro_float_get(datum, val)
      return true, val[0]
   elseif datum_type == INT then
      local val = ffi.new(int32_t_ptr)
      avro.avro_int32_get(datum, val)
      return true, val[0]
   elseif datum_type == LONG then
      local val = ffi.new(int64_t_ptr)
      avro.avro_int64_get(datum, val)
      return true, val[0]
   elseif datum_type == NULL then
      return true, nil
   elseif datum_type == STRING then
      local val = ffi.new(char_p_ptr)
      avro.avro_string_get(datum, val)
      return true, ffi.string(val[0])
   elseif datum_type == ENUM then
      return true, ffi.string(avro.avro_enum_get_name(datum))
   elseif datum_type == FIXED then
      local val = ffi.new(char_p_ptr)
      local size = ffi.new(int64_t_ptr)
      avro.avro_fixed_get(datum, val, size)
      return true, ffi.string(val[0], size[0])
   else
      return false
   end
end

-- A helper method that returns a LuaAvroValue wrapper for non-scalar
-- values, and the Lua equivalent for scalar values.
local function scalar_or_wrapper(datum)
   local is_scalar, scalar = lua_scalar(datum)
   if is_scalar then
      return scalar
   else
      return LuaAvroValue(datum)
   end
end

-- A helper method that sets the content of a scalar value.  If the
-- value isn't a scalar, we raise an error.
local function set_scalar(datum, val)
   local datum_type = datum[0].type
   if datum_type == BOOLEAN then
      avro.avro_boolean_set(datum, val)
      return
   elseif datum_type == BYTES then
      avro.avro_bytes_set(datum, val, #val)
      return
   elseif datum_type == DOUBLE then
      avro.avro_double_set(datum, val)
      return
   elseif datum_type == FLOAT then
      avro.avro_float_set(datum, val)
      return
   elseif datum_type == INT then
      avro.avro_int32_set(datum, val)
      return
   elseif datum_type == LONG then
      avro.avro_int64_set(datum, val)
      return
   elseif datum_type == NULL then
      return
   elseif datum_type == STRING then
      avro.avro_string_set(datum, val)
      return
   elseif datum_type == ENUM then
      return true, ffi.string(avro.avro_enum_set_name(datum))
   elseif datum_type == FIXED then
      avro.avro_fixed_set(datum, val, #val)
      return
   else
      error("Avro value isn't a scalar")
   end
end

function Value_class:append(element_val)
   if self.datum[0].type ~= ARRAY then
      error("Can only append to an array")
   end

   local array_schema = avro.avro_datum_get_schema(self.datum)
   local element_schema = avro.avro_schema_array_items(array_schema)
   local element = avro.avro_datum_from_schema(element_schema)
   local rc = avro.avro_array_append_datum(self.datum, element)
   if rc ~= 0 then avro_error() end

   if element_val then
      set_scalar(element, element_val)
   end

   return LuaAvroValue(element)
end

function Value_class:discriminant()
   if self.datum[0].type ~= UNION then
      error("Can't get discriminant of a non-union value")
   end

   local discriminant = avro.avro_union_discriminant(self.datum)
   local union_schema = avro.avro_datum_get_schema(self.datum)
   local branch = avro.avro_schema_union_branch(union_schema, discriminant)
   return ffi.string(avro.avro_schema_type_name(branch))
end

local encoding_consumer = avro.avro_encoding_consumer_new()
local sizeof_consumer = avro.avro_sizeof_consumer_new()

local static_buf = ffi.new([[ char[65536] ]])

function Value_class:encode()
   local size = ffi.new(size_t_ptr)
   avro.avro_consume_datum(self.datum, sizeof_consumer, size)

   -- Use the static buffer if we, to save on some mallocs.
   local buf, free_buf
   if size[0] <= ffi.sizeof(static_buf) then
      buf = static_buf
      free_buf = false
   else
      buf = ffi.C.malloc(size[0])
      if buf == nil then return nil, "Out of memory" end
      free_buf = true
   end

   local writer = avro.avro_writer_memory(buf, size[0])
   local rc = avro.avro_consume_datum(self.datum, encoding_consumer, writer)
   avro.avro_writer_free(writer)

   if rc ~= 0 then
      if free_buf then ffi.C.free(buf) end
      return nil, ffi.string(avro.avro_strerror())
   else
      local result = ffi.string(buf, size[0])
      if free_buf then ffi.C.free(buf) end
      return result
   end
end

function Value_class:encoded_size()
   local size = ffi.new(size_t_ptr)
   avro.avro_consume_datum(self.datum, sizeof_consumer, size)
   return size[0]
end

function raw_encode_value(self, buf, size)
   local writer = avro.avro_writer_memory(buf, size)
   local rc = avro.avro_consume_datum(self.datum, encoding_consumer, writer)
   avro.avro_writer_free(writer)
   if rc == 0 then
      return true
   else
      return false, ffi.string(avro.avro_strerror())
   end
end

function Value_class:get(index)
   if type(index) == "number" then
      if self.datum[0].type == ARRAY then
         local size = avro.avro_array_size(self.datum)
         if index < 1 or index > size then
            return nil, "Index out of bounds"
         end
         local element = ffi.new(avro_datum_t_ptr)
         avro.avro_array_get(self.datum, index-1, element)
         return scalar_or_wrapper(element[0])
      end

      return nil, "Can only get integer index from arrays"

   elseif type(index) == "string" then
      local datum_type = self.datum[0].type

      if datum_type == MAP then
         local element = ffi.new(avro_datum_t_ptr)
         avro.avro_map_get(self.datum, index, element)
         if element[0] == nil then
            return nil, "Map element doesn't exist"
         end
         return scalar_or_wrapper(element[0])

      elseif datum_type == RECORD then
         local field = ffi.new(avro_datum_t_ptr)
         avro.avro_record_get(self.datum, index, field)
         if field[0] == nil then
            return nil, "Record field doesn't exist"
         end
         return scalar_or_wrapper(field[0])

      elseif datum_type == UNION then
         if index == "_" then
            local branch = avro.avro_union_current_branch(self.datum)
            return scalar_or_wrapper(branch)
         else
            local union_schema = avro.avro_datum_get_schema(self.datum)
            local discriminant = ffi.new(int_ptr)
            local branch_schema = avro.avro_schema_union_branch_by_name(
               union_schema, discriminant, index
            )
            if branch_schema == nil then
               return nil, ffi.string(avro.avro_strerror())
            end
            local branch = ffi.new(avro_datum_t_ptr)
            local rc = avro.avro_union_set_discriminant(
               self.datum, discriminant[0], branch
            )
            if rc ~= 0 then
               return nil, ffi.string(avro.avro_strerror())
            end
            return scalar_or_wrapper(branch[0])
         end
      end

      return nil, "Can only get string index from map, record, or union"
   end

   return nil, "Can only get integer or string index"
end

local function iterate_array(state, unused)
   -- NOTE: state.next_index is 0-based
   -- Have we reached the end?
   if state.next_index >= state.length then return nil end
   -- Nope.
   local element = ffi.new(avro_datum_t_ptr)
   avro.avro_array_get(state.datum, state.next_index, element)
   state.next_index = state.next_index + 1
   -- Result should be a 1-based index for Lua
   return state.next_index, scalar_or_wrapper(element[0])
end

local function iterate_map(state, unused)
   -- NOTE: state.next_index is 0-based
   -- Have we reached the end?
   if state.next_index >= state.length then return nil end
   -- Nope.
   local key = ffi.new(const_char_p_ptr)
   local element = ffi.new(avro_datum_t_ptr)
   avro.avro_map_get_key(state.datum, state.next_index, key)
   avro.avro_map_get(state.datum, key[0], element)
   state.next_index = state.next_index + 1
   return ffi.string(key[0]), scalar_or_wrapper(element[0])
end

function Value_class:iterate()
   local datum_type = self.datum[0].type

   if datum_type == ARRAY then
      local state = {
         datum = self.datum,
         next_index = 0,
         length = avro.avro_array_size(self.datum),
      }
      return iterate_array, state, nil

   elseif datum_type == MAP then
      local state = {
         datum = self.datum,
         next_index = 0,
         length = avro.avro_map_size(self.datum),
      }
      return iterate_map, state, nil
   end
end

function Value_class:scalar()
   local is_scalar, scalar = lua_scalar(self.datum)
   if is_scalar then
      return scalar
   else
      error("Value isn't a scalar")
   end
end

local function create_element(datum, index)
   if type(index) == "number" then
      if datum[0].type == ARRAY then
         local size = avro.avro_array_size(datum)
         if index < 1 or index > size then
            return nil, "Index out of bounds"
         end
         local element = ffi.new(avro_datum_t_ptr)
         avro.avro_array_get(datum, index-1, element)
         return element[0]
      end

      return nil, "Can only get integer index from arrays"

   elseif type(index) == "string" then
      local datum_type = datum[0].type

      if datum_type == MAP then
         local element = ffi.new(avro_datum_t_ptr)
         avro.avro_map_get(datum, index, element)
         if element[0] == nil then
            local map_schema = avro.avro_datum_get_schema(datum)
            local element_schema = avro.avro_schema_map_values(map_schema)
            element[0] = avro.avro_datum_from_schema(element_schema)
            avro.avro_map_set(datum, index, element[0])
         end
         return element[0]

      elseif datum_type == RECORD then
         local field = ffi.new(avro_datum_t_ptr)
         avro.avro_record_get(datum, index, field)
         if field[0] == nil then
            return nil, "Record field doesn't exist"
         end
         return field[0]

      elseif datum_type == UNION then
         if index == "_" then
            return avro.avro_union_current_branch(datum)
         else
            local union_schema = avro.avro_datum_get_schema(datum)
            local discriminant = ffi.new(int_ptr)
            local branch_schema = avro.avro_schema_union_branch_by_name(
               union_schema, discriminant, index
            )
            if branch_schema == nil then
               return nil, ffi.string(avro.avro_strerror())
            end
            local branch = ffi.new(avro_datum_t_ptr)
            local rc = avro.avro_union_set_discriminant(
               datum, discriminant[0], branch
            )
            if rc ~= 0 then
               return nil, ffi.string(avro.avro_strerror())
            end
            return branch[0]
         end
      end

      return nil, "Can only get string index from map, record, or union"
   end

   return nil, "Can only get integer or string index"
end

function Value_class:set(arg1, arg2)
   if arg2 then
      local index, val = arg1, arg2
      local element, err = create_element(self.datum, index)
      if not element then return element, err end
      set_scalar(element, val)
   else
      set_scalar(self.datum, arg1)
   end
end

function Value_class:type()
   return self.datum[0].type
end

function Value_mt:__tostring()
   local json = ffi.new(char_p_ptr)
   local rc = avro.avro_datum_to_json(self.datum, true, json)
   if rc ~= 0 then avro_error() end
   local result = ffi.string(json[0])
   ffi.C.free(json[0])
   return result
end

function Value_mt:__index(idx)
   -- First try Value_class; if there's a function with the given name,
   -- then that's our result.
   local result = Value_class[idx]
   if result then return result end

   -- Otherwise defer to the get() method.
   return Value_class.get(self, idx)
end

function Value_mt:__newindex(idx)
   -- First try Value_class; if there's a function with the given name,
   -- then you need to use the set() method directly.  (We don't want
   -- the caller to overwrite any methods.)
   local result = Value_class[idx]
   if result then error("Cannot set field with [] syntax") end

   -- Otherwise defer to the set() method.
   return Value_class.set(self, idx)
end

function Value_mt:__gc()
   if self.datum ~= nil then
      avro.avro_datum_decref(self.datum)
      self.datum = nil
   end
end

LuaAvroValue = ffi.metatype([[LuaAvroValue]], Value_mt)

------------------------------------------------------------------------
-- Resolvers

local Resolver_class = {}
local Resolver_mt = { __index = Resolver_class }

function Resolver_class:decode(buf, dest)
   local reader = avro.avro_reader_memory(buf, #buf)
   local void_datum = ffi.cast([[void *]], dest.datum)
   local rc = avro.avro_consume_binary(reader, self.resolver, void_datum)
   avro.avro_reader_free(reader)
   if rc == 0 then
      return true
   else
      return nil, ffi.string(avro.avro_strerror())
   end
end

function Resolver_mt:__gc()
   if self.resolver ~= nil then
      avro.avro_consumer_free(self.resolver)
      self.resolver = nil
   end
end

function Resolver(wschema, rschema)
   local resolver = avro.avro_resolver_new(wschema.schema, rschema.schema)
   if resolver == nil then return nil, ffi.string(avro.avro_strerror()) end
   return LuaAvroResolver(resolver)
end

LuaAvroResolver = ffi.metatype([[LuaAvroResolver]], Resolver_mt)

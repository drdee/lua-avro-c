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
void *malloc(size_t size);
void free(void *ptr);

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

local function get_avro_error()
   return nil, ffi.string(avro.avro_strerror())
end

local function avro_error()
   error(ffi.string(avro.avro_strerror()))
end


------------------------------------------------------------------------
-- Avro value interface

-- Note that the avro_value_t definition below does not exactly match
-- the one from the Avro C library.  We need to store an additional
-- field, indicating whether the value was created with
-- avro_generic_value_new (and therefore should be freed in its __gc
-- metamethod).  Ideally, we'd use a wrapper struct like this:
--
-- typedef struct LuaAvroValue {
--     avro_value_t  value;
--     bool  should_free;
-- } LuaAvroValue;
--
-- Unfortunately, the LuaJIT compiler doesn't currently support
-- JIT-compiling nested structures, and since Avro values will be used
-- in a lot of tight loops, it's important to get those compiled to
-- machine code.
--
-- So to get around this, we're incorporating the extra field into our
-- own definition of avro_value_t.  The beginning of the struct still
-- matches what the library expects, so we should be okay.

ffi.cdef [[
typedef struct avro_value_iface  avro_value_iface_t;

typedef struct avro_value {
	const avro_value_iface_t  *iface;
	void  *self;
        bool  should_free;
} avro_value_t;

typedef avro_obj_t  *avro_schema_t;
typedef struct avro_wrapped_buffer  avro_wrapped_buffer_t;

struct avro_value_iface {
	avro_value_iface_t *(*incref)(avro_value_iface_t *iface);
	void (*decref)(avro_value_iface_t *iface);
	int (*reset)(const avro_value_iface_t *iface, void *self);
	avro_type_t (*get_type)(const avro_value_iface_t *iface, const void *self);
	avro_schema_t (*get_schema)(const avro_value_iface_t *iface, const void *self);
	int (*get_boolean)(const avro_value_iface_t *iface,
			   const void *self, int *out);
	int (*get_bytes)(const avro_value_iface_t *iface,
			 const void *self, const void **buf, size_t *size);
	int (*grab_bytes)(const avro_value_iface_t *iface,
			  const void *self, avro_wrapped_buffer_t *dest);
	int (*get_double)(const avro_value_iface_t *iface,
			  const void *self, double *out);
	int (*get_float)(const avro_value_iface_t *iface,
			 const void *self, float *out);
	int (*get_int)(const avro_value_iface_t *iface,
		       const void *self, int32_t *out);
	int (*get_long)(const avro_value_iface_t *iface,
			const void *self, int64_t *out);
	int (*get_null)(const avro_value_iface_t *iface,
			const void *self);
	int (*get_string)(const avro_value_iface_t *iface,
			  const void *self, const char **str, size_t *size);
	int (*grab_string)(const avro_value_iface_t *iface,
			   const void *self, avro_wrapped_buffer_t *dest);
	int (*get_enum)(const avro_value_iface_t *iface,
			const void *self, int *out);
	int (*get_fixed)(const avro_value_iface_t *iface,
			 const void *self, const void **buf, size_t *size);
	int (*grab_fixed)(const avro_value_iface_t *iface,
			  const void *self, avro_wrapped_buffer_t *dest);
	int (*set_boolean)(const avro_value_iface_t *iface,
			   void *self, int val);
	int (*set_bytes)(const avro_value_iface_t *iface,
			 void *self, void *buf, size_t size);
	int (*give_bytes)(const avro_value_iface_t *iface,
			  void *self, avro_wrapped_buffer_t *buf);
	int (*set_double)(const avro_value_iface_t *iface,
			  void *self, double val);
	int (*set_float)(const avro_value_iface_t *iface,
			 void *self, float val);
	int (*set_int)(const avro_value_iface_t *iface,
		       void *self, int32_t val);
	int (*set_long)(const avro_value_iface_t *iface,
			void *self, int64_t val);
	int (*set_null)(const avro_value_iface_t *iface, void *self);
	int (*set_string)(const avro_value_iface_t *iface,
			  void *self, char *str);
	int (*set_string_len)(const avro_value_iface_t *iface,
			      void *self, char *str, size_t size);
	int (*give_string_len)(const avro_value_iface_t *iface,
			       void *self, avro_wrapped_buffer_t *buf);
	int (*set_enum)(const avro_value_iface_t *iface,
			void *self, int val);
	int (*set_fixed)(const avro_value_iface_t *iface,
			 void *self, void *buf, size_t size);
	int (*give_fixed)(const avro_value_iface_t *iface,
			  void *self, avro_wrapped_buffer_t *buf);
	int (*get_size)(const avro_value_iface_t *iface,
			const void *self, size_t *size);
	int (*get_by_index)(const avro_value_iface_t *iface,
			    const void *self, size_t index,
			    avro_value_t *child, const char **name);
	int (*get_by_name)(const avro_value_iface_t *iface,
			   const void *self, const char *name,
			   avro_value_t *child, size_t *index);
	int (*get_discriminant)(const avro_value_iface_t *iface,
				const void *self, int *out);
	int (*get_current_branch)(const avro_value_iface_t *iface,
				  const void *self, avro_value_t *branch);
	int (*append)(const avro_value_iface_t *iface,
		      void *self, avro_value_t *child_out, size_t *new_index);
	int (*add)(const avro_value_iface_t *iface,
		   void *self, const char *key,
		   avro_value_t *child, size_t *index, int *is_new);
	int (*set_branch)(const avro_value_iface_t *iface,
			  void *self, int discriminant,
			  avro_value_t *branch);
};
]]


------------------------------------------------------------------------
-- Forward declarations

ffi.cdef [[
typedef struct LuaAvroSchema {
    avro_schema_t  schema;
    avro_value_iface_t  *iface;
} LuaAvroSchema;

typedef struct LuaAvroResolvedWriter {
    avro_value_iface_t  *resolver;
    avro_value_t  value;
} LuaAvroResolvedWriter;

typedef struct avro_file_reader_t_  *avro_file_reader_t;
typedef struct avro_file_writer_t_  *avro_file_writer_t;

typedef struct LuaAvroDataInputFile {
    avro_file_reader_t  reader;
    avro_schema_t  wschema;
    avro_value_iface_t  *iface;
} LuaAvroDataInputFile;

typedef struct LuaAvroDataOutputFile {
    avro_file_writer_t  writer;
} LuaAvroDataOutputFile;
]]

local avro_schema_t = ffi.typeof([[avro_schema_t]])
local LuaAvroSchema

local avro_value_t = ffi.typeof([[avro_value_t]])
local LuaAvroValue

--local avro_consumer_t = ffi.typeof([[avro_consumer_t *]])
local LuaAvroConsumer

local LuaAvroDataInputFile
local LuaAvroDataOutputFile


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

local char_p = ffi.typeof([=[ char * ]=])
local char_p_ptr = ffi.typeof([=[ char *[1] ]=])
local const_char_p_ptr = ffi.typeof([=[ const char *[1] ]=])
local double_ptr = ffi.typeof([=[ double[1] ]=])
local float_ptr = ffi.typeof([=[ float[1] ]=])
local int_ptr = ffi.typeof([=[ int[1] ]=])
local int8_t_ptr = ffi.typeof([=[ int8_t[1] ]=])
local int32_t_ptr = ffi.typeof([=[ int32_t[1] ]=])
local int64_t_ptr = ffi.typeof([=[ int64_t[1] ]=])
local size_t_ptr = ffi.typeof([=[ size_t[1] ]=])
local void_p = ffi.typeof([=[ void * ]=])
local void_p_ptr = ffi.typeof([=[ void *[1] ]=])
local const_void_p_ptr = ffi.typeof([=[ const void *[1] ]=])

--local avro_datum_t_ptr = ffi.typeof([=[ avro_datum_t[1] ]=])
local avro_file_reader_t_ptr = ffi.typeof([=[ avro_file_reader_t[1] ]=])
local avro_file_writer_t_ptr = ffi.typeof([=[ avro_file_writer_t[1] ]=])
local avro_schema_t_ptr = ffi.typeof([=[ avro_schema_t[1] ]=])
local avro_schema_error_t_ptr = ffi.typeof([=[ avro_schema_error_t[1] ]=])


------------------------------------------------------------------------
-- Function declarations

-- avro/generic.h

ffi.cdef [[
avro_value_iface_t *
avro_generic_class_from_schema(avro_schema_t schema);

int
avro_generic_value_new(const avro_value_iface_t *iface, avro_value_t *dest);

void
avro_generic_value_free(avro_value_t *self);
]]

-- avro/io.h

ffi.cdef [[
int
avro_file_reader(const char *path, avro_file_reader_t *reader);

int
avro_file_reader_close(avro_file_reader_t reader);

avro_schema_t
avro_file_reader_get_writer_schema(avro_file_reader_t reader);

int
avro_file_reader_read_value(avro_file_reader_t reader, avro_value_t *dest);

int
avro_file_writer_append_value(avro_file_writer_t writer, avro_value_t *src);

int
avro_file_writer_close(avro_file_writer_t writer);

int
avro_file_writer_create(const char *path, avro_schema_t schema,
                        avro_file_writer_t *writer);

avro_reader_t
avro_reader_memory(const char *buf, int64_t len);

void
avro_reader_free(avro_reader_t reader);

int
avro_value_read(avro_reader_t reader, avro_value_t *dest);

int
avro_value_write(avro_writer_t writer, avro_value_t *src);

int
avro_value_sizeof(avro_value_t *src, size_t *size);

avro_writer_t
avro_writer_memory(char *buf, int64_t len);

void
avro_writer_free(avro_writer_t writer);
]]

-- avro/resolver.h

ffi.cdef [[
avro_value_iface_t *
avro_resolved_writer_new(avro_schema_t wschema, avro_schema_t rschema);

int
avro_resolved_writer_new_value(const avro_value_iface_t *iface,
                               avro_value_t *value);

void
avro_resolved_writer_free_value(avro_value_t *self);

void
avro_resolved_writer_set_dest(avro_value_t *self, avro_value_t *dest);
]]

-- avro/schema.h

ffi.cdef [[
avro_schema_t
avro_schema_array(const avro_schema_t items);

avro_schema_t
avro_schema_array_items(avro_schema_t schema);

void
avro_schema_decref(avro_schema_t schema);

const char *
avro_schema_enum_get(const avro_schema_t schema, int index);

int
avro_schema_enum_get_by_name(const avro_schema_t schema, const char *name);

int
avro_schema_from_json(const char *json_str, const int32_t json_len,
                      avro_schema_t *schema, avro_schema_error_t *err);

avro_schema_t
avro_schema_map_values(avro_schema_t schema);

const char *
avro_schema_type_name(const avro_schema_t schema);

avro_schema_t
avro_schema_union_branch(avro_schema_t schema, int discriminant);
]]

------------------------------------------------------------------------
-- Schemas

local Schema_class = {}
local Schema_mt = { __index = Schema_class }

function Schema_class:new_value()
   local value = LuaAvroValue()
   local rc = avro.avro_generic_value_new(self.iface, value)
   if rc ~= 0 then avro_error() end
   value.should_free = true
   return value
end

function Schema_class:type()
   return self.schema[0].type
end

function Schema_mt:__gc()
   if self.schema ~= nil then
      avro.avro_schema_decref(self.schema)
      self.schema = nil
   end
   if self.iface ~= nil then
      if self.iface.decref ~= nil then
         self.iface.decref(self.iface)
      end
      self.iface = nil
   end
end

local function new_schema(schema)
   local iface = avro.avro_generic_class_from_schema(schema)
   return LuaAvroSchema(schema, iface)
end

function Schema(json)
   local json_len = #json
   local schema = ffi.new(avro_schema_t_ptr)
   local schema_error = ffi.new(avro_schema_error_t_ptr)
   local rc = avro.avro_schema_from_json(json, json_len, schema, schema_error)
   if rc ~= 0 then avro_error() end
   return new_schema(schema[0])
end

function ArraySchema(items)
   local schema = avro.avro_schema_array(items.schema)
   if schema == nil then avro_error() end
   return new_schema(schema)
end

LuaAvroSchema = ffi.metatype([[LuaAvroSchema]], Schema_mt)

------------------------------------------------------------------------
-- Values

local Value_class = {}
local Value_mt = {}

local v_const_char_p = ffi.new(const_char_p_ptr)
local v_double = ffi.new(double_ptr)
local v_float = ffi.new(float_ptr)
local v_int = ffi.new(int_ptr)
local v_int32 = ffi.new(int32_t_ptr)
local v_int64 = ffi.new(int64_t_ptr)
local v_size = ffi.new(size_t_ptr)
local v_const_void_p = ffi.new(const_void_p_ptr)

-- A helper method that returns the Lua equivalent for scalar values.
local function lua_scalar(value)
   local value_type = value:type()
   if value_type == BOOLEAN then
      if value.iface.get_boolean == nil then
         return false, "No implementation for get_boolean"
      end
      local rc = value.iface.get_boolean(value.iface, value.self, v_int)
      if rc ~= 0 then avro_error() end
      return true, v_int[0] ~= 0
   elseif value_type == BYTES then
      local size = ffi.new(int64_t_ptr)
      if value.iface.get_bytes == nil then
         return false, "No implementation for get_bytes"
      end
      local rc = value.iface.get_bytes(value.iface, value.self, v_const_void_p, v_size)
      if rc ~= 0 then avro_error() end
      return true, ffi.string(v_const_void_p[0], v_size[0])
   elseif value_type == DOUBLE then
      if value.iface.get_double == nil then
         return false, "No implementation for get_double"
      end
      local rc = value.iface.get_double(value.iface, value.self, v_double)
      if rc ~= 0 then avro_error() end
      return true, v_double[0]
   elseif value_type == FLOAT then
      if value.iface.get_float == nil then
         return false, "No implementation for get_float"
      end
      local rc = value.iface.get_float(value.iface, value.self, v_float)
      if rc ~= 0 then avro_error() end
      return true, v_float[0]
   elseif value_type == INT then
      if value.iface.get_int == nil then
         return false, "No implementation for get_int"
      end
      local rc = value.iface.get_int(value.iface, value.self, v_int32)
      if rc ~= 0 then avro_error() end
      return true, v_int32[0]
   elseif value_type == LONG then
      if value.iface.get_long == nil then
         return false, "No implementation for get_long"
      end
      local rc = value.iface.get_long(value.iface, value.self, v_int64)
      if rc ~= 0 then avro_error() end
      return true, v_int64[0]
   elseif value_type == NULL then
      if value.iface.get_null == nil then
         return false, "No implementation for get_null"
      end
      local rc = value.iface.get_null(value.iface, value.self)
      if rc ~= 0 then avro_error() end
      return true, nil
   elseif value_type == STRING then
      local size = ffi.new(int64_t_ptr)
      if value.iface.get_string == nil then
         return false, "No implementation for get_string"
      end
      local rc = value.iface.get_string(value.iface, value.self, v_const_char_p, v_size)
      if rc ~= 0 then avro_error() end
      -- size contains the NUL terminator
      return true, ffi.string(v_const_char_p[0], v_size[0] - 1)
   elseif value_type == ENUM then
      if value.iface.get_enum == nil then
         return false, "No implementation for get_enum"
      end
      local rc = value.iface.get_enum(value.iface, value.self, v_int)
      if rc ~= 0 then avro_error() end
      local schema = value.iface.get_schema(value.iface, value.self)
      if schema == nil then avro_error() end
      local symbol_name = avro.avro_schema_enum_get(schema, v_int[0])
      if symbol_name == nil then avro_error() end
      return true, ffi.string(symbol_name)
   elseif value_type == FIXED then
      local size = ffi.new(int64_t_ptr)
      if value.iface.get_fixed == nil then
         return false, "No implementation for get_fixed"
      end
      local rc = value.iface.get_fixed(value.iface, value.self, v_const_void_p, v_size)
      if rc ~= 0 then avro_error() end
      return true, ffi.string(v_const_void_p[0], v_size[0])
   else
      return false, "Not a scalar"
   end
end

-- A helper method that returns a LuaAvroValue wrapper for non-scalar
-- values, and the Lua equivalent for scalar values.
local function scalar_or_wrapper(value)
   local is_scalar, scalar = lua_scalar(value)
   if is_scalar then
      return scalar
   else
      return value
   end
end

-- A helper method that sets the content of a scalar value.  If the
-- value isn't a scalar, we raise an error.
local function set_scalar(value, val)
   local value_type = value:type()
   if value_type == BOOLEAN then
      if value.iface.set_boolean == nil then
         error "No implementation for set_boolean"
      end
      local rc = value.iface.set_boolean(value.iface, value.self, val)
      if rc ~= 0 then avro_error() end
      return
   elseif value_type == BYTES then
      if value.iface.set_bytes == nil then
         error "No implementation for set_bytes"
      end
      local void_val = ffi.cast(void_p, val)
      local rc = value.iface.set_bytes(value.iface, value.self, void_val, #val)
      if rc ~= 0 then avro_error() end
      return
   elseif value_type == DOUBLE then
      if value.iface.set_double == nil then
         error "No implementation for set_double"
      end
      local rc = value.iface.set_double(value.iface, value.self, val)
      if rc ~= 0 then avro_error() end
      return
   elseif value_type == FLOAT then
      if value.iface.set_float == nil then
         error "No implementation for set_float"
      end
      local rc = value.iface.set_float(value.iface, value.self, val)
      if rc ~= 0 then avro_error() end
      return
   elseif value_type == INT then
      if value.iface.set_int == nil then
         error "No implementation for set_int"
      end
      local rc = value.iface.set_int(value.iface, value.self, val)
      if rc ~= 0 then avro_error() end
      return
   elseif value_type == LONG then
      if value.iface.set_long == nil then
         error "No implementation for set_long"
      end
      local rc = value.iface.set_long(value.iface, value.self, val)
      if rc ~= 0 then avro_error() end
      return
   elseif value_type == NULL then
      if value.iface.set_null == nil then
         error "No implementation for set_null"
      end
      local rc = value.iface.set_null(value.iface, value.self)
      if rc ~= 0 then avro_error() end
      return
   elseif value_type == STRING then
      if value.iface.set_string_len == nil then
         error "No implementation for set_string_len"
      end
      -- length must include the NUL terminator
      local char_val = ffi.cast(char_p, val)
      local rc = value.iface.set_string_len(value.iface, value.self, char_val, #val+1)
      if rc ~= 0 then avro_error() end
      return
   elseif value_type == ENUM then
      if value.iface.set_enum == nil then
         error "No implementation for set_enum"
      end
      local symbol_value
      if type(val) == "number" then
         symbol_value = val
      else
         local schema = value.iface.get_schema(value.iface, value.self)
         if schema == nil then avro_error() end
         symbol_value = avro.avro_schema_enum_get_by_name(schema, val)
         if symbol_value < 0 then
            error("No symbol named "..val)
         end
      end
      local rc = value.iface.set_enum(value.iface, value.self, symbol_value)
      if rc ~= 0 then avro_error() end
      return
   elseif value_type == FIXED then
      if value.iface.set_fixed == nil then
         error "No implementation for set_fixed"
      end
      local void_val = ffi.cast(void_p, val)
      local rc = value.iface.set_fixed(value.iface, value.self, void_val, #val)
      if rc ~= 0 then avro_error() end
      return
   else
      error("Avro value isn't a scalar")
   end
end

function Value_class:append(element_val)
   if self:type() ~= ARRAY then
      error("Can only append to an array")
   end

   if self.iface.append == nil then
      error "No implementation for append"
   end

   local element = LuaAvroValue()
   local rc = self.iface.append(self.iface, self.self, element, nil)
   if rc ~= 0 then avro_error() end

   if element_val then
      set_scalar(element, element_val)
   end

   return element
end

function Value_class:discriminant()
   if self:type() ~= UNION then
      error("Can't get discriminant of a non-union value")
   end

   if self.iface.get_discriminant == nil then
      error "No implementation for get_discriminant"
   end

   local rc = self.iface.get_discriminant(self.iface, self.self, v_int)
   if rc ~= 0 then avro_error() end

   local union_schema = self.iface.get_schema(self.iface, self.self)
   if union_schema == nil then avro_error() end

   local branch = avro.avro_schema_union_branch(union_schema, v_int[0])
   return ffi.string(avro.avro_schema_type_name(branch))
end

--local encoding_consumer = avro.avro_encoding_consumer_new()
--local sizeof_consumer = avro.avro_sizeof_consumer_new()

local static_buf = ffi.new([[ char[65536] ]])
local static_size = 65536

function Value_class:encode()
   local size = self:encoded_size()

   -- Use the static buffer if we can, to save on some mallocs.
   local buf, free_buf
   if size <= static_size then
      buf = static_buf
      free_buf = false
   else
      buf = ffi.C.malloc(size)
      if buf == nil then return nil, "Out of memory" end
      free_buf = true
   end

   local writer = avro.avro_writer_memory(buf, size)
   local rc = avro.avro_value_write(writer, self)
   avro.avro_writer_free(writer)

   if rc ~= 0 then
      if free_buf then ffi.C.free(buf) end
      return get_avro_error()
   else
      local result = ffi.string(buf, size)
      if free_buf then ffi.C.free(buf) end
      return result
   end
end

function Value_class:encoded_size()
   local rc = avro.avro_value_sizeof(self, v_size)
   if rc ~= 0 then avro_error() end
   return v_size[0]
end

function raw_encode_value(self, buf, size)
   local writer = avro.avro_writer_memory(buf, size)
   local rc = avro.avro_value_write(writer, self)
   avro.avro_writer_free(writer)
   if rc == 0 then
      return true
   else
      return get_avro_error()
   end
end

function Value_class:get(index)
   if type(index) == "number" then
      local value_type = self:type()
      if value_type == ARRAY then
         local rc = self.iface.get_size(self.iface, self.self, v_size)
         if rc ~= 0 then return get_avro_error() end
         if index < 1 or index > v_size[0] then
            return nil, "Index out of bounds"
         end
         local element = LuaAvroValue()
         element.should_free = false
         rc = self.iface.get_by_index(self.iface, self.self, index-1, element, nil)
         if rc ~= 0 then avro_error() end
         return scalar_or_wrapper(element)
      end

      return nil, "Can only get integer index from arrays"

   elseif type(index) == "string" then
      local value_type = self:type()

      if value_type == MAP then
         local element = LuaAvroValue()
         element.should_free = false
         local rc = self.iface.get_by_name(self.iface, self.self, index, element, nil)
         if rc ~= 0 then return get_avro_error() end
         return scalar_or_wrapper(element)

      elseif value_type == RECORD then
         local field = LuaAvroValue()
         field.should_free = false
         local rc = self.iface.get_by_name(self.iface, self.self, index, field, nil)
         if rc ~= 0 then return get_avro_error() end
         return scalar_or_wrapper(field)

      elseif value_type == UNION then
         if index == "_" then
            local branch = LuaAvroValue()
            branch.should_free = false
            local rc = self.iface.get_current_branch(self.iface, self.self, branch)
            if rc ~= 0 then return get_avro_error() end
            return scalar_or_wrapper(branch)
         else
            local union_schema = self.iface.get_schema(self.iface, self.self)
            local branch_schema = avro.avro_schema_union_branch_by_name(
               union_schema, v_int, index
            )
            if branch_schema == nil then return get_avro_error() end
            local branch = LuaAvroValue()
            local rc = self.iface.set_branch(
               self.iface, self.self,
               v_int[0], branch
            )
            if rc ~= 0 then return get_avro_error() end
            return scalar_or_wrapper(branch)
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
   local element = LuaAvroValue()
   local rc = state.value.iface.get_by_index(
      state.value.iface, state.value.self,
      state.next_index, element, nil
   )
   if rc ~= 0 then avro_error() end
   state.next_index = state.next_index + 1
   -- Result should be a 1-based index for Lua
   return state.next_index, scalar_or_wrapper(element)
end

local function iterate_map(state, unused)
   -- NOTE: state.next_index is 0-based
   -- Have we reached the end?
   if state.next_index >= state.length then return nil end
   -- Nope.
   local key = ffi.new(const_char_p_ptr)
   local element = LuaAvroValue()
   local rc = state.value.iface.get_by_index(
      state.value.iface, state.value.self,
      state.next_index, element, key
   )
   if rc ~= 0 then avro_error() end
   state.next_index = state.next_index + 1
   return ffi.string(key[0]), scalar_or_wrapper(element)
end

function Value_class:iterate()
   local value_type = self:type()

   if value_type == ARRAY then
      local rc = self.iface.get_size(self.iface, self.self, v_size)
      if rc ~= 0 then avro_error() end
      local state = {
         value = self,
         next_index = 0,
         length = v_size[0],
      }
      return iterate_array, state, nil

   elseif value_type == MAP then
      local rc = self.iface.get_size(self.iface, self.self, v_size)
      if rc ~= 0 then avro_error() end
      local state = {
         value = self,
         next_index = 0,
         length = v_size[0],
      }
      return iterate_map, state, nil
   end
end

function Value_class:scalar()
   local is_scalar, scalar = lua_scalar(self)
   if is_scalar then
      return scalar
   else
      error("Value isn't a scalar")
   end
end

local function create_element(value, index)
   if type(index) == "number" then
      local value_type = value:type()
      if value_type == ARRAY then
         local rc = self.iface.get_size(self.iface, self.self, v_size)
         if rc ~= 0 then return get_avro_error() end
         if index < 1 or index > v_size[0] then
            return nil, "Index out of bounds"
         end
         local element = LuaAvroValue()
         element.should_free = false
         rc = self.iface.get_by_index(self.iface, self.self, index-1, element, nil)
         if rc ~= 0 then avro_error() end
         return element
      end

      return nil, "Can only get integer index from arrays"

   elseif type(index) == "string" then
      local value_type = value:type()

      if value_type == MAP then
         local element = LuaAvroValue()
         element.should_free = false
         local rc = value.iface.add(value.iface, value.self, index, element, nil, nil)
         if rc ~= 0 then return get_avro_error() end
         return element

      elseif value_type == RECORD then
         local field = LuaAvroValue()
         field.should_free = false
         local rc = value.iface.get_by_name(value.iface, value.self, index, field, nil)
         if rc ~= 0 then return get_avro_error() end
         return field

      elseif value_type == UNION then
         if index == "_" then
            local branch = LuaAvroValue()
            branch.should_free = false
            local rc = value.iface.get_current_branch(value.iface, value.self, branch)
            if rc ~= 0 then return get_avro_error() end
            return branch
         else
            local union_schema = value.iface.get_schema(value.iface, value.self)
            local branch_schema = avro.avro_schema_union_branch_by_name(
               union_schema, v_int, index
            )
            if branch_schema == nil then return get_avro_error() end
            local branch = LuaAvroValue()
            local rc = value.iface.set_branch(
               value.iface, value.self,
               v_int[0], branch
            )
            if rc ~= 0 then return get_avro_error() end
            return branch
         end
      end

      return nil, "Can only get string index from map, record, or union"
   end

   return nil, "Can only get integer or string index"
end

function Value_class:set(arg1, arg2)
   if arg2 then
      local index, val = arg1, arg2
      local element, err = create_element(self, index)
      if not element then return element, err end
      set_scalar(element, val)
   else
      set_scalar(self, arg1)
   end
end

function Value_class:type()
   return self.iface.get_type(self.iface, self.self)
end

function Value_mt:__tostring()
   --[[
   local json = ffi.new(char_p_ptr)
   local rc = avro.avro_datum_to_json(self.datum, true, json)
   if rc ~= 0 then avro_error() end
   local result = ffi.string(json[0])
   ffi.C.free(json[0])
   return result
   ]]
   return "NIY"
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
   if self.should_free and self.self ~= nil then
      avro.avro_generic_value_free(self)
      self.iface = nil
      self.self = nil
      self.should_free = false
   end
end

LuaAvroValue = ffi.metatype([[avro_value_t]], Value_mt)

------------------------------------------------------------------------
-- ResolvedWriters

local ResolvedWriter_class = {}
local ResolvedWriter_mt = { __index = ResolvedWriter_class }

function raw_decode_value(resolver, buf, size, dest)
   local reader = avro.avro_reader_memory(buf, size)
   avro.avro_resolved_writer_set_dest(resolver.value, dest)
   local rc = avro.avro_value_read(reader, resolver.value)
   avro.avro_reader_free(reader)
   if rc == 0 then
      return true
   else
      return get_avro_error()
   end
end

function ResolvedWriter_class:decode(buf, dest)
   return raw_decode_value(self, buf, #buf, dest)
end

function ResolvedWriter_mt:__gc()
   if self.resolver ~= nil then
      self.resolver.decref(self.resolver)
      self.resolver = nil
   end

   if self.value.self ~= nil then
      avro.avro_resolved_writer_free_value(self.value)
      self.value.iface = nil
      self.value.self = nil
   end
end

function ResolvedWriter(wschema, rschema)
   local resolver = LuaAvroResolvedWriter()
   resolver.resolver = avro.avro_resolved_writer_new(wschema.schema, rschema.schema)
   if resolver.resolver == nil then return get_avro_error() end
   local rc = avro.avro_resolved_writer_new_value(resolver.resolver, resolver.value)
   if rc ~= 0 then return get_avro_error() end
   return resolver
end

LuaAvroResolvedWriter = ffi.metatype([[LuaAvroResolvedWriter]], ResolvedWriter_mt)

------------------------------------------------------------------------
-- Data files

local DataInputFile_class = {}
local DataInputFile_mt = { __index = DataInputFile_class }

local function new_input_file(reader)
   local l_reader = LuaAvroDataInputFile()
   l_reader.reader = reader
   l_reader.wschema = avro.avro_file_reader_get_writer_schema(reader)
   l_reader.iface = avro.avro_generic_class_from_schema(l_reader.wschema)
   return l_reader
end

function DataInputFile_class:read(value)
   if not value then
      value = LuaAvroValue()
      local rc = avro.avro_generic_value_new(self.iface, value)
      if rc ~= 0 then avro_error() end
      value.should_free = true
   end

   local rc = avro.avro_file_reader_read_value(self.reader, value)
   if rc ~= 0 then return get_avro_error() end
   return value
end

function DataInputFile_class:close()
   if self.reader ~= nil then
      avro.avro_file_reader_close(self.reader)
      self.reader = nil
   end
   self.wschema = nil
   if self.iface ~= nil then
      if self.iface.decref ~= nil then
         self.iface.decref(self.iface)
      end
      self.iface = nil
   end
end

DataInputFile_mt.__gc = DataInputFile_class.close
LuaAvroDataInputFile = ffi.metatype([[LuaAvroDataInputFile]], DataInputFile_mt)

local DataOutputFile_class = {}
local DataOutputFile_mt = { __index = DataOutputFile_class }

function DataOutputFile_class:write(value)
   local rc = avro.avro_file_writer_append_value(self.writer, value)
   if rc ~= 0 then avro_error() end
end

function DataOutputFile_class:close()
   if self.writer ~= nil then
      avro.avro_file_writer_close(self.writer)
      self.writer = nil
   end
end

DataOutputFile_mt.__gc = DataOutputFile_class.close
LuaAvroDataOutputFile = ffi.metatype([[LuaAvroDataOutputFile]], DataOutputFile_mt)

function open(path, mode, schema)
   mode = mode or "r"

   if mode == "r" then
      local reader = ffi.new(avro_file_reader_t_ptr)
      local rc = avro.avro_file_reader(path, reader)
      if rc ~= 0 then avro_error() end
      return new_input_file(reader[0])

   elseif mode == "w" then
      local writer = ffi.new(avro_file_writer_t_ptr)
      local rc = avro.avro_file_writer_create(path, schema.schema, writer)
      if rc ~= 0 then avro_error() end
      return LuaAvroDataOutputFile(writer[0])

   else
      error("Invalid mode "..mode)
   end
end

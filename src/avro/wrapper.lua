-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011, RedJack, LLC.
-- All rights reserved.
--
-- Please see the LICENSE.txt file in this distribution for license
-- details.
------------------------------------------------------------------------

local ACC = require "avro.constants"

local getmetatable = getmetatable
local error = error
local ipairs = ipairs
local print = print
local rawget = rawget
local setmetatable = setmetatable
local type = type

module "avro.wrapper"

-- This module provides a framework for creating wrapper classes around
-- the raw Avro values returned by the avro.c module.  We provide a
-- couple of default wrapper classes.  For compound values (arrays,
-- maps, records, and unions), the default wrapper implements a nice
-- table-like syntax for accessing the child values.  For scalar values,
-- the default wrapper is a Lua scalar that has the same value as the
-- underlying Avro value.  Together, this lets you access the contents
-- of a value (named "value), whose schema is
--
--   record test
--     long  ages[];
--   end
--
-- as "value.ages[2]", and have the result be a Lua number.
--
-- In addition to these default wrappers, you can install your own
-- wrapper classes.  A wrapper is defined by a table containing two
-- functions:
--
--   get(raw_value)
--     Return a wrapper instance for the given raw value.
--
--   set(raw_value, value)
--     Set the contents of the given raw value.  This should work when
--     "value" is a wrapped value instance or an arbitrary Lua value.
--
-- Each wrapper class is associated with a named Avro schema.  This
-- means that there can only be a single wrapper class for any of the
-- non-named types (boolean, bytes, double, float, int, long, null,
-- string, array, map, and union).  If you need to provide a custom
-- wrapper class for one of these types, you must wrap it in a named
-- record.


------------------------------------------------------------------------
-- Wrapper dispatch table

local scalar_types_array = {
   ACC.BOOLEAN, ACC.BYTES, ACC.DOUBLE, ACC.FLOAT, ACC.INT,
   ACC.LONG, ACC.NULL, ACC.STRING, ACC.ENUM, ACC.FIXED,
}
local scalar_types = {}
for _,v in ipairs(scalar_types_array) do scalar_types[v] = true end

-- These will be filled in below
local CompoundValue = {}
local ScalarValue = {}

local WRAPPERS = {}

function get_wrapper_class(raw_value)
   local wrapper = WRAPPERS[raw_value:schema_name()]

   if not wrapper then
      if scalar_types[raw_value:type()] then
         wrapper = ScalarValue
      else
         wrapper = CompoundValue
      end
   end

   return wrapper
end

function set_wrapper_class(schema_name, wrapper)
   WRAPPERS[schema_name] = wrapper
end

function get_wrapper(raw_value)
   return get_wrapper_class(raw_value).get(raw_value)
end

function set_wrapper(raw_value, value)
   return get_wrapper_class(raw_value).set(raw_value, value)
end


------------------------------------------------------------------------
-- Default compound value wrapper

local CompoundValue_class = {}
local CompoundValue_mt = {}

function CompoundValue.get(raw_value)
   local obj = { raw = raw_value }
   setmetatable(obj, CompoundValue_mt)
   return obj
end

function CompoundValue.set(raw_value, val)
   if getmetatable(val) == CompoundValue_mt then
      if val.raw ~= raw_value then
         raw_value:copy_from(val.raw)
      end
   else
      raw_value:set_from_ast(val)
   end
end

function CompoundValue_class:type()
   return self.raw:type()
end

function CompoundValue_class:get_(index)
   return self.raw:get(index)
end

function CompoundValue_class:get(index)
   local child = self.raw:get(index)
   return get_wrapper(child)
end

function CompoundValue_class:set_(index)
   return self.raw:set(index)
end

function CompoundValue_class:set(index, val)
   local child, err = self.raw:set(index)
   if not child then return child, err end
   set_wrapper(child, val)
end

function CompoundValue_class:append_(val)
   return self.raw:append()
end

function CompoundValue_class:append(val)
   local child = self.raw:append()
   if val then
      return set_wrapper(child, val)
   else
      return get_wrapper(child)
   end
end

function CompoundValue_class:add_(key)
   return self.raw:add(key)
end

function CompoundValue_class:add(key, val)
   local child = self.raw:add(key)
   if val then
      return set_wrapper(child, val)
   else
      return get_wrapper(child)
   end
end

function CompoundValue_class:discriminant()
   return self.raw:discriminant()
end

local function iterate_wrapped(state, unused)
   local k,v = state.f(state.s, state.var)
   if not k then return k,v end
   state.var = k
   return k, get_wrapper(v)
end

function CompoundValue_class:iterate(want_raw)
   if want_raw then
      return self.raw:iterate()
   else
      local f, s, var = self.raw:iterate()
      local state = { f=f, s=s, var=var }
      return iterate_wrapped, state, nil
   end
end

function CompoundValue_class:set_from_ast(ast)
   return self.raw:set_from_ast(ast)
end

function CompoundValue_class:hash()
   return self.raw:hash()
end

function CompoundValue_class:reset()
   return self.raw:reset()
end

function CompoundValue_class:release()
   return self.raw:release()
end

function CompoundValue_class:copy_from(other)
   return self.raw:copy_from(other.raw)
end

function CompoundValue_class:to_json()
   return self.raw:to_json()
end

CompoundValue_mt.__tostring = CompoundValue_class.to_json

function CompoundValue_mt:__eq(other)
   return self.raw == other.raw
end

function CompoundValue_mt:__index(idx)
   -- First try CompoundValue_class; if there's a function with the given name,
   -- then that's our result.
   local result = CompoundValue_class[idx]
   if result then return result end

   -- Otherwise defer to the get() method.
   return CompoundValue_class.get(self, idx)
end

function CompoundValue_mt:__newindex(idx, val)
   -- First try CompoundValue_class; if there's a function with the given name,
   -- then you need to use the set() method directly.  (We don't want
   -- the caller to overwrite any methods.)
   local result = CompoundValue_class[idx]
   if result then error("Cannot set field with [] syntax") end

   -- Otherwise mimic the set() method.
   local value_type = self.raw:type()
   if value_type == ACC.MAP then
      local child = self.raw:add(idx)
      set_wrapper(child, val)
   else
      local child = self.raw:get(idx)
      set_wrapper(child, val)
   end
end


------------------------------------------------------------------------
-- Default scalar value wrapper

function ScalarValue.get(raw_value)
   return raw_value:get()
end

function ScalarValue.set(raw_value, val)
   raw_value:set(val)
end

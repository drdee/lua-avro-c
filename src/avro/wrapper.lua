-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011, RedJack, LLC.
-- All rights reserved.
--
-- Please see the LICENSE.txt file in this distribution for license
-- details.
------------------------------------------------------------------------

local ACC = require "avro.constants"

local assert = assert
local getmetatable = getmetatable
local error = error
local ipairs = ipairs
local next = next
local pairs = pairs
local print = print
local rawget = rawget
local setmetatable = setmetatable
local tostring = tostring
local type = type

module "avro.wrapper"

-- This module provides a framework for creating wrapper classes around
-- the raw Avro values returned by the avro.c module.  We provide a
-- couple of default wrapper classes.  For compound values (arrays,
-- maps, records, and unions), the default wrapper implements a nice
-- table-like syntax for accessing the child values.  For scalar values,
-- the default wrapper is a Lua scalar that has the same value as the
-- underlying Avro value.  Together, this lets you access the contents
-- of a value (named "value"), whose schema is
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

function get_wrapper_class(schema)
   local wrapper = WRAPPERS[schema:name()]

   if not wrapper then
      if scalar_types[schema:type()] then
         wrapper = ScalarValue
      else
         wrapper = new_compound_wrapper(schema)
      end
   end

   return wrapper
end

function set_wrapper_class(schema_name, wrapper)
   WRAPPERS[schema_name] = wrapper
end

function get_wrapper(raw_value)
   return get_wrapper_class(raw_value:schema()).get(raw_value)
end

function set_wrapper(raw_value, value)
   return get_wrapper_class(raw_value:schema()).set(raw_value, value)
end


------------------------------------------------------------------------
-- Default scalar value wrapper

function ScalarValue.get(raw_value)
   return raw_value:get()
end

function ScalarValue.set(raw_value, val)
   raw_value:set(val)
end


------------------------------------------------------------------------
-- Default compound value wrapper

local cv_methods = {}
local cv_metamethods = {}

function cv_methods:type()
   return self.raw:type()
end

function cv_methods:get_(index)
   return self.raw:get(index)
end

function cv_methods:set_(index)
   return self.raw:set(index)
end

function cv_methods:set_from_ast(ast)
   return self.raw:set_from_ast(ast)
end

function cv_methods:hash()
   return self.raw:hash()
end

function cv_methods:reset()
   return self.raw:reset()
end

function cv_methods:release()
   return self.raw:release()
end

function cv_methods:copy_from(other)
   return self.raw:copy_from(other.raw)
end

function cv_methods:to_json()
   return self.raw:to_json()
end

cv_metamethods.__tostring = cv_methods.to_json

function cv_metamethods:__lt(other)
   return self.raw < other.raw
end

function cv_metamethods:__le(other)
   return self.raw <= other.raw
end

function cv_metamethods:__eq(other)
   return self.raw == other.raw
end

local function iterate_wrapped(state, unused)
   local k,v = state.f(state.s, state.var)
   if not k then return k,v end
   state.var = k
   return k, state.child_wrapper.get(v)
end

local MEMOIZED_WRAPPERS = {}

function new_compound_wrapper(schema)
   -- Links are handled specially.
   local schema_type = schema:type()
   if schema_type == ACC.LINK then
      return new_compound_wrapper(schema:link_target())
   end

   -- See if we've already created a wrapper for this schema.
   local schema_id = schema:id()
   if MEMOIZED_WRAPPERS[schema_id] then
      return MEMOIZED_WRAPPERS[schema_id]
   end

   local class = {}
   local mt = {}
   local wrapper = {}
   MEMOIZED_WRAPPERS[schema_id] = wrapper

   -- A bunch of methods are identical regardless of the schema.

   for name, method in pairs(cv_methods) do
      class[name] = method
   end

   for name, method in pairs(cv_metamethods) do
      mt[name] = method
   end

   -- But some bits depend on the details of the schema.

   if schema_type == ACC.ARRAY then
      local child_schema = schema:item_schema()
      local child_wrapper = assert(get_wrapper_class(child_schema))

      function class:get(index)
         local child, err = self.raw:get(index)
         if not child then return child, err end
         return child_wrapper.get(child)
      end

      function class:append_(val)
         return self.raw:append()
      end

      function class:append(val)
         local child = self.raw:append()
         if val then
            child_wrapper.set(child, val)
         else
            return child_wrapper.get(child)
         end
      end

      function class:iterate(want_raw)
         if want_raw then
            return self.raw:iterate()
         else
            local f, s, var = self.raw:iterate()
            local state = { f=f, s=s, var=var, child_wrapper=child_wrapper }
            return iterate_wrapped, state, nil
         end
      end

      function mt:__index(idx)
         -- First try a class method.
         local result = class[idx]
         if result then return result end

         -- Otherwise mimic the get() method
         return self:get(idx)
      end

      function mt:__newindex(idx, val)
         -- If there's a class method with this name, you can't use the
         -- table syntax.
         if class[idx] then
            error("Cannot set "..tostring(idx).." with [] syntax")
         end

         -- Otherwise mimic the non-existent set() method
         local child, err = self.raw:get(idx)
         if not child then return child, err end
         child_wrapper.set(child)
      end
   end

   if schema_type == ACC.MAP then
      local child_schema = schema:value_schema()
      local child_wrapper = assert(get_wrapper_class(child_schema))

      function class:get(index)
         local child, err = self.raw:get(index)
         if not child then return child, err end
         return child_wrapper.get(child)
      end

      function class:set(index, value)
         local child, err = self.raw:set(index)
         if not child then return child, err end
         child_wrapper.set(child, value)
      end

      function class:add_(key)
         return self.raw:add(key)
      end

      function class:add(key, val)
         local child = self.raw:add(key)
         if val then
            child_wrapper.set(child, val)
         else
            return child_wrapper.get(child)
         end
      end

      function class:iterate(want_raw)
         if want_raw then
            return self.raw:iterate()
         else
            local f, s, var = self.raw:iterate()
            local state = { f=f, s=s, var=var, child_wrapper=child_wrapper }
            return iterate_wrapped, state, nil
         end
      end

      function mt:__index(idx)
         -- First try a class method.
         local result = class[idx]
         if result then return result end

         -- Otherwise mimic the add() method
         return self:add(idx)
      end

      function mt:__newindex(idx, val)
         -- If there's a class method with this name, you can't use the
         -- table syntax.
         if class[idx] then
            error("Cannot set "..tostring(idx).." with [] syntax")
         end

         -- Otherwise mimic the add() method
         self:add(idx, val)
      end
   end

   if schema_type == ACC.RECORD then
      local fields = schema:fields()
      local wrappers = {}
      local real_indices = {}
      for i, field_table in ipairs(fields) do
         local field_name, field_schema = next(field_table)
         local wrapper = assert(get_wrapper_class(field_schema))
         wrappers[i] = wrapper
         wrappers[field_name] = wrapper
         real_indices[i] = i
         real_indices[field_name] = i
      end

      function class:get(index)
         local child, err = self.raw:get(index)
         if not child then return child, err end
         return wrappers[index].get(child)
      end

      function class:set(index)
         local child, err = self.raw:set(index)
         if not child then return child, err end
         wrappers[index].set(child, val)
      end

      function mt:__index(idx)
         -- First try a class method.
         local result = class[idx]
         if result then return result end

         -- Otherwise see if there's a field with this name or index.
         local real_index = real_indices[idx]
         if real_index then
            local child, err = self.raw:get(real_index)
            if not child then return child, err end
            return wrappers[real_index].get(child)
         else
            return nil, "No field "..tostring(idx)
         end
      end

      function mt:__newindex(idx, val)
         -- If there's a class method with this name, you can't use the
         -- table syntax.
         if class[idx] then
            error("Cannot set "..tostring(idx).." with [] syntax")
         end

         -- Otherwise mimic the set() method
         local real_index = real_indices[idx]
         if real_index then
            local child, err = self.raw:get(real_index)
            if not child then return child, err end
            wrappers[real_index].set(child, val)
         else
            return nil, "No field "..tostring(idx)
         end
      end
   end

   if schema_type == ACC.UNION then
      local branches = schema:branches()
      local wrappers = {}
      local real_indices = {}
      for i, branch_table in ipairs(branches) do
         local branch_name, branch_schema = next(branch_table)
         local wrapper = assert(get_wrapper_class(branch_schema))
         wrappers[i] = wrapper
         wrappers[branch_name] = wrapper
         real_indices[i] = i
         real_indices[branch_name] = i
      end

      function class:get(index)
         local child, err = self.raw:get(index)
         if not child then return child, err end
         if not index then
            index = self.raw:discriminant_index()
         end
         return wrappers[index].get(child)
      end

      function class:set(index)
         local child, err = self.raw:set(index)
         if not child then return child, err end
         wrappers[index].set(child, val)
      end

      function class:discriminant_index()
         return self.raw:discriminant_index()
      end

      function class:discriminant()
         return self.raw:discriminant()
      end

      function mt:__index(idx)
         -- First try a class method.
         local result = class[idx]
         if result then return result end

         -- The special "_" field represents the current active branch.
         if idx == "_" then
            local disc_index = self.raw:discriminant_index()
            local child = self.raw:get()
            return wrappers[disc_index].get(child)
         end

         -- Otherwise see if there's a field with this name or index.
         local real_index = real_indices[idx]
         if real_index then
            local child, err = self.raw:set(real_index)
            if not child then return child, err end
            return wrappers[real_index].get(child)
         else
            return nil, "No branch "..tostring(idx)
         end
      end

      function mt:__newindex(idx, val)
         -- If there's a class method with this name, you can't use the
         -- table syntax.
         if class[idx] then
            error("Cannot set "..tostring(idx).." with [] syntax")
         end

         -- The special "_" field represents the current active branch.
         if idx == "_" then
            local disc_index = self.raw:discriminant_index()
            local child = self.raw:get()
            wrappers[disc_index].set(child, val)
            return
         end

         -- Otherwise mimic the set() method
         local real_index = real_indices[idx]
         if real_index then
            local child, err = self.raw:set(real_index)
            if not child then return child, err end
            wrappers[real_index].set(child, val)
         else
            return nil, "No field "..tostring(idx)
         end
      end
   end

   -- The actual wrapper functions

   function wrapper.get(raw_value)
      local obj = { raw=raw_value }
      setmetatable(obj, mt)
      return obj
   end

   function wrapper.set(raw_value, val)
      if val.raw == raw_value then
         return
      end
      if getmetatable(val) == mt then
         raw_value:copy_from(val.raw)
      else
         raw_value:set_from_ast(val)
      end
   end

   return wrapper
end

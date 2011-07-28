-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011, RedJack, LLC.
-- All rights reserved.
--
-- Please see the LICENSE.txt file in this distribution for license
-- details.
------------------------------------------------------------------------

local ACC = require "avro.constants"

local error = error
local ipairs = ipairs
local print = print
local rawget = rawget
local setmetatable = setmetatable
local type = type

module "avro.wrapper"

-- A helpful wrapper API that lets you access Avro values using the Lua
-- table syntax

local Value_class = {}
local Value_mt = {}

function Value(raw_value)
   if not raw_value then error "What?" end
   local obj = { raw = raw_value }
   setmetatable(obj, Value_mt)
   return obj
end

function Value_class:raw_value()
   return self.raw
end

function Value_class:type()
   return self.raw:type()
end

local scalar_types_array = {
   ACC.BOOLEAN, ACC.BYTES, ACC.DOUBLE, ACC.FLOAT, ACC.INT,
   ACC.LONG, ACC.NULL, ACC.STRING, ACC.ENUM, ACC.FIXED,
}
local scalar_types = {}
for _,v in ipairs(scalar_types_array) do scalar_types[v] = true end

function Value_class:scalar()
   if scalar_types[self.raw:type()] then
      return self.raw:get()
   else
      error "Value isn't a scalar"
   end
end

function Value_class:get(index)
   if scalar_types[self.raw:type()] then
      error "Value is a scalar"
   else
      local child = self.raw:get(index)
      if scalar_types[child:type()] then
         return child:get()
      else
         return Value(child)
      end
   end
end

local function set_scalar(raw, val)
   if scalar_types[raw:type()] then
      raw:set(val)
   else
      error "Value isn't a scalar"
   end
end

function Value_class:set(arg1, arg2)
   if arg2 then
      local child, err = self.raw:set(arg1)
      if not child then return child, err end
      set_scalar(child, arg2)
   else
      set_scalar(self.raw, arg1)
   end
end

function Value_class:append(val)
   local child = self.raw:append()
   if val then
      set_scalar(child, val)
   end
   return Value(child)
end

function Value_class:add(key, val)
   local child = self.raw:add(key)
   if val then
      set_scalar(child, val)
   end
   return Value(child)
end

function Value_class:discriminant()
   return self.raw:discriminant()
end

function Value_class:encode()
   return self.raw:encode()
end

function Value_class:encoded_size()
   return self.raw:encoded_size()
end

local function iterate_wrapped(state, unused)
   local k,v = state.f(state.s, state.var)
   if not k then return k,v end
   state.var = k
   if not state.no_scalar and scalar_types[v:type()] then
      return k, v:get()
   else
      return k, Value(v)
   end
end

function Value_class:iterate(no_scalar)
   local f, s, var = self.raw:iterate()
   local state = { f=f, s=s, var=var, no_scalar=no_scalar }
   return iterate_wrapped, state, nil
end

function Value_class:set_from_ast(ast)
   return self.raw:set_from_ast(ast)
end

function Value_class:hash()
   return self.raw:hash()
end

function Value_class:reset()
   return self.raw:reset()
end

function Value_class:copy_from(other)
   return self.raw:copy_from(other.raw)
end

function Value_class:set_source(src)
   return self.raw:set_source(src.raw)
end

function Value_class:to_json()
   return self.raw:to_json()
end

Value_mt.__tostring = Value_class.to_json

function Value_mt:__eq(other)
   return self.raw == other.raw
end

function Value_class:release()
   return self.raw:release()
end

function Value_mt:__index(idx)
   -- First try Value_class; if there's a function with the given name,
   -- then that's our result.
   if rawget(self, idx) then return rawget(self,idx) end

   local result = Value_class[idx]
   if result then return result end

   -- Otherwise defer to the get() method.
   return Value_class.get(self, idx)
end

function Value_mt:__newindex(idx, val)
   -- First try Value_class; if there's a function with the given name,
   -- then you need to use the set() method directly.  (We don't want
   -- the caller to overwrite any methods.)
   local result = Value_class[idx]
   if result then error("Cannot set field with [] syntax") end

   -- Otherwise mimic the set() method.
   local value_type = self.raw:type()
   if value_type == ACC.MAP then
      local child = self.raw:add(idx)
      child:set(val)
   else
      local child = self.raw:get(idx)
      child:set(val)
   end
end

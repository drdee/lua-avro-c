-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2012, RedJack, LLC.
-- All rights reserved.
--
-- Please see the LICENSE.txt file in this distribution for license
-- details.
------------------------------------------------------------------------

local A = require "avro"

------------------------------------------------------------------------
-- Helpers

-- The following function is from [1], and is MIT/X11-licensed.
-- [1] http://snippets.luacode.org/snippets/Deep_Comparison_of_Two_Values_3

function deepcompare(t1,t2,ignore_mt)
   local ty1 = type(t1)
   local ty2 = type(t2)
   if ty1 ~= ty2 then return false end
   -- non-table types can be directly compared
   if ty1 ~= 'table' and ty2 ~= 'table' then return t1 == t2 end
   -- as well as tables which have the metamethod __eq
   local mt = getmetatable(t1)
   if not ignore_mt and mt and mt.__eq then return t1 == t2 end
   for k1,v1 in pairs(t1) do
      local v2 = t2[k1]
      if v2 == nil or not deepcompare(v1,v2) then return false end
   end
   for k2,v2 in pairs(t2) do
      local v1 = t1[k2]
      if v1 == nil or not deepcompare(v1,v2) then return false end
   end
   return true
end

------------------------------------------------------------------------
-- Primitives

do
   local function test_parse(json, raw_value)
      local schema = A.Schema:new(json)
      local value = schema:new_value()
      value:set(raw_value)
      local actual = value:get()
      assert(actual == raw_value)
   end

   local function test_prim(prim_type, raw_value)
      test_parse([[{"type": "]]..prim_type..[["}]], raw_value)
      test_parse([["]]..prim_type..[["]], raw_value)
   end

   test_prim("boolean", false)
   test_prim("bytes", "abc")
   test_prim("double", 42.0)
   test_prim("float", 42.0)
   test_prim("int", 42)
   test_prim("long", 42)
   test_prim("null", nil)
   test_prim("string", "abc")
end

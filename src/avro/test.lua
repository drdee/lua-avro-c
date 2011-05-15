-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011, RedJack, LLC.
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
-- Schema:type()

do
   local function test_parse(json, expected)
      local schema = A.Schema(json)
      local actual = schema:type()
      assert(actual == expected)
   end

   local function test_prim(prim_type, expected)
      test_parse([[{"type": "]]..prim_type..[["}]], expected)
   end

   test_prim("boolean", A.BOOLEAN)
   test_prim("bytes", A.BYTES)
   test_prim("double", A.DOUBLE)
   test_prim("float", A.FLOAT)
   test_prim("int", A.INT)
   test_prim("long", A.LONG)
   test_prim("null", A.NULL)
   test_prim("string", A.STRING)
end

------------------------------------------------------------------------
-- Arrays

do
   local function test_array(prim_type, expected)
      local items_schema = A.Schema([[{"type": "]]..prim_type..[["}]])
      local schema = A.ArraySchema(items_schema)
      local array = schema:new_value()
      for _,val in ipairs(expected) do
         array:append(val)
      end
      local actual = {}
      for _,element in array:iterate() do
         table.insert(actual, element)
      end
      assert(deepcompare(actual, expected))
   end

   test_array("int", { 1,2,3,4 })
   test_array("string", { "", "a", "hello", "world!" })
end

------------------------------------------------------------------------
-- Maps

do
   local function test_map(prim_type, expected)
      local schema = A.Schema([[{"type": "map", "values": "]]..prim_type..[["}]])
      local map = schema:new_value()
      for key,val in pairs(expected) do
         map:set(key, val)
      end
      local actual = {}
      for key,element in map:iterate() do
         actual[key] = element
      end
      assert(deepcompare(actual, expected))
   end

   test_map("int", { a=1,b=2,c=3,d=4 })
   test_map("string", { a="", b="a", c="hello", d="world!" })
end

------------------------------------------------------------------------
-- Resolver()

do
   local function test_good_resolver(json1, json2)
      local schema1 = A.Schema(json1)
      local schema2 = A.Schema(json2)
      local resolver = assert(A.Resolver(schema1, schema2))
   end

   local function test_good_prim(prim_type1, prim_type2)
      test_good_resolver([[{"type": "]]..prim_type1..[["}]],
                         [[{"type": "]]..prim_type2..[["}]])
   end

   local function test_bad_resolver(json1, json2)
      local schema1 = A.Schema(json1)
      local schema2 = A.Schema(json2)
      local resolver = assert(not A.Resolver(schema1, schema2))
   end

   local function test_bad_prim(prim_type1, prim_type2)
      test_bad_resolver([[{"type": "]]..prim_type1..[["}]],
                        [[{"type": "]]..prim_type2..[["}]])
   end

   test_good_prim("boolean", "boolean")
   test_bad_prim ("boolean", "bytes")

   test_good_prim("bytes", "bytes")
   test_bad_prim ("bytes", "double")

   test_good_prim("double", "double")
   test_bad_prim ("double", "int")

   test_good_prim("float", "float")
   test_good_prim("float", "double")
   test_bad_prim ("float", "int")

   test_good_prim("int", "int")
   test_good_prim("int", "long")
   test_good_prim("int", "float")
   test_good_prim("int", "double")
   test_bad_prim ("int", "null")

   test_good_prim("long", "long")
   test_good_prim("long", "float")
   test_good_prim("long", "double")
   test_bad_prim ("long", "null")

   test_good_prim("null", "null")
   test_bad_prim ("null", "string")

   test_good_prim("string", "string")
   test_bad_prim ("string", "boolean")
end

------------------------------------------------------------------------
-- Resolver:decode()

do
   local function test_boolean(buf, expected_prim)
      local schema = A.Schema([[{"type": "boolean"}]])
      local actual = schema:new_value()
      local resolver = assert(A.Resolver(schema, schema))
      assert(resolver:decode(buf, actual))
      assert(actual:scalar() == expected_prim)
   end

   test_boolean("\000", false)
   test_boolean("\001", true)

   local function test_int(buf, expected_prim)
      local schema = A.Schema([[{"type": "int"}]])
      local actual = schema:new_value()
      local resolver = assert(A.Resolver(schema, schema))
      assert(resolver:decode(buf, actual))
      assert(actual:scalar() == expected_prim)
   end

   test_int("\000", 0)
   test_int("\001", -1)
   test_int("\002", 1)
end

------------------------------------------------------------------------
-- Resolver:encode()

do
   local function test_boolean(expected_buf, prim_value)
      local schema = A.Schema([[{"type": "boolean"}]])
      local value = schema:new_value()
      value:set(prim_value)
      local actual_buf = assert(value:encode())
      assert(actual_buf == expected_buf)
   end

   test_boolean("\000", false)
   test_boolean("\001", true)

   local function test_int(expected_buf, prim_value)
      local schema = A.Schema([[{"type": "int"}]])
      local actual = schema:new_value()
      local value = schema:new_value()
      value:set(prim_value)
      local actual_buf = assert(value:encode())
      assert(actual_buf == expected_buf)
   end

   test_int("\000", 0)
   test_int("\001", -1)
   test_int("\002", 1)
end

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
-- Schema:type()

do
   local function test_parse(json, expected)
      local schema = A.Schema(json)
      local actual = schema:type()
      assert(actual == expected)
   end

   local function test_prim(prim_type, expected)
      test_parse([[{"type": "]]..prim_type..[["}]], expected)
      test_parse(prim_type, expected)
      test_parse(A[prim_type], expected)
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
-- Helper constructors

do
   local json = [[
      {
         "type": "record",
         "name": "test",
         "fields": [
            {"name": "i", "type": "int"},
            {"name": "l", "type": "long"},
            {"name": "e", "type":
               {
                  "type": "enum",
                  "name": "color",
                  "symbols": ["RED","GREEN","BLUE"]
               }
            },
            {"name": "a", "type":
               { "type": "array", "items": "double" }},
            {"name": "m", "type":
               { "type": "map", "values": "float" }},
            {"name": "ipv4", "type":
               { "type": "fixed", "name": "ipv4", "size": 4 }},
            {"name": "ipv6", "type":
               { "type": "fixed", "name": "ipv6", "size": 16 }},
            {"name": "u", "type": ["null", "string", "double"]},
            {"name": "sub", "type":
               {
                  "type": "record",
                  "name": "subtest",
                  "fields": [
                     {"name": "s", "type": "string"}
                  ]
               }
            },
            {"name": "children", "type":
               { "type": "array", "items": "test" }},
            {"name": "parent", "type": ["null", "test"]}
         ]
      }
   ]]

   local schema1 = A.Schema(json)
   local schema2 = A.record "test" {
      {i = "int"},
      {l = [[ {"type": "long"} ]]},
      {e = A.enum "color" {"RED","GREEN","BLUE"} },
      {a = A.array { A.double }},
      {m = A.map(A.float)},
      {ipv4 = A.fixed "ipv4" {size=4}},
      {ipv6 = A.fixed "ipv6"(16)},
      {u = A.union {A.null, A.string, A.double}},
      {sub = A.record "subtest" {
         s = A.string,
      }},
      {children = A.array { A.link "test" }},
      {parent = A.union { A.null, A.link "test" }},
   }

   --print(schema1)
   --print(schema2)
   assert(schema1 == schema2)
end
-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011, RedJack, LLC.
-- All rights reserved.
--
-- Please see the LICENSE.txt file in this distribution for license
-- details.
------------------------------------------------------------------------

local AC = require "avro.c"

local ipairs = ipairs
local next = next
local pairs = pairs
local type = type

module "avro.schema"

boolean = AC.Schema "boolean"
bytes = AC.Schema "bytes"
double = AC.Schema "double"
float = AC.Schema "float"
int = AC.Schema "int"
long = AC.Schema "long"
null = AC.Schema "null"
string = AC.Schema "string"

-- Creates a new Avro record schema with the given name.  The result of
-- this function is another function, which you pass a Lua table to
-- describing the fields of the record.  So the overall syntax is
-- something like:
--
--   local schema = record "packet" {
--      timestamp = record "timestamp" {
--         value = "long",
--      },
--      full_length = "long",
--      packet = "bytes",
--   }
--
-- OR
--
--   local schema = record "packet" {
--      {timestamp = record "timestamp" {
--         value = "long",
--      }},
--      {full_length = "long"},
--      {packet = "bytes"},
--   }
--
-- In the first syntax, the entries in the Lua table are keyed by name,
-- and are therefore unordered, so you don't know in advance which order
-- the fields of the record will be in.  Most of the time, that's not
-- good.
--
-- In the second syntax, the outer table is an array-like table, with
-- numerical indices, and each value is a single-element table.  This
-- lets us ensure that the fields appear in the schema in the same order
-- they appear in the Lua source code.

function record(name)
   return function (fields)
      local schema = AC.RecordSchema(name)
      for _, field_table in ipairs(fields) do
         local field_name, field_schema_spec = next(field_table)
         local field_schema = AC.Schema(field_schema_spec)
         schema:append_field(field_name, field_schema)
      end
      for field_name, field_schema_spec in pairs(fields) do
         if type(field_name) == "string" then
            local field_schema = AC.Schema(field_schema_spec)
            schema:append_field(field_name, field_schema)
         end
      end
      return schema
   end
end

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
local tonumber = tonumber
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

-- The constructors for enum, fixed, and record schemas all take in a
-- name, followed by a Lua table describing the contents of the type.
-- We do this by only taking in the name in this function, which returns
-- a function that takes in the Lua table.  This gives us an overall
-- syntax that doesn't require parentheses:
--
--   local schema = enum "color" { "RED", "GREEN", "BLUE" }

------------------------------------------------------------------------
-- Arrays and maps
--
--   local schema = array { item_schema }
--   local schema = array(item_schema)

function array(args)
   local item_schema_spec
   if type(args) == "table" then
      _, item_schema_spec = next(args)
   else
      item_schema_spec = args
   end
   return AC.ArraySchema(AC.Schema(item_schema_spec))
end

function map(args)
   local value_schema_spec
   if type(args) == "table" then
      _, value_schema_spec = next(args)
   else
      value_schema_spec = args
   end
   return AC.MapSchema(AC.Schema(value_schema_spec))
end

------------------------------------------------------------------------
-- Enums
--
--   local schema = enum "color" { "RED", "GREEN", "BLUE" }

function enum(name)
   return function (symbols)
      local schema = AC.EnumSchema(name)
      for _, symbol_name in ipairs(symbols) do
         schema:append_symbol(symbol_name)
      end
      return schema
   end
end

------------------------------------------------------------------------
-- Fixeds
--
--   local schema = fixed "ipv4" { size=4 }
--   local schema = fixed "ipv4"(4)

function fixed(name)
   return function (args)
      local size
      if type(args) == "table" then
         size = args.size
      else
         size = tonumber(args)
      end
      return AC.FixedSchema(name, size)
   end
end

------------------------------------------------------------------------
-- Records
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

------------------------------------------------------------------------
-- Unions
--
--   local schema = union { branch_schemas }

function union(branches)
   local schema = AC.UnionSchema()
   for _, branch_schema_spec in ipairs(branches) do
      local branch_schema = AC.Schema(branch_schema_spec)
      schema:append_branch(branch_schema)
   end
   return schema
end

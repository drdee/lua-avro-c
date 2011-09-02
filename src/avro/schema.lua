-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011, RedJack, LLC.
-- All rights reserved.
--
-- Please see the LICENSE.txt file in this distribution for license
-- details.
------------------------------------------------------------------------

local AC = require "avro.c"

local error = error
local ipairs = ipairs
local next = next
local pairs = pairs
local print = print
local tonumber = tonumber
local type = type

module "avro.schema"

------------------------------------------------------------------------
-- Various helper functions

function clone(schema)
   -- This isn't the most efficient, but we clone a schema via the JSON
   -- encoding.
   return AC.Schema(schema:to_json())
end

-- Adds additional fields to a record schema.  This updates the schema
-- that you pass in; if you need to retain an unmodified copy of the
-- original, call add_fields() instead.

function add_fields_in_place(schema, fields)
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
end

function add_fields(schema, fields)
   local new_schema = clone(schema)
   add_fields_in_place(new_schema, fields)
   return new_schema
end

--

------------------------------------------------------------------------
-- Pre-constructed primitive types

boolean = AC.Schema "boolean"
bytes = AC.Schema "bytes"
double = AC.Schema "double"
float = AC.Schema "float"
int = AC.Schema "int"
long = AC.Schema "long"
null = AC.Schema "null"
string = AC.Schema "string"

------------------------------------------------------------------------
-- Helper constructors for compound types

-- The constructors for enum, fixed, and record schemas all take in a
-- name, followed by a Lua table describing the contents of the type.
-- We do this by only taking in the name in this function, which returns
-- a function that takes in the Lua table.  This gives us an overall
-- syntax that doesn't require parentheses:
--
--   local schema = enum "color" { "RED", "GREEN", "BLUE" }

-- Links
--
--   local schema = link "schema_name"
--
-- For links, we maintain a hash table of each named schema that we've
-- constructed so far.  Normally, you'd think that the innermost part of
-- a schema constructor expression would be evaluated first, meaning
-- that the link wouldn't be able to immediately see the schema that it
-- points to.  Luckily, however, the curried-function approach that we
-- use to construct records means that we can create the (empty) record
-- schema, and assign it into the hash table, *before* the second
-- function (which defines the record's fields) is evaluated.  Nice!

local LINK_TARGETS = {}
local LINK_DEPTH = 0

local function init_links()
   LINK_DEPTH = LINK_DEPTH + 1
end

local function done_links()
   LINK_DEPTH = LINK_DEPTH - 1
   if LINK_DEPTH == 0 then
      LINK_TARGETS = {}
   end
end

local function save_link(name, schema)
   LINK_TARGETS[name] = schema
end

function link(name)
   if not LINK_TARGETS[name] then
      error("No schema named "..name)
   else
      --print("--- link "..name)
      return AC.LinkSchema(LINK_TARGETS[name])
   end
end

-- Arrays and maps
--
--   local schema = array { item_schema }
--   local schema = array(item_schema)

function array(args)
   --print("--- array")
   init_links()
   local item_schema_spec
   if type(args) == "table" then
      _, item_schema_spec = next(args)
   else
      item_schema_spec = args
   end
   done_links()
   return AC.ArraySchema(AC.Schema(item_schema_spec))
end

function map(args)
   --print("--- map")
   init_links()
   local value_schema_spec
   if type(args) == "table" then
      _, value_schema_spec = next(args)
   else
      value_schema_spec = args
   end
   done_links()
   return AC.MapSchema(AC.Schema(value_schema_spec))
end

-- Enums
--
--   local schema = enum "color" { "RED", "GREEN", "BLUE" }

function enum(name)
   --print("--- enum "..name)
   init_links()
   return function (symbols)
      local schema = AC.EnumSchema(name)
      for _, symbol_name in ipairs(symbols) do
         schema:append_symbol(symbol_name)
      end
      save_link(name, schema)
      done_links()
      return schema
   end
end

-- Fixeds
--
--   local schema = fixed "ipv4" { size=4 }
--   local schema = fixed "ipv4"(4)

function fixed(name)
   --print("--- fixed "..name)
   init_links()
   return function (args)
      local size
      if type(args) == "table" then
         size = args.size
      else
         size = tonumber(args)
      end
      local schema = AC.FixedSchema(name, size)
      save_link(name, schema)
      done_links()
      return schema
   end
end

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
   --print("--- record "..name)
   init_links()
   local schema = AC.RecordSchema(name)
   save_link(name, schema)
   return function (fields)
      add_fields_in_place(schema, fields)
      done_links()
      return schema
   end
end

-- Unions
--
--   local schema = union { branch_schemas }

function union(branches)
   --print("--- union")
   init_links()
   local schema = AC.UnionSchema()
   for _, branch_schema_spec in ipairs(branches) do
      local branch_schema = AC.Schema(branch_schema_spec)
      schema:append_branch(branch_schema)
   end
   done_links()
   return schema
end

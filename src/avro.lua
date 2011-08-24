-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011, RedJack, LLC.
-- All rights reserved.
--
-- Please see the LICENSE.txt file in this distribution for license
-- details.
------------------------------------------------------------------------

local AC = require "avro.c"
local ACC = require "avro.constants"
local AS = require "avro.schema"
local AW = require "avro.wrapper"

local pairs = pairs
local print = print
local setmetatable = setmetatable
local string = string

module "avro"

------------------------------------------------------------------------
-- Constants

for k,v in pairs(ACC) do
   if string.sub(k,1,1) ~= "_" then
      _M[k] = v
   end
end


------------------------------------------------------------------------
-- Copy a bunch of public functions from the submodules.

ArraySchema = AC.ArraySchema
ResolvedReader = AC.ResolvedReader
ResolvedWriter = AC.ResolvedWriter
Schema = AC.Schema
open = AC.open
raw_decode_value = AC.raw_decode_value
raw_encode_value = AC.raw_encode_value
raw_value = AC.raw_value
wrapped_value = AC.wrapped_value

get_wrapper_class = AW.get_wrapper_class
set_wrapper_class = AW.set_wrapper_class
get_wrapper = AW.get_wrapper
set_wrapper = AW.set_wrapper

boolean = AS.boolean
bytes = AS.bytes
double = AS.double
float = AS.float
int = AS.int
long = AS.long
null = AS.null
_M.string = AS.string  -- need the _M b/c we import Lua's string above

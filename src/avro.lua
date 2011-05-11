-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011, RedJack, LLC.
-- All rights reserved.
--
-- Please see the LICENSE.txt file in this distribution for license
-- details.
------------------------------------------------------------------------

-- Loads either avro.c.legacy or avro.c.ffi, depending on whether the
-- LuaJIT FFI module is available.

local ffi_present = pcall(require, "ffi")
local mod
if ffi_present then
   --print("Loading ffi version")
   mod = require("avro.c.ffi")
else
   --print("Loading legacy version")
   mod = require("avro.c.legacy")
end
mod.ffi_present = ffi_present
return mod

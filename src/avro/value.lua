-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2012, RedJack, LLC.
-- All rights reserved.
--
-- Please see the LICENSE.txt file in this distribution for license
-- details.
------------------------------------------------------------------------

local AC = require "avro.c"
local ACC = require "avro.constants"

local print = print
local setmetatable = setmetatable

module "avro.value"

------------------------------------------------------------------------
-- Base value class

Value = {}
Value.__mt = { __index=Value }


------------------------------------------------------------------------
-- Primitives

-- boolean

local BooleanValue = {}
BooleanValue.__mt = { __index=BooleanValue }
setmetatable(BooleanValue, { __index=Value })

function BooleanValue:get()
   return AC.value_get_boolean(self.c_value)
end

function BooleanValue:set(val)
   AC.value_set_boolean(self.c_value, val)
end

-- bytes

local BytesValue = {}
BytesValue.__mt = { __index=BytesValue }
setmetatable(BytesValue, { __index=Value })

function BytesValue:get()
   return AC.value_get_bytes(self.c_value)
end

function BytesValue:set(val)
   AC.value_set_bytes(self.c_value, val)
end

-- double

local DoubleValue = {}
DoubleValue.__mt = { __index=DoubleValue }
setmetatable(DoubleValue, { __index=Value })

function DoubleValue:get()
   return AC.value_get_double(self.c_value)
end

function DoubleValue:set(val)
   AC.value_set_double(self.c_value, val)
end

-- float

local FloatValue = {}
FloatValue.__mt = { __index=FloatValue }
setmetatable(FloatValue, { __index=Value })

function FloatValue:get()
   return AC.value_get_float(self.c_value)
end

function FloatValue:set(val)
   AC.value_set_float(self.c_value, val)
end

-- int

local IntValue = {}
IntValue.__mt = { __index=IntValue }
setmetatable(IntValue, { __index=Value })

function IntValue:get()
   return AC.value_get_int(self.c_value)
end

function IntValue:set(val)
   AC.value_set_int(self.c_value, val)
end

-- long

local LongValue = {}
LongValue.__mt = { __index=LongValue }
setmetatable(LongValue, { __index=Value })

function LongValue:get()
   return AC.value_get_long(self.c_value)
end

function LongValue:set(val)
   AC.value_set_long(self.c_value, val)
end

-- null

local NullValue = {}
NullValue.__mt = { __index=NullValue }
setmetatable(NullValue, { __index=Value })

function NullValue:get()
   return AC.value_get_null(self.c_value)
end

function NullValue:set(val)
   AC.value_set_null(self.c_value, val)
end

-- string

local StringValue = {}
StringValue.__mt = { __index=StringValue }
setmetatable(StringValue, { __index=Value })

function StringValue:get()
   return AC.value_get_string(self.c_value)
end

function StringValue:set(val)
   AC.value_set_string(self.c_value, val)
end


------------------------------------------------------------------------
-- Constructors

local VALUE_CLASSES = {
   [ACC.BOOLEAN] = BooleanValue,
   [ACC.BYTES] = BytesValue,
   [ACC.DOUBLE] = DoubleValue,
   [ACC.FLOAT] = FloatValue,
   [ACC.INT] = IntValue,
   [ACC.LONG] = LongValue,
   [ACC.NULL] = NullValue,
   [ACC.STRING] = StringValue,
}

function Value:new_raw(c_value)
   local c_type = AC.value_type(c_value)
   local obj = {
      c_value = c_value,
   }
   return setmetatable(obj, VALUE_CLASSES[c_type].__mt)
end

function Value:set_raw(c_value)
   self.c_value = c_value
end

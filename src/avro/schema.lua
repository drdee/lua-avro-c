-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011, RedJack, LLC.
-- All rights reserved.
--
-- Please see the LICENSE.txt file in this distribution for license
-- details.
------------------------------------------------------------------------

local AC = require "avro.c"

local pairs = pairs

module "avro.schema"

boolean = AC.Schema "boolean"
bytes = AC.Schema "bytes"
double = AC.Schema "double"
float = AC.Schema "float"
int = AC.Schema "int"
long = AC.Schema "long"
null = AC.Schema "null"
string = AC.Schema "string"

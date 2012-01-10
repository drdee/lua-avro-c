package = "lua-avro"
version = "scm-0"

source = {
   url = "git@git.redjack.com:fathom/lua-avro.git",
   branch = "develop"
}

description = {
   summary = "Lua bindings for Avro C library",
   homepage = "http://git.redjack.com/fathom/lua-avro",
   license = "Proprietary"
}

dependencies = {
   "lua >= 5.1",
}

external_dependencies = {
   AVRO = {
      header = "avro.h",
   },
}

build = {
   type = "builtin",
   modules = {
      avro = "src/avro.lua",
      ["avro.constants"] = "src/avro/constants.lua",
      ["avro.dkjson"] = "src/avro/dkjson.lua",
      ["avro.schema"] = "src/avro/schema.lua",
      ["avro.value"] = "src/avro/value.lua",
      ["avro.wrapper"] = "src/avro/wrapper.lua",
      ["avro.c"] = "src/avro/c.lua",
      ["avro.legacy.avro"] = {
         sources = {"src/avro/legacy/avro.c"},
         libraries = {"avro"},
         incdirs = {"$(AVRO_INCDIR)"},
         libdirs = {"$(AVRO_LIBDIR)"},
      },
      ["avro.ffi.avro"] = "src/avro/ffi/avro.lua",
      ["avro.test"] = "src/avro/test.lua",
      ["avro.tests.raw"] = "src/avro/tests/raw.lua",
      ["avro.tests.schema"] = "src/avro/tests/schema.lua",
      ["avro.tests.value"] = "src/avro/tests/value.lua",
      ["avro.tests.wrapper"] = "src/avro/tests/wrapper.lua",
   },
}

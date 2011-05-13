package = "lua-avro"
version = "0.2"

source = {
   url = "git@git.redjack.com:fathom/lua-avro.git",
   tag = "0.2",
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
      ["avro.test"] = "src/avro/test.lua",
      ["avro.c.legacy"] = {
         sources = {"src/avro/c/legacy.c"},
         libraries = {"avro"},
         incdirs = {"$(AVRO_INCDIR)"},
         libdirs = {"$(AVRO_LIBDIR)"},
      },
      ["avro.c.ffi"] = "src/avro/c/ffi.lua",
   },
}

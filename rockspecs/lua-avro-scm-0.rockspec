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
      avro = {
         sources = {"src/avro.c"},
         libraries = {"avro"},
         incdirs = {"$(AVRO_INCDIR)"},
         libdirs = {"$(AVRO_LIBDIR)"},
      },
   },
}

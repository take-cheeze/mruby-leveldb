#include <mruby.h>
#include <mruby/class.h>
#include <mruby/data.h>
#include <mruby/hash.h>

#include <leveldb/db.h>


namespace {

using namespace leveldb;

RClass *get_error(mrb_state *M) {
  return mrb_class_get_under(M, mrb_class_get(M, "LevelDB"), "Error");
}

void check_error(mrb_state *M, Status const& s) {
  if (s.ok()) { return; }

  std::string const str = s.ToString();
  mrb_exc_raise(M, mrb_exc_new(M, get_error(M), str.data(), str.size()));
}

template<class T>
T& get_ref(mrb_state *M, mrb_value const& v, mrb_data_type const& t) {
  if (not DATA_PTR(v)) {
    mrb_raise(M, get_error(M), "already destroyed data");
  }
  return *((T*)mrb_data_get_ptr(M, v, &t));
}

#define symbol_value_lit(M, lit) mrb_symbol_value(mrb_intern_lit(M, lit))

void parse_opt(mrb_state *M, Options& opt, mrb_value const& val) {
  if (mrb_nil_p(val)) { return; }

  opt.create_if_missing = mrb_bool(mrb_hash_get(M, val, symbol_value_lit(M, "create_if_missing")));
  opt.error_if_exists = mrb_bool(mrb_hash_get(M, val, symbol_value_lit(M, "error_if_exists")));
  opt.paranoid_checks = mrb_bool(mrb_hash_get(M, val, symbol_value_lit(M, "paranoid_checks")));

  mrb_value const write_buffer_size = mrb_hash_get(M, val, symbol_value_lit(M, "write_buffer_size"));
  if (not mrb_nil_p(write_buffer_size)) {
    mrb_check_type(M, write_buffer_size, MRB_TT_FIXNUM);
    opt.write_buffer_size = mrb_fixnum(write_buffer_size);
  }

  mrb_value const max_open_files = mrb_hash_get(M, val, symbol_value_lit(M, "max_open_files"));
  if (not mrb_nil_p(max_open_files)) {
    mrb_check_type(M, max_open_files, MRB_TT_FIXNUM);
    opt.max_open_files = mrb_fixnum(max_open_files);
  }

  mrb_value const block_size = mrb_hash_get(M, val, symbol_value_lit(M, "block_size"));
  if (not mrb_nil_p(block_size)) {
    mrb_check_type(M, block_size, MRB_TT_FIXNUM);
    opt.block_size = mrb_fixnum(block_size);
  }

  mrb_value const block_restart_interval = mrb_hash_get(M, val, symbol_value_lit(M, "block_restart_interval"));
  if (not mrb_nil_p(block_restart_interval)) {
    mrb_check_type(M, block_restart_interval, MRB_TT_FIXNUM);
    opt.block_restart_interval = mrb_fixnum(block_restart_interval);
  }

  mrb_value const compression = mrb_hash_get(M, val, symbol_value_lit(M, "compression"));
  if (not mrb_nil_p(compression)) {
    mrb_check_type(M, block_restart_interval, MRB_TT_SYMBOL);
    if (mrb_symbol(compression) == mrb_intern_lit(M, "no") or
        mrb_symbol(compression) == mrb_intern_lit(M, "none")) {
      opt.compression = kNoCompression;
    } else if (mrb_symbol(compression) == mrb_intern_lit(M, "snappy")) {
      opt.compression = kSnappyCompression;
    } else {
      mrb_raisef(M, get_error(M), "Invalid compression: %S", compression);
    }
  }

  // comparator
  // env
  // logger
  // block_cache
  // filter_policy
}

void parse_opt(mrb_state *M, ReadOptions& opt, mrb_value const& val) {
  if (mrb_nil_p(val)) { return; }

  opt.verify_checksums = mrb_bool(mrb_hash_get(M, val, symbol_value_lit(M, "verify_checksums")));

  mrb_value const fill_cache = mrb_hash_get(M, val, symbol_value_lit(M, "fill_cache"));
  if (not mrb_nil_p(fill_cache)) {
    opt.fill_cache = mrb_bool(fill_cache);
  }

  /* TODO
  mrb_value const snapshot = mrb_hash_get(M, val, symbol_value_lit(M, "snapshot"));
  if (not mrb_nil_p(snapshot)) {
    opt.snapshot = get_ref<Snapshot>(M, snapshot, snapshot_type);
  }
  */
}

void parse_opt(mrb_state *M, WriteOptions& opt, mrb_value const& val) {
  if (mrb_nil_p(val)) { return; }

  opt.sync = mrb_bool(mrb_hash_get(M, val, symbol_value_lit(M, "sync")));
}

void db_free(mrb_state*, void *p) {
  delete ((DB*)p);
}
mrb_data_type const leveldb_type = { "LevelDB", db_free };

mrb_value db_init(mrb_state *M, mrb_value self) {
  DB *db;
  mrb_value opt_val = mrb_nil_value();
  Options opt;
  char *str; int str_len;

  mrb_get_args(M, "s|H", &str, &str_len, &opt_val);

  parse_opt(M, opt, opt_val);
  check_error(M, DB::Open(opt, std::string(str, str_len), &db));

  mrb_assert(db);

  DATA_PTR(self) = db;
  DATA_TYPE(self) = &leveldb_type;

  return self;
}

mrb_value db_close(mrb_state *M, mrb_value self) {
  delete &get_ref<DB>(M, self, leveldb_type);
  DATA_PTR(self) = NULL;
  return self;
}

mrb_value db_put(mrb_state *M, mrb_value self)
{
  mrb_value opt_val = mrb_nil_value();
  WriteOptions opt;
  char *key, *val; int key_len, val_len;

  mrb_get_args(M, "ss|H", &key, &key_len, &val, &val_len, &opt_val);

  parse_opt(M, opt, opt_val);
  check_error(M, get_ref<DB>(M, self, leveldb_type).Put(opt, Slice(key, key_len), Slice(val, val_len)));
  return mrb_str_new(M, val, val_len);
}

mrb_value db_get(mrb_state *M, mrb_value self) {
  mrb_value opt_val = mrb_nil_value();
  ReadOptions opt;
  char *key; int key_len;

  mrb_get_args(M, "s|H", &key, &key_len, &opt_val);

  parse_opt(M, opt, opt_val);
  std::string val;
  Status const s = get_ref<DB>(M, self, leveldb_type).Get(opt, Slice(key, key_len), &val);
  if (s.IsNotFound()) {
    return mrb_nil_value();
  } else {
    check_error(M, s);
    return mrb_str_new(M, val.data(), val.size());
  }
}

mrb_value db_delete(mrb_state *M, mrb_value self) {
  mrb_value opt_val = mrb_nil_value();
  WriteOptions opt;
  char *key; int key_len;

  mrb_get_args(M, "s|H", &key, &key_len, &opt_val);

  parse_opt(M, opt, opt_val);
  check_error(M, get_ref<DB>(M, self, leveldb_type).Delete(opt, Slice(key, key_len)));
  return self;
}

}

extern "C" void mrb_mruby_leveldb_gem_init(mrb_state *M) {
  RClass *db = mrb_define_class(M, "LevelDB", M->object_class);
  MRB_SET_INSTANCE_TT(db, MRB_TT_DATA);

  // optional argument is option
  mrb_define_method(M, db, "initialize", db_init, MRB_ARGS_REQ(1) | MRB_ARGS_OPT(1));
  mrb_define_method(M, db, "put", db_put, MRB_ARGS_REQ(2) | MRB_ARGS_OPT(1));
  mrb_define_method(M, db, "delete", db_delete, MRB_ARGS_REQ(1) | MRB_ARGS_OPT(1));
  mrb_define_method(M, db, "get", db_get, MRB_ARGS_REQ(1) | MRB_ARGS_OPT(1));
  mrb_define_alias(M, db, "[]", "get");
  mrb_define_method(M, db, "close", db_close, MRB_ARGS_NONE());

  mrb_define_class_under(M, db, "Error", M->eException_class);
}

extern "C" void mrb_mruby_leveldb_gem_final(mrb_state*) {}

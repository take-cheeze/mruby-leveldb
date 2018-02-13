#include <mruby.h>
#include <mruby/array.h>
#include <mruby/class.h>
#include <mruby/data.h>
#include <mruby/hash.h>
#include <mruby/string.h>
#include <mruby/variable.h>

#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <leveldb/env.h>

#include <functional>
#include <memory>
#include <vector>


namespace {

using namespace leveldb;
using std::placeholders::_1;

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
  return *reinterpret_cast<T*>(mrb_data_get_ptr(M, v, &t));
}

void logger_free(mrb_state *M, void *p) {
  delete static_cast<Logger*>(p);
}
mrb_data_type const logger_type = { "logger", logger_free };

typedef std::unique_ptr<Snapshot const, std::function<void(Snapshot const*)>> SnapshotRef;
void snapshot_free(mrb_state *M, void *p) {
  if (p) {
    reinterpret_cast<SnapshotRef*>(p)->~SnapshotRef();
    mrb_free(M, p);
  }
}
mrb_data_type const snapshot_type = {  "snapshot", snapshot_free };

void iterator_free(mrb_state*, void *p) {
  if (p) { delete reinterpret_cast<Iterator*>(p); }
}
mrb_data_type const iterator_type = { "iterator", &iterator_free };

#define symbol_value_lit(M, lit) mrb_symbol_value(mrb_intern_lit(M, lit))

void parse_opt(mrb_state *M, mrb_value const& self, Options& opt, mrb_value const& val) {
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

  mrb_value compression = mrb_hash_get(M, val, symbol_value_lit(M, "compression"));
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

  mrb_value const info_log = mrb_hash_get(M, val, symbol_value_lit(M, "info_log"));
  if (not mrb_nil_p(info_log)) {
    opt.info_log = &get_ref<Logger>(M, info_log, logger_type);

    // protect logger from GC
    if (not mrb_nil_p(self)) {
      mrb_iv_set(M, self, mrb_intern_lit(M, "info_log"), info_log);
    }
  }

  // comparator
  // env
  // block_cache
  // filter_policy
}

void parse_opt(mrb_state *M, mrb_value const& /* obj */, ReadOptions& opt, mrb_value const& val) {
  if (mrb_nil_p(val)) { return; }

  opt.verify_checksums = mrb_bool(mrb_hash_get(M, val, symbol_value_lit(M, "verify_checksums")));

  mrb_value const fill_cache = mrb_hash_get(M, val, symbol_value_lit(M, "fill_cache"));
  if (not mrb_nil_p(fill_cache)) {
    opt.fill_cache = mrb_bool(fill_cache);
  }

  mrb_value const snapshot = mrb_hash_get(M, val, symbol_value_lit(M, "snapshot"));
  if (not mrb_nil_p(snapshot)) {
    opt.snapshot = get_ref<SnapshotRef>(M, snapshot, snapshot_type).get();
  }
}

void parse_opt(mrb_state *M, mrb_value const& /* obj */, WriteOptions& opt, mrb_value const& val) {
  if (mrb_nil_p(val)) { return; }

  opt.sync = mrb_bool(mrb_hash_get(M, val, symbol_value_lit(M, "sync")));
}

struct DBRef {
  std::unique_ptr<DB> const cxx;
  mrb_state * const M;
  mrb_value const mruby;

  DBRef(mrb_state *M, DB *ptr, mrb_value v) : cxx(ptr), M(M), mruby(v) {}

  ~DBRef() {
    mrb_assert(DATA_PTR(mruby));
    if (not mrb_iv_defined(M, mruby, mrb_intern_lit(M, "handles"))) { return; }

    mrb_value const handles = mrb_iv_get(M, mruby, mrb_intern_lit(M, "handles"));
    mrb_assert(mrb_array_p(handles));

    for (mrb_int i = 0; i < RARRAY_LEN(handles); ++i) {
      mrb_value const h = RARRAY_PTR(handles)[i];
      mrb_assert(mrb_type(h) == MRB_TT_DATA);
      if (not DATA_PTR(h)) { continue; }

      if (DATA_TYPE(h) == &snapshot_type) {
        reinterpret_cast<SnapshotRef*>(DATA_PTR(h))->~SnapshotRef();
        mrb_free(M, DATA_PTR(h));
      }
      else if (DATA_TYPE(h) == &iterator_type) {
        delete reinterpret_cast<Iterator*>(DATA_PTR(h));
      }
      else { mrb_assert(false); }
      DATA_PTR(h) = nullptr;
    }

    mrb_ary_clear(M, handles);
  }

  void clear_free_handles() {
    if (not mrb_iv_defined(M, mruby, mrb_intern_lit(M, "handles"))) { return; }

    mrb_value const handles = mrb_iv_get(M, mruby, mrb_intern_lit(M, "handles"));
    mrb_int new_idx = 0;
    for (mrb_int i = 0; i < RARRAY_LEN(handles); ++i) {
      if (DATA_PTR(RARRAY_PTR(handles)[i])) {
        mrb_ary_set(M, handles, new_idx++, RARRAY_PTR(handles)[i]);
      }
    }
    mrb_ary_resize(M, handles, new_idx);
  }

  void add_handle(mrb_value const& v) {
    mrb_value handles = mrb_iv_get(M, mruby, mrb_intern_lit(M, "handles"));
    if (mrb_nil_p(handles)) {
      handles = mrb_ary_new_capa(M, 1);
      mrb_iv_set(M, mruby, mrb_intern_lit(M, "handles"), handles);
    }
    mrb_assert(mrb_array_p(handles));
    mrb_ary_push(M, handles, v);
  }
};

void db_free(mrb_state *mrb, void *p) {
  if (p) {
    DBRef *const ref = reinterpret_cast<DBRef*>(p);
    ref->~DBRef();
    mrb_free(mrb, p);
  }
}
mrb_data_type const leveldb_type = { "LevelDB", db_free };

mrb_value db_init(mrb_state *M, mrb_value self) {
  mrb_value opt_val = mrb_nil_value();
  char *str; mrb_int str_len;

  mrb_get_args(M, "s|H", &str, &str_len, &opt_val);

  Options opt;
  parse_opt(M, self, opt, opt_val);

  DB *db;
  check_error(M, DB::Open(opt, std::string(str, str_len), &db));

  mrb_assert(db);

  DATA_PTR(self) = new(mrb_malloc(M, sizeof(DBRef))) DBRef(M, db, self);
  DATA_TYPE(self) = &leveldb_type;

  return self;
}

mrb_value db_close(mrb_state *M, mrb_value self) {
  get_ref<DBRef>(M, self, leveldb_type).~DBRef();
  mrb_free(M, DATA_PTR(self));
  DATA_PTR(self) = nullptr;
  mrb_iv_remove(M, self, mrb_intern_lit(M, "info_log"));
  return self;
}

mrb_value db_put(mrb_state *M, mrb_value self)
{
  mrb_value opt_val = mrb_nil_value();
  WriteOptions opt;
  char *key, *val; mrb_int key_len, val_len;

  mrb_get_args(M, "ss|H", &key, &key_len, &val, &val_len, &opt_val);

  parse_opt(M, self, opt, opt_val);
  check_error(M, get_ref<DBRef>(M, self, leveldb_type).cxx->Put(opt, Slice(key, key_len), Slice(val, val_len)));
  return mrb_str_new(M, val, val_len);
}

mrb_value db_get(mrb_state *M, mrb_value self) {
  mrb_value opt_val = mrb_nil_value();
  ReadOptions opt;
  char *key; mrb_int key_len;

  mrb_get_args(M, "s|H", &key, &key_len, &opt_val);

  parse_opt(M, self, opt, opt_val);
  std::string val;
  Status const s = get_ref<DBRef>(M, self, leveldb_type).cxx->Get(opt, Slice(key, key_len), &val);
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
  char *key; mrb_int key_len;

  mrb_get_args(M, "s|H", &key, &key_len, &opt_val);

  parse_opt(M, self, opt, opt_val);
  check_error(M, get_ref<DBRef>(M, self, leveldb_type).cxx->Delete(opt, Slice(key, key_len)));
  return self;
}

mrb_value db_property(mrb_state *M, mrb_value self) {
  char *key; mrb_int key_len;
  mrb_get_args(M, "s", &key, &key_len);

  std::string str;
  return get_ref<DBRef>(M, self, leveldb_type).cxx->GetProperty(Slice(key, key_len), &str)
      ? mrb_str_new(M, str.data(), str.size()) : mrb_nil_value();
}

mrb_value db_approximate_sizes(mrb_state *M, mrb_value self) {
  mrb_value *ranges_val; mrb_int n;
  mrb_value result = mrb_nil_value();
  mrb_get_args(M, "a|A", &ranges_val, &n, &result);

  if (mrb_nil_p(result)) {
    result = mrb_ary_new_capa(M, n);
  } else {
    mrb_ary_clear(M, result);
  }

  std::vector<Range> ranges(n);
  for (mrb_int i = 0; i < n; ++i) {
    mrb_value
        start = mrb_str_to_str(M, mrb_ary_entry(ranges_val[i], 0)),
        limit = mrb_str_to_str(M, mrb_ary_entry(ranges_val[i], 1));
    ranges[i] = Range(Slice(RSTRING_PTR(start), RSTRING_LEN(start)),
                      Slice(RSTRING_PTR(limit), RSTRING_LEN(limit)));
  }
  std::vector<uint64_t> sizes(n);
  get_ref<DBRef>(M, self, leveldb_type).cxx->GetApproximateSizes(ranges.data(), n, sizes.data());

  for (mrb_int i = 0; i < n; ++i) {
    mrb_ary_push(M, result, sizes[i] > MRB_INT_MAX? mrb_float_value(M, sizes[i]) : mrb_fixnum_value(sizes[i]));
  }
  return result;
}

mrb_value db_compact_range(mrb_state *M, mrb_value self) {
  char *begin, *end; mrb_int begin_len, end_len;
  int const argc = mrb_get_args(M, "|ss", &begin, &begin_len, &end, &end_len);
  switch(argc) {
    case 2: {
      Slice const b(begin, begin_len), e(end, end_len);
      get_ref<DBRef>(M, self, leveldb_type).cxx->CompactRange(&b, &e);
    } break;

    case 0: {
      get_ref<DBRef>(M, self, leveldb_type).cxx->CompactRange(nullptr, nullptr);
    } break;

    default:
      mrb_raisef(M, mrb_class_get(M, "ArgumentError"), "Wrong number of arguments: %S", mrb_fixnum_value(argc));
      break;
  }
  return self;
}

mrb_value db_snapshot(mrb_state *M, mrb_value self) {
  DBRef& ref = get_ref<DBRef>(M, self, leveldb_type);
  ref.clear_free_handles();
  mrb_value ret =  mrb_obj_value(mrb_data_object_alloc(
      M, mrb_class_get_under(M, mrb_class_get(M, "LevelDB"), "Snapshot"),
      new(mrb_malloc(M, sizeof(SnapshotRef)))
      SnapshotRef(ref.cxx->GetSnapshot(), std::bind(&DB::ReleaseSnapshot, ref.cxx.get(), _1)),
      &snapshot_type));
  ref.add_handle(ret);
  return ret;
}

mrb_value db_iterate(mrb_state *M, mrb_value self) {
  mrb_value opt_val = mrb_nil_value(), b;
  mrb_get_args(M, "&|o", &b, &opt_val);

  if (mrb_nil_p(b)) {
    mrb_raise(M, mrb_class_get(M, "ArgumentError"), "Expected block in LevelDB#iterate .");
  }

  ReadOptions opt;
  parse_opt(M, self, opt, opt_val);
  std::unique_ptr<Iterator> const it(get_ref<DBRef>(M, self, leveldb_type).cxx->NewIterator(opt));
  it->SeekToFirst();
  check_error(M, it->status());

  while (it->Valid()) {
    Slice const key = it->key(), val = it->value();
    mrb_value args[] =
        { mrb_str_new(M, key.data(), key.size()), mrb_str_new(M, val.data(), val.size()) };
    mrb_yield_argv(M, b, 2, args);

    it->Next();
    check_error(M, it->status());
  }

  return self;
}

mrb_value db_iterator(mrb_state *M, mrb_value self) {
  mrb_value opt_val = mrb_nil_value();
  mrb_get_args(M, "|o", &opt_val);
  DBRef& ref = get_ref<DBRef>(M, self, leveldb_type);
  ref.clear_free_handles();
  ReadOptions opt;
  parse_opt(M, self, opt, opt_val);
  mrb_value ret =  mrb_obj_value(mrb_data_object_alloc(
      M, mrb_class_get_under(M, mrb_class_get(M, "LevelDB"), "Iterator"),
      ref.cxx->NewIterator(opt), &iterator_type));
  ref.add_handle(ret);
  return ret;
}

mrb_value iterator_init(mrb_state *M, mrb_value self) {
  mrb_value db, opt_val = mrb_nil_value();
  mrb_get_args(M, "o|o", &db, &opt_val);

  DBRef& ref = get_ref<DBRef>(M, db, iterator_type);
  ref.clear_free_handles();

  ReadOptions opt;
  parse_opt(M, self, opt, opt_val);

  DATA_PTR(self) = ref.cxx->NewIterator(opt);
  DATA_TYPE(self) = &iterator_type;
  ref.add_handle(self);
  return self;
}

mrb_value iterator_delete(mrb_state *M, mrb_value self) {
  delete &get_ref<Iterator>(M, self, iterator_type);
  DATA_PTR(self) = NULL;
  return self;
}

mrb_value iterator_valid_p(mrb_state *M, mrb_value self) {
  return mrb_bool_value(get_ref<Iterator>(M, self, iterator_type).Valid());
}

mrb_value iterator_seek(mrb_state *M, mrb_value self) {
  char *str; mrb_int str_len;
  mrb_get_args(M, "s", &str, &str_len);
  Iterator& it = get_ref<Iterator>(M, self, iterator_type);
  it.Seek(Slice(str, str_len));
  return check_error(M, it.status()), self;
}

mrb_value iterator_seek_to_first(mrb_state *M, mrb_value self) {
  Iterator& it = get_ref<Iterator>(M, self, iterator_type);
  it.SeekToFirst();
  return check_error(M, it.status()), self;
}

mrb_value iterator_seek_to_last(mrb_state *M, mrb_value self) {
  Iterator& it = get_ref<Iterator>(M, self, iterator_type);
  it.SeekToLast();
  return check_error(M, it.status()), self;
}

mrb_value iterator_next(mrb_state *M, mrb_value self) {
  Iterator& it = get_ref<Iterator>(M, self, iterator_type);
  it.Next();
  return check_error(M, it.status()), self;
}

mrb_value iterator_prev(mrb_state *M, mrb_value self) {
  Iterator& it = get_ref<Iterator>(M, self, iterator_type);
  it.Prev();
  return check_error(M, it.status()), self;
}

mrb_value iterator_key(mrb_state *M, mrb_value self) {
  Iterator& it = get_ref<Iterator>(M, self, iterator_type);
  Slice const ret = it.key();
  return check_error(M, it.status()), mrb_str_new(M, ret.data(), ret.size());
}

mrb_value iterator_value(mrb_state *M, mrb_value self) {
  Iterator& it = get_ref<Iterator>(M, self, iterator_type);
  Slice const ret = it.value();
  return check_error(M, it.status()), mrb_str_new(M, ret.data(), ret.size());
}

mrb_value snapshot_init(mrb_state *M, mrb_value self) {
  mrb_value db;
  mrb_get_args(M, "o", &db);

  DBRef& ref = get_ref<DBRef>(M, db, leveldb_type);
  ref.clear_free_handles();

  DATA_PTR(self) = new(mrb_malloc(M, sizeof(SnapshotRef))) SnapshotRef(
      ref.cxx->GetSnapshot(), std::bind(&DB::ReleaseSnapshot, ref.cxx.get(), _1));
  DATA_TYPE(self) = &snapshot_type;
  ref.add_handle(self);

  return self;
}

mrb_value snapshot_release(mrb_state *M, mrb_value self) {
  get_ref<SnapshotRef>(M, self, snapshot_type).~SnapshotRef();
  mrb_free(M, DATA_PTR(self));
  DATA_PTR(self) = nullptr;
  return self;
}

mrb_value db_destroy(mrb_state *M, mrb_value self) {
  mrb_value opt_val = mrb_nil_value();
  Options opt;
  char *fname; mrb_int fname_len;

  mrb_get_args(M, "s|H", &fname, &fname_len, &opt_val);
  parse_opt(M, mrb_nil_value(), opt, opt_val);
  check_error(M, DestroyDB(std::string(fname, fname_len), opt));
  return self;
}

mrb_value db_repair(mrb_state *M, mrb_value self) {
  mrb_value opt_val = mrb_nil_value();
  Options opt;
  char *fname; mrb_int fname_len;

  mrb_get_args(M, "s|H", &fname, &fname_len, &opt_val);
  parse_opt(M, mrb_nil_value(), opt, opt_val);
  check_error(M, RepairDB(std::string(fname, fname_len), opt));
  return self;
}

void batch_free(mrb_state *M, void *p) {
  reinterpret_cast<WriteBatch*>(p)->~WriteBatch();
  mrb_free(M, p);
}
mrb_data_type const write_batch_type = { "write_batch", batch_free };

mrb_value db_write(mrb_state *M, mrb_value self) {
  mrb_value batch, opt_val = mrb_nil_value();
  WriteOptions opt;

  mrb_get_args(M, "o|H", &batch, &opt_val);

  parse_opt(M, self, opt, opt_val);
  check_error(M, get_ref<DBRef>(M, self, leveldb_type).cxx->Write(
      opt, &get_ref<WriteBatch>(M, batch, write_batch_type)));
  return self;
}

mrb_value batch_init(mrb_state *M, mrb_value self) {
  DATA_PTR(self) = new(mrb_malloc(M, sizeof(WriteBatch))) WriteBatch();
  DATA_TYPE(self) = &write_batch_type;
  return self;
}

mrb_value batch_put(mrb_state *M, mrb_value self) {
  char *key, *val; mrb_int key_len, val_len;
  mrb_get_args(M, "ss", &key, &key_len, &val, &val_len);
  return get_ref<WriteBatch>(M, self, write_batch_type)
      .Put(Slice(key, key_len), Slice(val, val_len)), self;
}

mrb_value batch_delete(mrb_state *M, mrb_value self) {
  char *key; mrb_int key_len;
  mrb_get_args(M, "s", &key, &key_len);
  return get_ref<WriteBatch>(M, self, write_batch_type).Delete(Slice(key, key_len)), self;
}

mrb_value batch_clear(mrb_state *M, mrb_value self) {
  return get_ref<WriteBatch>(M, self, write_batch_type).Clear(), self;
}

struct ArrayBatchHandler : public WriteBatch::Handler {
  mrb_state* const M;
  mrb_value const result;

  ArrayBatchHandler(mrb_state *M) : M(M), result(mrb_ary_new(M)) {}

  void Put(Slice const& k, Slice const& v) override {
    mrb_value const ary[] = {
      symbol_value_lit(M, "put"),
      mrb_str_new(M, k.data(), k.size()),
      mrb_str_new(M, v.data(), v.size()) };
    mrb_ary_push(M, result, mrb_ary_new_from_values(M, 3, ary));
  }

  void Delete(Slice const& k) override {
    mrb_value const ary[] = {
      symbol_value_lit(M, "delete"),
      mrb_str_new(M, k.data(), k.size()) };
    mrb_ary_push(M, result, mrb_ary_new_from_values(M, 2, ary));
  }
};

struct BlockBatchHandler : public WriteBatch::Handler {
  mrb_state* const M;
  mrb_value const block;

  BlockBatchHandler(mrb_state *M, mrb_value const& b) : M(M), block(b) {}

  void Put(Slice const& k, Slice const& v) override {
    mrb_value const ary[] = {
      symbol_value_lit(M, "put"),
      mrb_str_new(M, k.data(), k.size()),
      mrb_str_new(M, v.data(), v.size()) };
    mrb_yield(M, block, mrb_ary_new_from_values(M, 3, ary));
  }

  void Delete(Slice const& k) override {
    mrb_value const ary[] = {
      symbol_value_lit(M, "delete"),
      mrb_str_new(M, k.data(), k.size()) };
    mrb_yield(M, block, mrb_ary_new_from_values(M, 2, ary));
  }
};

mrb_value batch_iterate(mrb_state *M, mrb_value self) {
  mrb_value b;
  mrb_get_args(M, "&", &b);

  if (mrb_nil_p(b)) { // return array
    ArrayBatchHandler h(M);
    return get_ref<WriteBatch>(M, self, write_batch_type).Iterate(&h), h.result;
  } else {
    BlockBatchHandler h(M, b);
    return get_ref<WriteBatch>(M, self, write_batch_type).Iterate(&h), self;
  }
}

mrb_value logger_init(mrb_state *M, mrb_value self) {
  char *fname; mrb_int fname_len;
  mrb_get_args(M, "s", &fname, &fname_len);

  Logger *logger;
  check_error(M, Env::Default()->NewLogger(std::string(fname, fname_len), &logger));
  mrb_assert(logger);

  DATA_PTR(self) = logger;
  DATA_TYPE(self) = &logger_type;
  return self;
}

void log(mrb_state *M, mrb_value const& self, char const* str, ...) {
  va_list vl;
  va_start(vl, str);
  get_ref<Logger>(M, self, logger_type).Logv(str, vl);
  va_end(vl);
}

mrb_value logger_log(mrb_state *M, mrb_value self) {
  char *str;;
  mrb_get_args(M, "z", &str);
  return log(M, self, str), self;
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
  mrb_define_method(M, db, "write", db_write, MRB_ARGS_REQ(1) | MRB_ARGS_OPT(1));
  mrb_define_method(M, db, "property", db_property, MRB_ARGS_REQ(1));
  mrb_define_method(M, db, "approximate_sizes", db_approximate_sizes, MRB_ARGS_REQ(1) | MRB_ARGS_OPT(1));
  mrb_define_method(M, db, "compact_range", db_compact_range, MRB_ARGS_REQ(2));
  mrb_define_method(M, db, "snapshot", db_snapshot, MRB_ARGS_NONE());
  mrb_define_method(M, db, "iterator", db_iterator, MRB_ARGS_OPT(1));
  mrb_define_method(M, db, "iterate", db_iterate, MRB_ARGS_OPT(1) | MRB_ARGS_BLOCK());

  mrb_define_class_method(M, db, "destroy", db_destroy, MRB_ARGS_REQ(1) | MRB_ARGS_OPT(1));
  mrb_define_class_method(M, db, "repair", db_repair, MRB_ARGS_REQ(1) | MRB_ARGS_OPT(1));

  mrb_define_class_under(M, db, "Error", M->eException_class);

  RClass *batch = mrb_define_class_under(M, db, "WriteBatch", M->object_class);
  MRB_SET_INSTANCE_TT(batch, MRB_TT_DATA);
  mrb_define_method(M, batch, "initialize", batch_init, MRB_ARGS_NONE());
  mrb_define_method(M, batch, "put", batch_put, MRB_ARGS_REQ(2));
  mrb_define_method(M, batch, "[]=", batch_put, MRB_ARGS_REQ(2));
  mrb_define_method(M, batch, "delete", batch_delete, MRB_ARGS_REQ(1));
  mrb_define_method(M, batch, "clear", batch_clear, MRB_ARGS_NONE());
  mrb_define_method(M, batch, "iterate", batch_iterate, MRB_ARGS_BLOCK());

  RClass *logger = mrb_define_class_under(M, db, "Logger", M->object_class);
  MRB_SET_INSTANCE_TT(logger, MRB_TT_DATA);
  mrb_define_method(M, logger, "initialize", logger_init, MRB_ARGS_REQ(1));
  mrb_define_method(M, logger, "log", logger_log, MRB_ARGS_REQ(1));

  RClass *snapshot = mrb_define_class_under(M, db, "Snapshot", M->object_class);
  MRB_SET_INSTANCE_TT(snapshot, MRB_TT_DATA);
  mrb_define_method(M, snapshot, "initialize", snapshot_init, MRB_ARGS_REQ(1));
  mrb_define_method(M, snapshot, "release", snapshot_release, MRB_ARGS_REQ(1));

  RClass *iterator = mrb_define_class_under(M, db, "Iterator", M->object_class);
  MRB_SET_INSTANCE_TT(iterator, MRB_TT_DATA);
  mrb_define_method(M, iterator, "initialize", iterator_init, MRB_ARGS_REQ(1) | MRB_ARGS_OPT(1));
  mrb_define_method(M, iterator, "delete", iterator_delete, MRB_ARGS_NONE());
  mrb_define_method(M, iterator, "valid?", iterator_valid_p, MRB_ARGS_NONE());
  mrb_define_method(M, iterator, "seek", iterator_seek, MRB_ARGS_REQ(1));
  mrb_define_method(M, iterator, "seek_to_first", iterator_seek_to_first, MRB_ARGS_NONE());
  mrb_define_method(M, iterator, "seek_to_last", iterator_seek_to_last, MRB_ARGS_NONE());
  mrb_define_method(M, iterator, "next", iterator_next, MRB_ARGS_NONE());
  mrb_define_method(M, iterator, "prev", iterator_prev, MRB_ARGS_NONE());
  mrb_define_method(M, iterator, "key", iterator_key, MRB_ARGS_NONE());
  mrb_define_method(M, iterator, "value", iterator_value, MRB_ARGS_NONE());
}

extern "C" void mrb_mruby_leveldb_gem_final(mrb_state*) {}

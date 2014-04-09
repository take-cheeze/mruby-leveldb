TEST_DB_NAME = 'leveldb'

def assert_leveldb name, opts = {}, &b
  assert name do
    db = LevelDB.open TEST_DB_NAME, opts
    b.call db
    db.close
  end
end

assert 'LevelDB.open/close' do
  db = LevelDB.open TEST_DB_NAME, :create_if_missing => true
  db.close

  assert_raise(LevelDB::Error) { db.get 'key' }
end

assert_leveldb 'LevelDB#put/get' do |db|
  assert_equal 'test', db.put('key', 'test', :sync => true)
  assert_equal 'test', db.get('key')
end

assert_leveldb 'LevelDB#[]' do |db|
  db.put 'key', 'test', :sync => true
  assert_equal 'test', db['key']
end

assert_leveldb 'LevelDB#delete' do |db|
  db.put 'key', 'test', :sync => true
  assert_equal 'test', db['key']

  db.delete 'key', :sync => true
  assert_nil db['key'] # when getting key that doesn't exist, mruby-leveldb returns nil
end

assert_leveldb 'LevelDB#write' do |db|
  db.put 'key', 'test'
  db.put 'mruby', 'test'

  assert_equal 'test', db['key']
  assert_equal 'test', db['mruby']

  db.write LevelDB::WriteBatch.new.delete('key').delete('mruby')

  assert_nil db['key']
  assert_nil db['mruby']
end

assert_leveldb 'LevelDB#property' do |db|
  assert_true db.property('leveldb.stats').kind_of? String
  assert_nil db.property 'invalid propertyyyyyy' # not found property
end

assert_leveldb 'LevelDB#approximate_sizes' do |db|
  db.put 'test', 'tt'
  db.put 'test1', 'ttt'
  db.put 'test2', 'tttt'
  db.put 'test3', 'ttttt'

  result = ['test']
  ret = db.approximate_sizes [['test', 'test1'], ['test', 'test2']], result
  assert_equal ret, result # when array passed it'll be cleared and used as result

  assert_equal [], db.approximate_sizes([])
end

assert_leveldb 'LevelDB#compact_range' do |db|
  assert_raise(ArgumentError) { db.compact_range '' }
  db.put 'test', 'tt'
  db.put 'test1', 'ttt'
  db.compact_range # compact all
  db.compact_range 'test', 'test1'
end

assert 'LevelDB::WriteBatch' do
  assert_raise(NameError) { WriteBatch.new }
  LevelDB::WriteBatch.new
end

assert 'LevelDB::WriteBatch#iterate' do
  b = LevelDB::WriteBatch.new
  b.put 'test', 'value'
  b.delete 'test'
  b.iterate do |t, k, v|
    case t
    when :put
      assert_equal 'test', k
      assert_equal 'value', v
    when :delete
      assert_equal 'test', k
      assert_nil v
    else
      assert_true false # should not reach
    end
  end
  assert_equal [[:put, 'test', 'value'], [:delete, 'test']], b.iterate
end

assert 'LevelDB::WriteBatch#put / []=' do
  b = LevelDB::WriteBatch.new
  b.put 'test', 'value'
  b['mruby'] = 'cute'
  assert_equal [[:put, 'test', 'value'], [:put, 'mruby', 'cute']], b.iterate
end

assert 'LevelDB::WriteBatch#delete' do
  b = LevelDB::WriteBatch.new
  b.delete 'test'
  assert_equal [[:delete, 'test']], b.iterate
end

assert 'LevelDB::WriteBatch#clear' do
  b = LevelDB::WriteBatch.new
  b.delete 'test'
  assert_equal [[:delete, 'test']], b.iterate
  b.clear
  assert_equal [], b.iterate
end

assert 'LevelDB::Logger' do
  logger = LevelDB::Logger.new 'leveldb.log'
  db = LevelDB.open TEST_DB_NAME, :info_log => logger
  logger.log 'test'
  db.close
end

assert 'LevelDB.destroy' do
  LevelDB.destroy TEST_DB_NAME
end

assert 'LevelDB.repair' do
  LevelDB.repair TEST_DB_NAME
end

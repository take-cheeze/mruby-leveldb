assert 'LevelDB.open/close' do
  db = LevelDB.open 'test-db', :create_if_missing => true
  db.close

  assert_raise(LevelDB::Error) { db.get 'key' }
end

assert 'LevelDB#put/get' do
  db = LevelDB.open 'test-db'

  assert_equal 'test', db.put('key', 'test', :sync => true)
  assert_equal 'test', db.get('key')

  db.close

  true
end

assert 'LevelDB#[]' do
  db = LevelDB.open 'test-db'

  db.put 'key', 'test', :sync => true
  assert_equal 'test', db['key']

  db.close
end

assert 'LevelDB#delete' do
  db = LevelDB.open 'test-db'

  db.put 'key', 'test', :sync => true
  assert_equal 'test', db['key']

  db.delete 'key', :sync => true
  assert_nil db['key']

  db.close
end

assert 'LevelDB#write' do
  db = LevelDB.open 'test-db'

  db.put 'key', 'test'
  db.put 'mruby', 'test'

  assert_equal 'test', db['key']
  assert_equal 'test', db['mruby']

  db.write LevelDB::WriteBatch.new.delete('key').delete('mruby')

  assert_nil db['key']
  assert_nil db['mruby']

  db.close
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

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

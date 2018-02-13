MRuby::Gem::Specification.new('mruby-leveldb') do |spec|
  spec.author = 'Takeshi Watanabe'
  spec.license = 'BSD3'
  spec.version = version = '1.20'

  spec.linker.libraries << 'leveldb' << 'pthread'

  next if build.cc.search_header_path 'leveldb/db.h'

  require 'open-uri'
  require 'open3'

  leveldb_dir = "#{build_dir}/leveldb-#{version}"
  leveldb_lib = libfile "#{leveldb_dir}/out-static/libleveldb"
  header = "#{leveldb_dir}/include/leveldb/db.h"

  task :clean do
    FileUtils.rm_rf [leveldb_dir, "#{build_dir}/leveldb-#{version}.tar.gz"]
  end

  file header do |t|
    FileUtils.mkdir_p leveldb_dir
    _pp 'Fetching ', "leveldb-#{version}"

    begin
      Dir.chdir build_dir do
        open "https://github.com/google/leveldb/archive/v#{version}.tar.gz" do |ar|
          f = File.open "leveldb-#{version}.tar.gz", 'wb'
          f.write ar.read
          f.close
        end
        _pp 'Extracting', "leveldb-#{version}"
        `tar -zxf leveldb-#{version}.tar.gz`
        raise IOError if $?.exitstatus != 0
      end
    rescue IOError
      File.delete "#{leveldb_dir}"
      fail "Fetching leveldb source code failed."
    end
  end

  file leveldb_lib => header do |t|
    Dir.chdir leveldb_dir do
      e = {
        'CC' => spec.build.cc.command,
        'CXX' => spec.build.cxx.command,
        'LD' => spec.build.linker.command,
        'AR' => spec.build.archiver.command }
      _pp 'Building', "leveldb-#{version}"
      Open3.popen2e(e, "make out-static/libleveldb.a") do |stdin, stdout, thread|
        print stdout.read
        fail "leveldb-#{version} build failed" if thread.value != 0
      end
    end
  end

  Dir.glob("#{dir}/src/*.cxx") { |f| file f => leveldb_lib }

  spec.cxx.include_paths << "#{leveldb_dir}/include"
  spec.linker.library_paths << File.dirname(leveldb_lib)
end

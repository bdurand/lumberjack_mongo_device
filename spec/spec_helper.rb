require 'rubygems'
require 'fileutils'

if ENV["MONGO_VERSION"]
  gem "mongo", ENV["MONGO_VERSION"]
else
  gem "mongo"
end

MONGO_PATH = ENV["MONGO_PATH"] || File.dirname(`which mongod`.chomp)

require File.expand_path("../../lib/lumberjack_mongo_device.rb", __FILE__)

def tmp_dir
  File.expand_path("../tmp", __FILE__)
end

def start_mongo
  FileUtils.rm_r(tmp_dir) if File.exist?(tmp_dir)
  FileUtils.mkdir_p(File.join(tmp_dir, "db"))
  `'#{MONGO_PATH}/mongod' --logpath '#{tmp_dir}/mongo.log' --pidfilepath '#{tmp_dir}/mongo.pid' --dbpath '#{tmp_dir}/db' --nohttpinterface --fork`
  
  # Ensure server is accepting connections
  stop_time = Time.now + 10
  loop do
    raise "Mongo server failed to start up in 10 seconds" if Time.now >= stop_time
    begin
      connection = Mongo::Connection.new
      connection.close
      break
    rescue Mongo::ConnectionFailure
      sleep(0.1)
    end
  end
end

def stop_mongo
  pid = File.read("#{tmp_dir}/mongo.pid").chomp
  `kill -15 #{pid}`
end

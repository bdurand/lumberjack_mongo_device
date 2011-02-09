require 'spec_helper'

describe Lumberjack::MongoDevice do
  
  before :all do
    start_mongo
  end
  
  after :all do
    stop_mongo
  end
  
  after :each do
    collection.drop
    collection.db.connection.close
  end
  
  let(:time_1){ Time.parse("2011-02-01T18:32:31Z") }
  let(:time_2){ Time.parse("2011-02-01T18:32:32Z") }
  let(:time_3){ Time.parse("2011-02-01T18:32:33Z") }
  let(:entry_1){ Lumberjack::LogEntry.new(time_1, Lumberjack::Severity::INFO, "message 1", "test", 12345, "ABCD") }
  let(:entry_2){ Lumberjack::LogEntry.new(time_2, Lumberjack::Severity::WARN, "message 2", "spec", 4321, "1234") }
  let(:entry_3){ Lumberjack::LogEntry.new(time_3, Lumberjack::Severity::ERROR, "message 3", "test", 12345, "ABCD") }
  
  let(:db){ Mongo::Connection.new.db("test") }
  let(:collection){ db.collection("log") }
  
  it "should use an existing collection" do
    device = Lumberjack::MongoDevice.new(collection)
    device.write(entry_1)
    device.flush
    collection.count.should == 1
  end
  
  it "should connect to a specified database collection on a specified host and port" do
    connection = db.connection
    collection
    Mongo::Connection.should_receive(:new).with("127.0.0.1", 12345, :safe => true).and_return(connection)
    connection.should_receive(:db).with("test").and_return(db)
    db.should_receive(:create_collection).with("log", :capped => nil, :size => nil, :max => nil).and_return(collection)
    device = Lumberjack::MongoDevice.new(:host => "127.0.0.1", :port => 12345, :safe => true, :db => "test", :collection => "log")
  end
  
  it "should connect to a server and authenticate to use the database if :username and :password are specified" do
    connection = db.connection
    collection
    Mongo::Connection.should_receive(:new).and_return(connection)
    connection.should_receive(:db).with("test").and_return(db)
    db.should_receive(:authenticate).with("user", "pass")
    device = Lumberjack::MongoDevice.new(:username => "user", :password => "pass", :db => "test", :collection => "log")
  end
  
  it "should connect to a server and create a new collection if it doesn't exist" do
    db.collections.collect{|c| c.name}.should_not include("log")
    device = Lumberjack::MongoDevice.new(:db => "test", :collection => "log")
    db.collections.collect{|c| c.name}.should include("log")
    collection.index_information.collect{|k, v| v["key"].collect{|k1, v1| k1}.join}.sort.should == ["_id", "time", "unit_of_work_id"]
  end
  
  it "should connect to a server and create a new capped collection if it doesn't exist and :size is specified" do
    db.collections.collect{|c| c.name}.should_not include("log")
    device = Lumberjack::MongoDevice.new(:db => "test", :collection => "log", :size => 32768)
    db.collections.collect{|c| c.name}.should include("log")
    collection.options["capped"].should
    collection.options["size"].should == 32768
  end
  
  it "should connect to a server and create a new capped collection if it doesn't exist and :max is specified" do
    db.collections.collect{|c| c.name}.should_not include("log")
    device = Lumberjack::MongoDevice.new(:db => "test", :collection => "log", :max => 1000)
    db.collections.collect{|c| c.name}.should include("log")
    collection.options["capped"].should
    collection.options["max"].should == 1000
  end
  
  it "should connect to a server and use an existing collection if it exists" do
    collection.create_index(:pid)
    db.collections.collect{|c| c.name}.should include("log")
    device = Lumberjack::MongoDevice.new(:db => "test", :collection => "log", :max => 1000)
    db.collections.collect{|c| c.name}.should include("log")
    collection.options.should == nil
    collection.index_information.collect{|k, v| v["key"].collect{|k1, v1| k1}.join}.sort.should == ["_id", "pid"]
  end
  
  it "should write entries to the collection" do
    device = Lumberjack::MongoDevice.new(:db => "test", :collection => "log")
    device.write(entry_1)
    device.write(entry_2)
    device.write(entry_3)
    device.flush
    collection.count.should == 3
    doc = collection.find_one(:message => "message 1")
    doc["time"].should == time_1
    doc["severity"].should == "INFO"
    doc["progname"].should == entry_1.progname
    doc["pid"].should == entry_1.pid
    doc["unit_of_work_id"].should == entry_1.unit_of_work_id
    doc["message"].should == entry_1.message
  end
  
  it "should close and flush a connection" do
    device = Lumberjack::MongoDevice.new(:db => "test", :collection => "log")
    connection = device.collection.db.connection
    connection.should_receive(:close)
    device.should_receive(:flush)
    device.close
  end
  
  context "buffering" do
    it "should buffer log entries and insert them in batches" do
      device = Lumberjack::MongoDevice.new(:db => "test", :collection => "log")
      device.write(entry_1)
      device.write(entry_2)
      device.write(entry_3)
      collection.count.should == 0
      device.flush
      collection.count.should == 3
    end
  
    it "should be able to control the buffer size" do
      device = Lumberjack::MongoDevice.new(:db => "test", :collection => "log")
      device.buffer_size = 2
      device.write(entry_1)
      collection.count.should == 0
      device.write(entry_2)
      collection.count.should == 2
      device.write(entry_3)
      collection.count.should == 2
      device.flush
      collection.count.should == 3
    end
  
    it "should set the buffer size on initialize with options" do
      device = Lumberjack::MongoDevice.new(:db => "test", :collection => "log", :buffer_size => 2)
      device.buffer_size = 2
      device.write(entry_1)
      collection.count.should == 0
      device.write(entry_2)
      collection.count.should == 2
      device.write(entry_3)
      collection.count.should == 2
      device.flush
      collection.count.should == 3
    end
  
    it "should set the buffer size on initialize with a collection" do
      device = Lumberjack::MongoDevice.new(collection, :buffer_size => 2)
      device.buffer_size = 2
      device.write(entry_1)
      collection.count.should == 0
      device.write(entry_2)
      collection.count.should == 2
      device.write(entry_3)
      collection.count.should == 2
      device.flush
      collection.count.should == 3
    end
    
    it "should output to standard error if the buffer can't be written'" do
    end
  end
  
  context "finding" do
    let(:device){ Lumberjack::MongoDevice.new(:db => "test", :collection => "log") }
    
    before :each do
      device.write(entry_1)
      device.write(entry_2)
      device.write(entry_3)
      device.flush
    end
    
    it "should find entries and yield them to a block" do
      entries = []
      device.find({:progname => "test"}, :sort => :time) do |entry|
        entries << entry
      end
    
      entries.size.should == 2
    
      entries.first.time.should == entry_1.time
      entries.first.severity.should == entry_1.severity
      entries.first.progname.should == entry_1.progname
      entries.first.pid.should == entry_1.pid
      entries.first.unit_of_work_id.should == entry_1.unit_of_work_id
      entries.first.message.should == entry_1.message
    
      entries.last.time.should == entry_3.time
      entries.last.severity.should == entry_3.severity
      entries.last.progname.should == entry_3.progname
      entries.last.pid.should == entry_3.pid
      entries.last.unit_of_work_id.should == entry_3.unit_of_work_id
      entries.last.message.should == entry_3.message
    end
  
    it "should find entries and return them as an array" do
      entries = device.find({:progname => "test"}, :sort => :time)
    
      entries.size.should == 2
    
      entries.first.time.should == entry_1.time
      entries.first.severity.should == entry_1.severity
      entries.first.progname.should == entry_1.progname
      entries.first.pid.should == entry_1.pid
      entries.first.unit_of_work_id.should == entry_1.unit_of_work_id
      entries.first.message.should == entry_1.message
    
      entries.last.time.should == entry_3.time
      entries.last.severity.should == entry_3.severity
      entries.last.progname.should == entry_3.progname
      entries.last.pid.should == entry_3.pid
      entries.last.unit_of_work_id.should == entry_3.unit_of_work_id
      entries.last.message.should == entry_3.message
    end
  
    it "should find the last entries in the log" do
      device.last.collect{|e| e.message}.should == [entry_3.message]
      device.last(2).collect{|e| e.message}.should == [entry_2.message, entry_3.message]
    end
  end
end

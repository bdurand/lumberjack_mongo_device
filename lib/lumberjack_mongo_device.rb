require 'mongo'
require 'lumberjack'

module Lumberjack
  # Write Lumberjack log entries to a MongoDB collection.
  #
  # Log entries will be stored as documents in a collection with fields for:
  #
  # * time
  # * severity (as a string i.e. "DEBUG")
  # * progname
  # * pid
  # * unit_of_work_id
  # * message
  class MongoDevice < Device
    TIME = "time"
    SEVERITY = "severity"
    PROGNAME = "progname"
    PID = "pid"
    UNIT_OF_WORK_ID = "unit_of_work_id"
    MESSAGE = "message"
    
    DEFAULT_BUFFER_SIZE = 0
    
    # Get the MongoDB collection that is being written to.
    attr_reader :collection
    
    # The size of the internal buffer. Log entries are buffered so they can be sent to MongoDB in batches for efficiency.
    attr_reader :buffer_size
    
    # Initialize the device by passing in either a Mongo::Collection object or a hash of options
    # to create the collection. Available options are:
    #
    # * <tt>:host</tt> - The host name to connect to (defaults to localhost).
    # * <tt>:port</tt> - The port to connect to (defaults to 27017).
    # * <tt>:db</tt> - The database name to use (required).
    # * <tt>:collection</tt> - The collection name to use (required).
    # * <tt>:username</tt> - The username to authenticate with for database connections (optional).
    # * <tt>:password</tt> - The password to authenticate with for database connections (optional).
    # * <tt>:max</tt> - If the collection does not aleady exist it will be capped at this number of records.
    # * <tt>:size</tt> - If the collection does not aleady exist it will be capped at this size in bytes.
    # * <tt>:buffer_size</tt> - The number of entries that will be buffered before they are sent to MongoDB. Default is not to buffer.
    #
    # If the collection does not already exist, it will be created. If either the <tt>:max</tt> or <tt>:size</tt>
    # options are provided, it will be created as a capped collection. Indexes will be created on +unit_of_work_id+
    # and +time+.
    def initialize(collection_or_options, options = nil)
      if collection_or_options.is_a?(Hash)
        options = collection_or_options.dup
        host = options.delete(:host)
        port = options.delete(:port)
        db_name = options.delete(:db)
        collection = options.delete(:collection)
        username = options.delete(:username)
        password = options.delete(:password)
        max = options.delete(:max)
        size = options.delete(:size)
        
        @buffer_size = options.delete(:buffer_size) || DEFAULT_BUFFER_SIZE
        
        connection = Mongo::Connection.new(host, port, options)
        db = connection.db(db_name)
        db.authenticate(username, password) if username && password
        if db.collections.collect{|coll| coll.name}.include?(collection.to_s)
          @collection = db.collection(collection)
        else
          begin
            @collection = db.create_collection(collection, :capped => (max || size), :max => max, :size => size)
            @collection.ensure_index(:time)
            @collection.ensure_index(:unit_of_work_id)
          rescue Mongo::OperationFailure
            # Create collection can fail if multiple processes try to create it at once.
            @collection = db.collection(collection)
            raise unless @collection
          end
        end
      else
        @collection = collection_or_options
        @buffer_size = options[:buffer_size] if options
        @buffer_size ||= DEFAULT_BUFFER_SIZE
      end
      
      @buffer = []
      @lock = Mutex.new
    end
    
    # Set the buffer size in bytes. The device will only be physically written to when the buffer size
    # is exceeded.
    def buffer_size=(value)
      @buffer_size = value
      flush
    end
    
    def write(entry)
      @lock.synchronize do
        @buffer << entry
      end
      flush if @buffer.size >= @buffer_size
    end
    
    def flush
      docs = []
      @lock.synchronize do
        @buffer.each do |entry|
          docs << {:time => entry.time, :severity => entry.severity_label, :progname => entry.progname, :pid => entry.pid, :unit_of_work_id => entry.unit_of_work_id, :message => entry.message}
        end
        begin
          @collection.insert(docs)
        rescue => e
          puts e.inspect
          puts e.backtrace.join("\n")
          $stderr.write("#{e.class.name}: #{e.message}#{' at ' + e.backtrace.first if e.backtrace}")
          @buffer.each do |entry|
            $stderr.puts(entry.to_s)
          end
          $stderr.flush
        ensure
          @buffer.clear
        end
      end
    end
    
    def close
      flush
      @lock.synchronize do
        @collection.db.connection.close
      end
    end
    
    # Retrieve Lumberjack::LogEntry objects from the MongoDB collection. If a block is given, it will be yielded to
    # with each entry. Otherwise, it will return an array of all the entries.
    def find(selector, options = {}, &block)
      entries = []
      @collection.find(selector, options) do |cursor|
        cursor.each do |doc|
          entry = LogEntry.new(doc[TIME], doc[SEVERITY], doc[MESSAGE], doc[PROGNAME], doc[PID], doc[UNIT_OF_WORK_ID])
          if block_given?
            yield entry
          else
            entries << entry
          end
        end
      end
      block_given? ? nil : entries
    end
    
    # Retrieve the last entries from the log.
    def last(number_of_entries = 1)
      find(nil, :sort => [:_id, :descending], :limit => number_of_entries).reverse
    end
  end
end

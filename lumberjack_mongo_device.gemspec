Gem::Specification.new do |s|
  s.name = 'lumberjack_mongo_device'
  s.version = File.read(File.expand_path("../VERSION", __FILE__)).strip
  s.summary = "A logging device for the lumberjack gem that writes log entries to a MongoDB collection."
  s.description = "A logging device for the lumberjack gem that writes log entries to a MongoDB collection."

  s.authors = ['Brian Durand']
  s.email = ['bdurand@embellishedvisions.com']
  s.homepage = "http://github.com/bdurand/lumberjack_mongo_device"

  s.files = ['README.rdoc', 'VERSION', 'Rakefile', 'MIT_LICENSE'] +  Dir.glob('lib/**/*'), Dir.glob('spec/**/*')
  s.require_path = 'lib'
  
  s.has_rdoc = true
  s.rdoc_options = ["--charset=UTF-8", "--main", "README.rdoc"]
  s.extra_rdoc_files = ["README.rdoc"]
  
  s.add_dependency "mongo", ">=1.1.5"
  s.add_dependency "lumberjack"
end

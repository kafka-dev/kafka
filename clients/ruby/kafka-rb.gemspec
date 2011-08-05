# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{kafka-rb}
  s.version = "0.0.5"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Alejandro Crosa"]
  s.autorequire = %q{kafka-rb}
  s.date = %q{2011-01-13}
  s.description = %q{kafka-rb allows you to produce and consume messages using the Kafka distributed publish/subscribe messaging service.}
  s.email = %q{alejandrocrosa@gmail.com}
  s.extra_rdoc_files = ["LICENSE"]
  s.files = ["LICENSE", "README.md", "Rakefile", "lib/kafka", "lib/kafka/error_codes.rb", "lib/kafka/batch.rb", "lib/kafka/consumer.rb", "lib/kafka/io.rb", "lib/kafka/message.rb", "lib/kafka/producer.rb", "lib/kafka/request_type.rb", "lib/kafka.rb", "spec/batch_spec.rb", "spec/consumer_spec.rb", "spec/io_spec.rb", "spec/kafka_spec.rb", "spec/message_spec.rb", "spec/producer_spec.rb", "spec/spec_helper.rb"]
  s.homepage = %q{http://github.com/acrosa/kafka-rb}
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.7}
  s.summary = %q{A Ruby client for the Kafka distributed publish/subscribe messaging service}

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<rspec>, [">= 0"])
    else
      s.add_dependency(%q<rspec>, [">= 0"])
    end
  else
    s.add_dependency(%q<rspec>, [">= 0"])
  end
end

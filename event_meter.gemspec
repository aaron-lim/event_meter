require_relative "lib/event_meter/version"

Gem::Specification.new do |spec|
  spec.name = "event_meter"
  spec.version = EventMeter::VERSION
  spec.authors = ["Aaron Lim"]
  spec.email = ["aaron.lim.yu.kwang@gmail.com"]

  spec.summary = "Small event-based runtime metrics for Ruby applications."
  spec.description = [
    "EventMeter records application events and turns them into storage-backed",
    "metrics for counts, speed, duration, and intervals."
  ].join(" ")
  spec.homepage = "https://github.com/aaron-lim/event_meter"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir.chdir(__dir__) do
    Dir["exe/*", "lib/**/*", "README.md", "LICENSE.txt"]
  end
  spec.bindir = "exe"
  spec.executables = ["event_meter"]
  spec.require_paths = ["lib"]

  spec.add_dependency "time_bucket_stream", "~> 0.1"

  spec.add_development_dependency "minitest", "~> 5.0"
  spec.add_development_dependency "pg", "~> 1.5"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "redis", ">= 5.0", "< 6.0"
end

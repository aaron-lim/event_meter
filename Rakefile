require "rake/testtask"

Rake::TestTask.new(:test) do |test|
  test.libs << "test"
  test.pattern = "test/**/*_test.rb"
end

Rake::TestTask.new(:soak) do |test|
  test.libs << "test"
  test.pattern = "test/soak.rb"
end

Rake::TestTask.new(:performance) do |test|
  test.libs << "test"
  test.pattern = "test/performance_test.rb"
end

task default: :test

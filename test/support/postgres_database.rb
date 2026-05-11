module EventMeterTestSupport
  module PostgresDatabase
    DEFAULT_DATABASE = "event_meter_test"
    DEFAULT_URL = "postgres:///#{DEFAULT_DATABASE}"
    MAINTENANCE_DATABASES = %w[postgres template1].freeze

    module_function

    def url(test:)
      explicit_url = ENV["EVENT_METER_POSTGRES_URL"].to_s.strip
      return explicit_url unless explicit_url.empty?

      database_url = ENV["DATABASE_URL"].to_s.strip
      return database_url unless database_url.empty?

      ensure_local_database!(test)
      DEFAULT_URL
    end

    def connect(test:, url: nil)
      require "pg"

      PG.connect(url || self.url(test: test))
    rescue LoadError => error
      test.skip "pg is unavailable: #{error.class}: #{error.message}"
    rescue Minitest::Skip
      raise
    rescue StandardError => error
      raise unless postgres_error?(error)

      test.skip "PostgreSQL is unavailable: #{error.class}: #{error.message}"
    end

    def ensure_local_database!(test)
      require "pg"

      connection = PG.connect(dbname: DEFAULT_DATABASE)
      connection.close
    rescue LoadError => error
      test.skip "pg is unavailable: #{error.class}: #{error.message}"
    rescue Minitest::Skip
      raise
    rescue StandardError => error
      raise unless postgres_error?(error)

      if missing_database?(error)
        create_local_database!(test)
      else
        test.skip "Local PostgreSQL is unavailable: #{error.class}: #{error.message}"
      end
    end

    def create_local_database!(test)
      connection = maintenance_connection(test)
      connection.exec("CREATE DATABASE #{connection.escape_identifier(DEFAULT_DATABASE)}")
    rescue StandardError => error
      raise unless postgres_error?(error)

      return if duplicate_database?(error)

      test.skip "Local PostgreSQL database #{DEFAULT_DATABASE.inspect} could not be created: #{error.class}: #{error.message}"
    ensure
      connection&.close
    end

    def maintenance_connection(test)
      last_error = nil

      MAINTENANCE_DATABASES.each do |database|
        return PG.connect(dbname: database)
      rescue StandardError => error
        raise unless postgres_error?(error)

        last_error = error
      end

      test.skip "Local PostgreSQL is unavailable: #{last_error.class}: #{last_error.message}"
    end

    def postgres_error?(error)
      defined?(PG::Error) && error.is_a?(PG::Error)
    end

    def missing_database?(error)
      error.is_a?(PG::InvalidCatalogName) || error.message.include?("database \"#{DEFAULT_DATABASE}\" does not exist")
    end

    def duplicate_database?(error)
      error.is_a?(PG::DuplicateDatabase) || error.message.include?("already exists")
    end
  end
end

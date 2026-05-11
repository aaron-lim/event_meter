require "optparse"

require_relative "../event_meter"

module EventMeter
  class CLI
    def self.call(argv, out: $stdout, err: $stderr)
      new(argv, out: out, err: err).call
    end

    def initialize(argv, out:, err:)
      @argv = argv.dup
      @out = out
      @err = err
    end

    def call
      case argv.shift
      when "postgres"
        postgres
      when "help", "-h", "--help", nil
        out.puts usage
        0
      else
        err.puts usage
        1
      end
    rescue OptionParser::ParseError, ArgumentError => error
      err.puts error.message
      1
    end

    private

    attr_reader :argv, :out, :err

    def postgres
      case argv.shift
      when "install"
        postgres_install
      when "schema"
        postgres_schema
      when "help", "-h", "--help", nil
        out.puts postgres_usage
        0
      else
        err.puts postgres_usage
        1
      end
    end

    def postgres_install
      options = postgres_options
      parse_postgres_options!(options)

      url = options.fetch(:url) || ENV["DATABASE_URL"]
      raise ArgumentError, "missing database URL; pass --url or set DATABASE_URL" if url.to_s.strip.empty?

      require_pg!

      connection = PG.connect(url)
      EventMeter::Stores::Rollup::Postgres.install!(
        connection: connection,
        table_prefix: options.fetch(:table_prefix)
      )
      out.puts "Installed EventMeter PostgreSQL tables with prefix #{options.fetch(:table_prefix)}"
      0
    ensure
      connection&.close
    end

    def postgres_schema
      options = postgres_options
      parse_postgres_options!(options)

      out.puts EventMeter::Stores::Rollup::Postgres.schema_sql(
        table_prefix: options.fetch(:table_prefix)
      )
      0
    end

    def postgres_options
      {
        table_prefix: "event_meter",
        url: nil
      }
    end

    def parse_postgres_options!(options)
      parser = OptionParser.new do |parser_config|
        parser_config.on("--table-prefix PREFIX", "PostgreSQL table prefix") do |value|
          options[:table_prefix] = value
        end
        parser_config.on("--url URL", "PostgreSQL connection URL") do |value|
          options[:url] = value
        end
      end
      parser.parse!(argv)
      raise OptionParser::InvalidArgument, argv.join(" ") unless argv.empty?
    end

    def require_pg!
      require "pg"
    rescue LoadError
      raise ArgumentError, "pg is required for postgres install; add gem \"pg\" to your app"
    end

    def usage
      <<~TEXT
        Usage:
          event_meter postgres schema [--table-prefix PREFIX]
          event_meter postgres install [--url DATABASE_URL] [--table-prefix PREFIX]
      TEXT
    end

    def postgres_usage
      <<~TEXT
        Usage:
          event_meter postgres schema [--table-prefix PREFIX]
          event_meter postgres install [--url DATABASE_URL] [--table-prefix PREFIX]
      TEXT
    end
  end
end

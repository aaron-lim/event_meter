require_relative "postgres"

module EventMeter
  module Stores
    module Rollup
      class ActiveRecordPostgres < Postgres
        class Connection
          TRANSACTION_COMMANDS = {
            "BEGIN" => :begin,
            "COMMIT" => :finish,
            "ROLLBACK" => :finish
          }.freeze

          attr_reader :connection_class

          def initialize(connection_class)
            @connection_class = connection_class
            @transaction_connection = nil
          end

          def exec(sql)
            with_connection_for(sql) { |connection| connection.exec(sql) }
          end

          def exec_params(sql, params)
            with_connection_for(sql) { |connection| connection.exec_params(sql, params) }
          end

          private

          def with_connection_for(sql)
            command = transaction_command(sql)

            return begin_transaction { |connection| yield connection } if command == :begin
            return finish_transaction { |connection| yield connection } if command == :finish
            return yield active_raw_connection if transaction_open?

            connection_pool.with_connection do |connection|
              yield connection.raw_connection
            end
          end

          def begin_transaction
            raise ConfigurationError, "event_meter postgres transaction is already open" if transaction_open?

            @transaction_connection = connection_pool.checkout
            yield active_raw_connection
          rescue StandardError
            checkin_transaction_connection
            raise
          end

          def finish_transaction
            return with_connection_for_non_transaction { |connection| yield connection } unless transaction_open?

            yield active_raw_connection
          ensure
            checkin_transaction_connection
          end

          def with_connection_for_non_transaction
            connection_pool.with_connection do |connection|
              yield connection.raw_connection
            end
          end

          def transaction_command(sql)
            TRANSACTION_COMMANDS[sql.to_s.strip.upcase]
          end

          def transaction_open?
            !@transaction_connection.nil?
          end

          def active_raw_connection
            @transaction_connection.raw_connection
          end

          def checkin_transaction_connection
            connection = @transaction_connection
            @transaction_connection = nil
            connection_pool.checkin(connection) if connection
          end

          def connection_pool
            connection_class.connection_pool
          end
        end

        attr_reader :connection_class

        def self.install!(connection_class: default_connection_class, table_prefix: "event_meter")
          connection_class.connection_pool.with_connection do |connection|
            Postgres.install!(
              connection: connection.raw_connection,
              table_prefix: table_prefix
            )
          end
        end

        def self.default_connection_class
          return ::ActiveRecord::Base if defined?(::ActiveRecord::Base)

          raise ConfigurationError, "ActiveRecord is required for active_record_postgres rollup storage"
        end

        def initialize(connection_class: self.class.default_connection_class, lock_connection: nil, **options)
          @connection_class = connection_class
          super(
            connection: Connection.new(connection_class),
            lock_connection: lock_connection || Connection.new(connection_class),
            **options
          )
        end

        def for_report(name:, version:)
          name = name.to_s
          version = version.to_i

          self.class.new(
            connection_class: connection_class,
            connection_lock: connection_lock,
            lock_connection: lock_connection,
            lock_connection_lock: lock_connection_lock,
            namespace: namespace,
            table_prefix: table_prefix,
            report_name: name,
            version: version,
            lock_scope: "#{Keys.event_name(name)}:#{Keys.version_key(version)}"
          )
        end
      end
    end
  end
end

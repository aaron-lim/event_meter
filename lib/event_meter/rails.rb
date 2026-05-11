require_relative "../event_meter"
require_relative "stores/rollup/active_record_postgres"

module EventMeter
  module Rails
    module_function

    def configure(namespace:, stream_path:, stream_storage: :file, rollup_storage: :postgres,
      table_prefix: "event_meter", connection_class: nil, stream_sync: :flush,
      auto_cleanup_history: false, cleanup_history_retention: nil,
      cleanup_history_interval: nil, summary_key_limit: nil, logger: nil)

      connection_class ||= Stores::Rollup::ActiveRecordPostgres.default_connection_class

      EventMeter.configure do |config|
        config.namespace = namespace
        config.stream_storage = build_stream_storage(
          stream_storage,
          path: stream_path,
          sync: stream_sync
        )
        config.rollup_storage = build_rollup_storage(
          rollup_storage,
          connection_class: connection_class,
          namespace: namespace,
          table_prefix: table_prefix
        )
        config.auto_cleanup_history = auto_cleanup_history
        config.cleanup_history_retention = cleanup_history_retention if cleanup_history_retention
        config.cleanup_history_interval = cleanup_history_interval if cleanup_history_interval
        config.summary_key_limit = summary_key_limit if summary_key_limit
        config.auto_cleanup_error_handler = auto_cleanup_error_handler(logger) if logger
      end
    end

    def migration_sql(table_prefix: "event_meter")
      Stores::Rollup::Postgres.schema_sql(table_prefix: table_prefix)
    end

    def install_postgres!(connection_class: nil, table_prefix: "event_meter")
      Stores::Rollup::ActiveRecordPostgres.install!(
        connection_class: connection_class || Stores::Rollup::ActiveRecordPostgres.default_connection_class,
        table_prefix: table_prefix
      )
    end

    def auto_cleanup_error_handler(logger)
      lambda do |error|
        logger.warn "EventMeter auto cleanup failed: #{error.class}: #{error.message}"
      end
    end

    def build_stream_storage(storage, path:, sync:)
      validate_storage!(storage, expected: :file, name: "stream_storage")

      Stores::Stream::File.new(path: path, sync: sync)
    end

    def build_rollup_storage(storage, connection_class:, namespace:, table_prefix:)
      validate_storage!(storage, expected: :postgres, name: "rollup_storage")

      Stores::Rollup::ActiveRecordPostgres.new(
        connection_class: connection_class,
        namespace: namespace,
        table_prefix: table_prefix
      )
    end

    def validate_storage!(storage, expected:, name:)
      return if storage_key(storage) == expected

      raise ArgumentError, "#{name} must be #{expected.inspect}"
    end

    def storage_key(storage)
      storage.respond_to?(:to_sym) ? storage.to_sym : storage
    end
  end
end

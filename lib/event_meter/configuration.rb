require "monitor"

module EventMeter
  class Configuration
    DEFAULT_NAMESPACE = "event_meter:v1"
    DEFAULT_REDIS_READ_LIMIT = nil
    DEFAULT_ROLLUP_TTL = 31 * 24 * 60 * 60
    DEFAULT_LOCK_TTL = 30
    DEFAULT_AUTO_CLEANUP_HISTORY = false
    DEFAULT_CLEANUP_HISTORY_RETENTION = DEFAULT_ROLLUP_TTL
    DEFAULT_CLEANUP_HISTORY_INTERVAL = 60 * 60
    DEFAULT_SUMMARY_KEY_LIMIT = 10_000
    DEFAULT_AUTO_CLEANUP_ERROR_HANDLER = lambda do |error|
      warn "EventMeter auto cleanup failed: #{error.class}: #{error.message}"
    end

    attr_reader :namespace, :redis, :redis_read_limit, :rollup_ttl, :lock_ttl,
      :auto_cleanup_history, :cleanup_history_retention, :cleanup_history_interval,
      :summary_key_limit, :auto_cleanup_error_handler

    def initialize
      @configuration_lock = Monitor.new
      @namespace = DEFAULT_NAMESPACE
      @redis_read_limit = DEFAULT_REDIS_READ_LIMIT
      @rollup_ttl = DEFAULT_ROLLUP_TTL
      @lock_ttl = DEFAULT_LOCK_TTL
      @auto_cleanup_history = DEFAULT_AUTO_CLEANUP_HISTORY
      @cleanup_history_retention = DEFAULT_CLEANUP_HISTORY_RETENTION
      @cleanup_history_interval = DEFAULT_CLEANUP_HISTORY_INTERVAL
      @summary_key_limit = DEFAULT_SUMMARY_KEY_LIMIT
      @auto_cleanup_error_handler = DEFAULT_AUTO_CLEANUP_ERROR_HANDLER
    end

    def namespace=(value)
      namespace = value.to_s
      raise ArgumentError, "namespace cannot be blank" if namespace.strip.empty?

      synchronize do
        @namespace = namespace
        reset_default_storages
      end
    end

    def redis=(client_or_factory)
      synchronize do
        @redis = client_or_factory
        reset_redis_client_cache
        reset_default_storages
      end
    end

    def redis_read_limit=(value)
      redis_read_limit = positive_integer_or_nil(value, "redis_read_limit")

      synchronize do
        @redis_read_limit = redis_read_limit
        reset_default_storages
      end
    end

    def rollup_ttl=(value)
      rollup_ttl = positive_integer(value, "rollup_ttl")

      synchronize do
        @rollup_ttl = rollup_ttl
        reset_default_rollup_storage
      end
    end

    def lock_ttl=(value)
      @lock_ttl = positive_integer(value, "lock_ttl")
    end

    def auto_cleanup_history=(value)
      @auto_cleanup_history = boolean(value, "auto_cleanup_history")
    end

    def cleanup_history_retention=(value)
      @cleanup_history_retention = positive_integer(value, "cleanup_history_retention")
    end

    def cleanup_history_interval=(value)
      @cleanup_history_interval = positive_integer(value, "cleanup_history_interval")
    end

    def summary_key_limit=(value)
      @summary_key_limit = positive_integer_or_nil(value, "summary_key_limit")
    end

    def auto_cleanup_error_handler=(handler)
      unless handler.nil? || handler.respond_to?(:call)
        raise ArgumentError, "auto_cleanup_error_handler must respond to call"
      end

      @auto_cleanup_error_handler = handler
    end

    def stream_storage
      synchronize do
        reset_redis_clients_if_stale
        @stream_storage ||= default_stream_storage
      end
    end

    def stream_storage=(storage)
      raise ArgumentError, "stream_storage cannot be nil" if storage.nil?

      synchronize do
        @stream_storage = storage
        @stream_storage_default = false
      end
    end

    def rollup_storage
      synchronize do
        reset_redis_clients_if_stale
        @rollup_storage ||= default_rollup_storage
      end
    end

    def rollup_storage=(storage)
      raise ArgumentError, "rollup_storage cannot be nil" if storage.nil?

      synchronize do
        @rollup_storage = storage
        @rollup_storage_default = false
      end
    end

    private

    def synchronize(&block)
      @configuration_lock.synchronize(&block)
    end

    def default_stream_storage
      return missing_storage!("stream_storage") unless redis

      @stream_storage_default = true
      Stores::Stream::Redis.new(
        redis: redis_client(:stream),
        lock_redis: redis_client(:stream_lock),
        namespace: namespace,
        redis_read_limit: redis_read_limit
      )
    end

    def default_rollup_storage
      return missing_storage!("rollup_storage") unless redis

      @rollup_storage_default = true
      Stores::Rollup::Redis.new(
        redis: redis_client(:rollup),
        lock_redis: redis_client(:rollup_lock),
        namespace: namespace,
        rollup_ttl: rollup_ttl
      )
    end

    def reset_default_storages
      @stream_storage = nil if @stream_storage_default
      reset_default_rollup_storage
    end

    def reset_default_rollup_storage
      @rollup_storage = nil if @rollup_storage_default
    end

    def redis_client(purpose = :default)
      synchronize do
        reset_redis_clients_if_stale
        initialize_redis_client_cache
        @redis_clients[purpose] ||= begin
          client = redis.is_a?(Proc) ? redis.call : redis
          raise ConfigurationError, "redis client cannot be nil" if client.nil?
          raise ConfigurationError, "redis client cannot be closed" if redis_client_closed?(client)

          client
        end
      end
    end

    def reset_redis_clients_if_stale
      return unless @redis_clients
      return unless redis_client_cache_stale?

      reset_redis_client_cache
      reset_default_storages
    end

    def reset_redis_client_cache
      @redis_clients = nil
      @redis_client_pid = nil
    end

    def initialize_redis_client_cache
      return if @redis_clients

      @redis_clients = {}
      @redis_client_pid = process_id
    end

    def redis_client_cache_stale?
      @redis_client_pid != process_id || @redis_clients.any? do |_purpose, client|
        redis_client_closed?(client)
      end
    end

    def process_id
      Process.pid
    end

    def redis_client_closed?(client)
      client.respond_to?(:closed?) && client.closed?
    rescue StandardError
      false
    end

    def missing_storage!(name)
      raise ConfigurationError, "configure #{name}, or set config.redis to use Redis storage"
    end

    def positive_integer(value, name)
      integer = Integer(value)
      return integer if integer.positive?

      raise ArgumentError, "#{name} must be positive"
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "#{name} must be positive"
    end

    def positive_integer_or_nil(value, name)
      return nil if value.nil?

      positive_integer(value, name)
    end

    def boolean(value, name)
      return value if value == true || value == false

      raise ArgumentError, "#{name} must be true or false"
    end
  end
end

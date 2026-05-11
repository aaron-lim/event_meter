require "test_helper"

class EventMeterConfigurationTest < EventMeterTest
  def test_config_accepts_a_redis_client_or_a_redis_factory
    direct_client = Object.new
    factory_client = Object.new
    replacement_client = Object.new

    EventMeter.configure { |config| config.redis = direct_client }
    assert_same direct_client, EventMeter.configuration.send(:redis_client)

    EventMeter.configuration.redis = replacement_client
    assert_same replacement_client, EventMeter.configuration.send(:redis_client)

    EventMeter.reset
    EventMeter.configure { |config| config.redis = -> { factory_client } }
    assert_same factory_client, EventMeter.configuration.send(:redis_client)
  end

  def test_configure_without_a_block_returns_the_configuration
    assert_same EventMeter.configuration, EventMeter.configure
  end

  def test_namespace_cannot_be_blank
    config = EventMeter::Configuration.new

    assert_raises(ArgumentError) { config.namespace = "" }
    assert_raises(ArgumentError) { config.namespace = nil }
  end

  def test_numeric_limits_must_be_positive
    config = EventMeter::Configuration.new

    %i[
      rollup_ttl
      lock_ttl
      cleanup_history_retention
      cleanup_history_interval
    ].each do |attribute|
      assert_raises(ArgumentError) { config.public_send("#{attribute}=", 0) }
      assert_raises(ArgumentError) { config.public_send("#{attribute}=", nil) }
      assert_raises(ArgumentError) { config.public_send("#{attribute}=", Float::NAN) }

      config.public_send("#{attribute}=", "2")
      assert_equal 2, config.public_send(attribute)
    end
  end

  def test_summary_key_limit_can_be_nil_or_positive
    config = EventMeter::Configuration.new

    assert_equal 10_000, config.summary_key_limit

    config.summary_key_limit = nil
    assert_nil config.summary_key_limit

    config.summary_key_limit = "2"
    assert_equal 2, config.summary_key_limit

    assert_raises(ArgumentError) { config.summary_key_limit = 0 }
    assert_raises(ArgumentError) { config.summary_key_limit = Float::NAN }
  end

  def test_auto_cleanup_history_is_explicitly_boolean
    config = EventMeter::Configuration.new

    assert_equal false, config.auto_cleanup_history

    config.auto_cleanup_history = true
    assert_equal true, config.auto_cleanup_history

    config.auto_cleanup_history = false
    assert_equal false, config.auto_cleanup_history

    assert_raises(ArgumentError) { config.auto_cleanup_history = nil }
    assert_raises(ArgumentError) { config.auto_cleanup_history = "true" }
  end

  def test_auto_cleanup_error_handler_can_be_nil_or_callable
    config = EventMeter::Configuration.new
    handler = ->(_error) {}

    assert_respond_to config.auto_cleanup_error_handler, :call

    config.auto_cleanup_error_handler = handler
    assert_same handler, config.auto_cleanup_error_handler

    config.auto_cleanup_error_handler = nil
    assert_nil config.auto_cleanup_error_handler

    assert_raises(ArgumentError) { config.auto_cleanup_error_handler = "warn" }
  end

  def test_redis_read_limit_can_be_nil_or_positive
    config = EventMeter::Configuration.new

    config.redis_read_limit = nil
    assert_nil config.redis_read_limit

    config.redis_read_limit = "2"
    assert_equal 2, config.redis_read_limit

    assert_raises(ArgumentError) { config.redis_read_limit = 0 }
    assert_raises(ArgumentError) { config.redis_read_limit = Float::NAN }
  end

  def test_default_storage_tracks_namespace_and_redis_changes
    first_redis = Object.new
    second_redis = Object.new
    config = EventMeter::Configuration.new

    config.redis = first_redis
    config.namespace = "billing_app:event_meter:v1"

    first_stream_storage = config.stream_storage
    first_rollup_storage = config.rollup_storage

    assert_same first_redis, first_stream_storage.redis
    assert_same first_redis, first_stream_storage.lock_redis
    assert_equal "billing_app:event_meter:v1", first_stream_storage.namespace
    assert_same first_redis, first_rollup_storage.redis
    assert_same first_redis, first_rollup_storage.lock_redis
    assert_equal "billing_app:event_meter:v1", first_rollup_storage.namespace

    config.redis = second_redis
    config.namespace = "billing_app:event_meter:v2"

    refute_same first_stream_storage, config.stream_storage
    refute_same first_rollup_storage, config.rollup_storage
    assert_same second_redis, config.stream_storage.redis
    assert_same second_redis, config.stream_storage.lock_redis
    assert_equal "billing_app:event_meter:v2", config.stream_storage.namespace
    assert_same second_redis, config.rollup_storage.redis
    assert_same second_redis, config.rollup_storage.lock_redis
    assert_equal "billing_app:event_meter:v2", config.rollup_storage.namespace

    second_stream_storage = config.stream_storage
    config.redis_read_limit = 10

    refute_same second_stream_storage, config.stream_storage
    assert_equal 10, config.stream_storage.redis_read_limit
  end

  def test_default_redis_factory_uses_dedicated_clients_for_lock_refreshing
    clients = []
    config = EventMeter::Configuration.new
    config.redis = -> { Object.new.tap { |client| clients << client } }

    stream_storage = config.stream_storage
    rollup_storage = config.rollup_storage

    assert_equal 4, clients.length
    assert_same clients[0], stream_storage.redis
    assert_same clients[1], stream_storage.lock_redis
    assert_same clients[2], rollup_storage.redis
    assert_same clients[3], rollup_storage.lock_redis
    refute_same stream_storage.redis, stream_storage.lock_redis
    refute_same rollup_storage.redis, rollup_storage.lock_redis
    refute_same stream_storage.lock_redis, rollup_storage.lock_redis
  end

  def test_default_redis_storage_refreshes_factory_clients_after_process_changes_and_closes
    created_clients = []
    config = EventMeter::Configuration.new
    config.redis = -> do
      redis_client_double.tap { |client| created_clients << client }
    end
    first_stream_storage = config.stream_storage
    first_rollup_storage = config.rollup_storage
    first_stream_client = first_stream_storage.redis
    first_rollup_client = first_rollup_storage.redis

    mark_redis_clients_as_from_another_process(config)
    second_stream_storage = config.stream_storage
    second_rollup_storage = config.rollup_storage
    second_stream_client = second_stream_storage.redis
    second_rollup_client = second_rollup_storage.redis

    refute_same first_stream_storage, second_stream_storage
    refute_same first_rollup_storage, second_rollup_storage
    refute_same first_stream_client, second_stream_client
    refute_same first_rollup_client, second_rollup_client

    second_stream_client.close!
    third_stream_storage = config.stream_storage

    refute_same second_stream_storage, third_stream_storage
    refute_same second_stream_client, third_stream_storage.redis

    assert_operator created_clients.length, :>=, 8
  end

  def test_default_redis_storage_cache_is_thread_safe_during_resets
    config = EventMeter::Configuration.new
    config.redis = -> { redis_client_double }
    errors = Queue.new

    threads = [
      Thread.new do
        100.times do
          config.stream_storage.redis
          config.rollup_storage.redis
        end
      rescue StandardError => error
        errors << error
      end,
      Thread.new do
        100.times do
          config.redis = -> { redis_client_double }
          config.stream_storage.lock_redis
          config.rollup_storage.lock_redis
        end
      rescue StandardError => error
        errors << error
      end,
      Thread.new do
        100.times do
          mark_redis_clients_as_from_another_process(config)
          config.stream_storage.redis
        end
      rescue StandardError => error
        errors << error
      end
    ]
    threads.each(&:join)

    assert_empty queue_values(errors).map { |error| "#{error.class}: #{error.message}" }
  end

  def test_default_redis_rollup_storage_tracks_rollup_ttl_changes
    config = EventMeter::Configuration.new
    config.redis = Object.new
    config.rollup_ttl = 12

    first_rollup_storage = config.rollup_storage

    assert_equal 12, first_rollup_storage.rollup_ttl

    config.rollup_ttl = 30

    refute_same first_rollup_storage, config.rollup_storage
    assert_equal 30, config.rollup_storage.rollup_ttl
  end

  def test_storage_must_be_configured_explicitly_without_redis
    config = EventMeter::Configuration.new

    stream_error = assert_raises(EventMeter::ConfigurationError) do
      config.stream_storage
    end
    rollup_error = assert_raises(EventMeter::ConfigurationError) do
      config.rollup_storage
    end

    assert_equal "configure stream_storage, or set config.redis to use Redis storage", stream_error.message
    assert_equal "configure rollup_storage, or set config.redis to use Redis storage", rollup_error.message
  end

  def test_redis_factory_must_return_a_client
    config = EventMeter::Configuration.new
    config.redis = -> {}

    error = assert_raises(EventMeter::ConfigurationError) do
      config.stream_storage
    end

    assert_equal "redis client cannot be nil", error.message
  end

  def test_redis_factory_must_return_an_open_client
    config = EventMeter::Configuration.new
    config.redis = -> { redis_client_double.tap(&:close!) }

    error = assert_raises(EventMeter::ConfigurationError) do
      config.stream_storage
    end

    assert_equal "redis client cannot be closed", error.message
  end

  def test_custom_storage_is_not_replaced_when_namespace_or_redis_changes
    config = EventMeter::Configuration.new
    stream_storage = memory_stream_storage
    rollup_storage = memory_rollup_storage(namespace: "custom")

    config.stream_storage = stream_storage
    config.rollup_storage = rollup_storage
    config.redis = Object.new
    config.namespace = "billing_app:event_meter:v2"

    assert_same stream_storage, config.stream_storage
    assert_same rollup_storage, config.rollup_storage
  end

  def test_custom_storage_cannot_be_nil
    config = EventMeter::Configuration.new

    assert_raises(ArgumentError) { config.stream_storage = nil }
    assert_raises(ArgumentError) { config.rollup_storage = nil }
  end

  def test_indexes_are_order_independent_and_not_duplicated
    config = EventMeter::ReportDefinition.new(name: "invoice_delivery", version: 1)

    config.index_by(:queue, :provider)
    config.index_by(:provider, :queue)
    config.measure_interval_by(:customer_id, group_by: [:queue, :provider])
    config.measure_interval_by(:customer_id, group_by: [:provider, :queue])

    assert_equal [[], [:provider, :queue]], config.indexes.map(&:params)
    assert_equal 1, config.intervals.length
  end

  def test_indexes_accept_strings_symbols_and_arrays
    config = EventMeter::ReportDefinition.new(name: "invoice_delivery", version: 1)

    config.index_by("q")
    config.index_by(:provider, "queue")
    config.measure_interval_by("customer_id", group_by: "q")

    assert_equal [[], [:q], [:provider, :queue]], config.indexes.map(&:params)
    assert_equal [:q], config.intervals.first.group_by
  end

  def test_index_params_must_be_strings_or_symbols
    config = EventMeter::ReportDefinition.new(name: "invoice_delivery", version: 1)

    assert_raises(ArgumentError) { config.index_by(nil) }
    assert_raises(ArgumentError) { config.index_by("") }
    assert_raises(ArgumentError) { config.measure_interval_by(nil) }
  end

  def test_event_name_cannot_be_blank
    assert_raises(ArgumentError) { EventMeter::ReportDefinition.new(name: "", version: 1) }

    event = EventMeter.start("")
    result = event.success

    assert event.error?
    assert_instance_of ArgumentError, event.error
    assert_equal "event name cannot be blank", event.error.message
    assert result.error?
    assert_same event.error, result.error
  end

  def test_unsupported_by_requires_an_index
    configure_delivery_event
    process_delivery_pending

    error = assert_raises(EventMeter::UnsupportedQueryError) do
      EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 5),
        by: { delivery_mode: "background_worker" }
      )
    end

    assert_includes error.message, "no index configured"
  end

  private

  def redis_client_double
    Object.new.tap do |client|
      client.instance_variable_set(:@closed, false)

      def client.closed?
        @closed
      end

      def client.close!
        @closed = true
      end
    end
  end

  def mark_redis_clients_as_from_another_process(config)
    config.instance_variable_set(:@redis_client_pid, -1)
  end

  def queue_values(queue)
    values = []
    values << queue.pop until queue.empty?
    values
  end
end

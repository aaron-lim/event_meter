require "test_helper"
require "json"
require "securerandom"

class EventMeterPerformanceTest < EventMeterTest
  def self.performance_integer(name, default)
    value = Integer(ENV.fetch(name, default.to_s))
    return value if value.positive?

    raise ArgumentError
  rescue ArgumentError, TypeError, RangeError
    raise ArgumentError, "#{name} must be a positive integer"
  end

  BATCH_EVENT_COUNT = performance_integer("EVENT_METER_PERFORMANCE_BATCH_EVENTS", 4000)
  FILE_HISTORY_MINUTES = performance_integer("EVENT_METER_PERFORMANCE_FILE_HISTORY_MINUTES", 720)
  REDIS_SCAN_KEY_COUNT = performance_integer("EVENT_METER_PERFORMANCE_REDIS_SCAN_KEYS", 4000)

  def test_large_batch_processing_stays_within_budget
    base = utc(2026, 5, 6, 1, 0)

    BATCH_EVENT_COUNT.times do |index|
      record_delivery(
        delivery_attributes(index),
        status: status_for(index),
        started_at: base + index,
        duration_ms: 10 + (index % 40)
      )
    end

    result = assert_performance_budget(
      "large_batch_processing",
      seconds: performance_seconds("EVENT_METER_PERFORMANCE_BATCH_SECONDS", 4.0)
    ) do
      process_delivery_pending
    end

    summary = EventMeter.summary(
      DELIVERY_EVENT,
      version: DELIVERY_VERSION,
      from: base,
      to: base + BATCH_EVENT_COUNT + 60
    )

    assert_equal expected_delivery_result(processed: BATCH_EVENT_COUNT), result.to_h
    assert_equal BATCH_EVENT_COUNT, summary.fetch(:count)
  end

  def test_file_rollup_cleanup_stays_within_budget_for_large_history
    namespace = "event_meter:test:performance:file:#{Process.pid}:#{SecureRandom.hex(4)}"
    stream_storage = nil

    Dir.mktmpdir("event-meter-performance-file") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: File.join(root, "stream"),
        sync: :flush
      )

      EventMeter.configure do |config|
        config.namespace = namespace
        config.stream_storage = stream_storage
        config.rollup_storage = EventMeter::Stores::Rollup::File.new(path: File.join(root, "rollup"))
      end

      base = utc(2026, 5, 6, 1, 0)
      Time.stub(:now, Time.now.utc - 120) do
        FILE_HISTORY_MINUTES.times do |index|
          record_delivery(
            delivery_attributes(index, unique_customer: true),
            started_at: base + (index * 60),
            duration_ms: 20 + (index % 30)
          )
        end
      end

      assert_equal expected_delivery_result(processed: FILE_HISTORY_MINUTES), process_delivery_pending.to_h

      result = assert_performance_budget(
        "file_rollup_cleanup",
        seconds: performance_seconds("EVENT_METER_PERFORMANCE_FILE_CLEANUP_SECONDS", 6.0)
      ) do
        EventMeter.cleanup_history(before: base + ((FILE_HISTORY_MINUTES / 2) * 60))
      end

      assert_operator result.fetch(:rollup_keys_deleted), :>, 0
      assert_operator result.fetch(:interval_state_keys_deleted), :>, 0
    end
  ensure
    stream_storage&.close
  end

  def test_redis_rollup_cleanup_scan_stays_within_budget_and_batches_deletes
    namespace = "event_meter:test:performance:redis:#{Process.pid}:#{SecureRandom.hex(4)}"
    before = utc(2026, 5, 6, 12, 0)
    redis = LargeScanRedis.new(
      keys: redis_scan_keys(namespace, before, REDIS_SCAN_KEY_COUNT),
      values: redis_scan_values(namespace, before, REDIS_SCAN_KEY_COUNT)
    )
    storage = EventMeter::Stores::Rollup::Redis.new(redis: redis, namespace: namespace)

    result = assert_performance_budget(
      "redis_rollup_cleanup_scan",
      seconds: performance_seconds("EVENT_METER_PERFORMANCE_REDIS_SCAN_SECONDS", 3.0)
    ) do
      storage.cleanup_history(
        before: before,
        events: [DELIVERY_EVENT],
        interval_state: true
      )
    end

    assert_equal REDIS_SCAN_KEY_COUNT, result.fetch(:rollup_keys_deleted)
    assert_equal REDIS_SCAN_KEY_COUNT, result.fetch(:interval_state_keys_deleted)
    assert_equal REDIS_SCAN_KEY_COUNT, result.fetch(:processed_entries_deleted)
    assert_equal ["#{namespace}:rollup:*", "#{namespace}:state:*", "#{namespace}:processed:*"], redis.scan_matches
    assert redis.deleted_batches.all? { |batch| batch.length <= 500 }
  end

  private

  def assert_performance_budget(label, seconds:)
    GC.start
    started_at = monotonic_time
    result = yield
    elapsed = monotonic_time - started_at

    emit_performance_sample(label, elapsed: elapsed, budget: seconds)
    assert_operator elapsed, :<=, seconds, "#{label} took #{elapsed.round(3)}s; budget is #{seconds}s"

    result
  end

  def performance_seconds(name, default)
    value = Float(ENV.fetch(name, default.to_s))
    return value if value.positive?

    raise ArgumentError
  rescue ArgumentError, TypeError, RangeError
    raise ArgumentError, "#{name} must be a positive number"
  end

  def emit_performance_sample(label, elapsed:, budget:)
    return unless ENV["EVENT_METER_PERFORMANCE_REPORT"] == "1"

    puts JSON.generate(
      event: "performance_sample",
      label: label,
      elapsed_seconds: elapsed.round(6),
      budget_seconds: budget
    )
  end

  def monotonic_time
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def delivery_attributes(index, unique_customer: false)
    {
      customer_id: unique_customer ? index : index % 250,
      provider: index.even? ? "postmark" : "mailgun",
      delivery_mode: index.even? ? "email" : "sms",
      queue: index.even? ? "fast" : "bulk"
    }
  end

  def status_for(index)
    return "failure" if (index % 17).zero?
    return "skipped" if (index % 19).zero?

    "success"
  end

  def redis_scan_keys(namespace, before, count)
    keys = []

    count.times do |index|
      keys << redis_rollup_key(namespace, before - ((index + 1) * 60), index)
      keys << redis_rollup_key(namespace, before + ((index + 1) * 60), index)
      keys << redis_state_key(namespace, index)
      keys << redis_state_key(namespace, index + count)
      keys << redis_processed_key(namespace, index)
      keys << redis_processed_key(namespace, index + count)
      keys << "other:#{namespace}:rollup:invoice_delivery:v1:minute:202605061100:provider=postmark:#{index}"
    end

    keys
  end

  def redis_scan_values(namespace, before, count)
    before_ms = (before.to_f * 1000).to_i

    count.times.with_object({}) do |index, values|
      values[redis_state_key(namespace, index)] = (before_ms - 1).to_s
      values[redis_state_key(namespace, index + count)] = (before_ms + 1).to_s
      values[redis_processed_key(namespace, index)] = (before - 60).iso8601(6)
      values[redis_processed_key(namespace, index + count)] = (before + 60).iso8601(6)
    end
  end

  def redis_rollup_key(namespace, bucket_time, index)
    [
      namespace,
      "rollup",
      DELIVERY_EVENT,
      "v#{DELIVERY_VERSION}",
      "minute",
      EventMeter::TimeBuckets.id(bucket_time, :minute),
      "provider=#{index.even? ? "postmark" : "mailgun"}"
    ].join(":")
  end

  def redis_state_key(namespace, index)
    [
      namespace,
      "state",
      DELIVERY_EVENT,
      "v#{DELIVERY_VERSION}",
      "interval",
      "customer_id",
      index
    ].join(":")
  end

  def redis_processed_key(namespace, index)
    [
      namespace,
      "processed",
      DELIVERY_EVENT,
      "v#{DELIVERY_VERSION}",
      index
    ].join(":")
  end

  class LargeScanRedis
    attr_reader :deleted_batches, :scan_matches

    def initialize(keys:, values:)
      @keys = keys
      @values = values
      @deleted_batches = []
      @scan_matches = []
    end

    def scan_each(match:)
      scan_matches << match

      @keys.each do |key|
        yield key if File.fnmatch?(match, key)
      end
    end

    def get(key)
      @values[key]
    end

    def pipelined
      pipeline = Pipeline.new(@values)
      yield pipeline
      pipeline.results
    end

    def del(*keys)
      deleted_batches << keys
      keys.each { |key| @values.delete(key) }
      keys.length
    end

    class Pipeline
      attr_reader :results

      def initialize(values)
        @values = values
        @results = []
      end

      def get(key)
        results << @values[key]
      end
    end
  end
end

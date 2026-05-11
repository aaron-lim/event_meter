require "test_helper"
require "fileutils"
require "json"
require "securerandom"

class EventMeterSoakTest < EventMeterTest
  EVENT_NAME = "invoice_delivery"
  VERSION = 1
  PROVIDERS = %w[postmark mailgun].freeze
  DELIVERY_MODES = %w[email sms].freeze
  QUEUES = %w[fast normal bulk].freeze
  START_STEP_SECONDS = 1

  def test_soak_for_configured_duration
    runner = SoakRunner.new(test: self, config: SoakConfig.from_env)
    runner.run
  end

  class SoakConfig
    attr_reader :batch_size,
      :cleanup_seconds,
      :customer_count,
      :delete_fail_every,
      :duration_seconds,
      :report_seconds,
      :rollup_kind,
      :sleep_seconds,
      :stream_kind

    def self.from_env
      new(
        batch_size: positive_integer("EVENT_METER_SOAK_BATCH_SIZE", 100),
        cleanup_seconds: non_negative_float("EVENT_METER_SOAK_CLEANUP_SECONDS", 0),
        customer_count: positive_integer("EVENT_METER_SOAK_CUSTOMERS", 100),
        delete_fail_every: non_negative_integer("EVENT_METER_SOAK_DELETE_FAIL_EVERY", 0),
        duration_seconds: positive_float("EVENT_METER_SOAK_SECONDS", 10),
        report_seconds: positive_float("EVENT_METER_SOAK_REPORT_SECONDS", 5),
        rollup_kind: storage_kind("EVENT_METER_SOAK_ROLLUP", default_rollup_kind, %w[file redis postgres]),
        sleep_seconds: non_negative_float("EVENT_METER_SOAK_SLEEP_SECONDS", 0.05),
        stream_kind: storage_kind("EVENT_METER_SOAK_STREAM", "file", %w[file redis])
      )
    end

    def initialize(
      batch_size:,
      cleanup_seconds:,
      customer_count:,
      delete_fail_every:,
      duration_seconds:,
      report_seconds:,
      rollup_kind:,
      sleep_seconds:,
      stream_kind:
    )
      @batch_size = batch_size
      @cleanup_seconds = cleanup_seconds
      @customer_count = customer_count
      @delete_fail_every = delete_fail_every
      @duration_seconds = duration_seconds
      @report_seconds = report_seconds
      @rollup_kind = rollup_kind
      @sleep_seconds = sleep_seconds
      @stream_kind = stream_kind
    end

    def to_h
      {
        batch_size: batch_size,
        cleanup_seconds: cleanup_seconds,
        customer_count: customer_count,
        delete_fail_every: delete_fail_every,
        duration_seconds: duration_seconds,
        report_seconds: report_seconds,
        rollup: rollup_kind,
        sleep_seconds: sleep_seconds,
        stream: stream_kind
      }
    end

    def self.default_rollup_kind
      postgres_url ? "postgres" : "file"
    end

    def self.storage_kind(name, default, allowed)
      value = ENV.fetch(name, default).to_s
      return value if allowed.include?(value)

      raise ArgumentError, "#{name} must be one of: #{allowed.join(", ")}"
    end

    def self.positive_integer(name, default)
      value = Integer(ENV.fetch(name, default.to_s))
      return value if value.positive?

      raise ArgumentError
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "#{name} must be a positive integer"
    end

    def self.non_negative_integer(name, default)
      value = Integer(ENV.fetch(name, default.to_s))
      return value if value >= 0

      raise ArgumentError
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "#{name} must be a non-negative integer"
    end

    def self.positive_float(name, default)
      value = Float(ENV.fetch(name, default.to_s))
      return value if value.positive?

      raise ArgumentError
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "#{name} must be a positive number"
    end

    def self.non_negative_float(name, default)
      value = Float(ENV.fetch(name, default.to_s))
      return value if value >= 0

      raise ArgumentError
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "#{name} must be a non-negative number"
    end

    def self.postgres_url
      ENV["EVENT_METER_POSTGRES_URL"] || ENV["DATABASE_URL"]
    end
  end

  class SoakRunner
    attr_reader :config, :test

    def initialize(test:, config:)
      @config = config
      @test = test
      @base_time = Time.now.utc
      @cleanup_runs = []
      @expected = ExpectedMetrics.new
      @namespace = "event_meter:test:soak:#{Process.pid}:#{SecureRandom.hex(4)}"
      @processed = 0
      @skipped = 0
      @malformed = 0
      @samples = []
      @started_at = nil
      @storage = nil
      @written = 0
    end

    def run
      with_storage do
        configure_event_meter
        @started_at = monotonic_time
        before = sample("before")
        emit("soak_start", before)

        soak_loop
        drain_stream

        after = sample("after")
        report = final_report(before, after)
        verify_report!(report)
        emit("soak_finish", report)
      end
    end

    private

    def soak_loop
      deadline = monotonic_time + config.duration_seconds
      next_report_at = monotonic_time + config.report_seconds
      next_cleanup_at = cleanup_deadline

      while monotonic_time < deadline || @written.zero?
        append_batch
        process_pending

        if config.cleanup_seconds.positive? && monotonic_time >= next_cleanup_at
          @cleanup_runs << EventMeter.cleanup_history(before: @base_time - 1)
          next_cleanup_at = monotonic_time + config.cleanup_seconds
        end

        if monotonic_time >= next_report_at
          emit("soak_sample", sample("sample"))
          next_report_at = monotonic_time + config.report_seconds
        end

        sleep config.sleep_seconds if config.sleep_seconds.positive?
      end
    end

    def cleanup_deadline
      return Float::INFINITY unless config.cleanup_seconds.positive?

      monotonic_time + config.cleanup_seconds
    end

    def append_batch
      config.batch_size.times do
        event = build_event(@written)
        @storage.writer_stream.append(EventMeter::EventPayload.build(
          EVENT_NAME,
          params: event.fetch(:params),
          status: event.fetch(:status),
          started_at: event.fetch(:started_at),
          duration_ms: event.fetch(:duration_ms)
        ))
        @expected.add(event)
        @written += 1
      end
    end

    def process_pending
      result = EventMeter.process_pending(EVENT_NAME, version: VERSION) do |report|
        report.index_by(:customer_id)
        report.index_by(:provider)
        report.index_by(:provider, :delivery_mode, :queue)
        report.measure_interval_by(:customer_id, group_by: :provider)
        report.measure_interval_by(:customer_id, group_by: [:provider, :delivery_mode, :queue])
      end

      @processed += result.processed
      @skipped += result.skipped_already_processed
      @malformed += result.malformed
      result
    end

    def drain_stream
      10.times do
        result = process_pending
        return if result.processed.zero? && result.skipped_already_processed.zero? && result.complete
      end
    end

    def build_event(index)
      customer_slot = index % config.customer_count
      provider = PROVIDERS.fetch(customer_slot % PROVIDERS.length)

      {
        params: {
          customer_id: 10_000 + customer_slot,
          provider: provider,
          delivery_mode: DELIVERY_MODES.fetch(customer_slot % DELIVERY_MODES.length),
          queue: QUEUES.fetch(customer_slot % QUEUES.length)
        },
        status: status_for(index),
        started_at: @base_time + index * START_STEP_SECONDS,
        duration_ms: 10 + (index % 50)
      }
    end

    def status_for(index)
      return "failure" if (index % 17).zero?
      return "skipped" if (index % 19).zero?

      "success"
    end

    def configure_event_meter
      EventMeter.reset
      EventMeter.configure do |event_meter_config|
        event_meter_config.namespace = @namespace
        event_meter_config.stream_storage = processor_stream
        event_meter_config.rollup_storage = @storage.rollup
        event_meter_config.lock_ttl = 1
      end
    end

    def processor_stream
      stream = @storage.processor_stream
      return stream unless config.delete_fail_every.positive?

      DeleteFailingStream.new(stream, every: config.delete_fail_every)
    end

    def sample(label)
      summary = @written.positive? ? safe_summary({}) : nil
      sample = {
        label: label,
        elapsed_seconds: elapsed_seconds,
        written: @written,
        processed: @processed,
        skipped_already_processed: @skipped,
        malformed: @malformed,
        summary_count: summary && summary.fetch(:count),
        missing_count: summary ? @written - summary.fetch(:count) : nil,
        resources: resource_snapshot
      }
      @samples << sample
      sample
    end

    def final_report(before, after)
      {
        config: config.to_h,
        namespace: @namespace,
        elapsed_seconds: elapsed_seconds,
        written: @written,
        processed: @processed,
        skipped_already_processed: @skipped,
        malformed: @malformed,
        cleanup_runs: @cleanup_runs,
        before: before.fetch(:resources),
        after: after.fetch(:resources),
        delta: diff_hash(after.fetch(:resources), before.fetch(:resources)),
        summaries: final_summaries,
        samples: @samples
      }
    end

    def final_summaries
      {
        all: safe_summary({}),
        providers: PROVIDERS.to_h { |provider| [provider, safe_summary(provider: provider)] }
      }
    end

    def verify_report!(report)
      test.assert_equal @written, report.dig(:summaries, :all, :count), "all count"
      test.assert_equal @expected.all.fetch(:duration_ms_sum), report.dig(:summaries, :all, :duration_ms_sum), "all duration"
      test.assert_equal 0, report.dig(:summaries, :all, :interval_ms_count), "all interval count"

      PROVIDERS.each do |provider|
        expected = @expected.providers.fetch(provider)
        summary = report.dig(:summaries, :providers, provider)

        test.assert_equal expected.fetch(:count), summary.fetch(:count), "#{provider} count"
        test.assert_equal expected.fetch(:duration_ms_sum), summary.fetch(:duration_ms_sum), "#{provider} duration"
        test.assert_equal expected.fetch(:interval_ms_count), summary.fetch(:interval_ms_count), "#{provider} interval count"
        test.assert_equal expected.fetch(:interval_ms_sum), summary.fetch(:interval_ms_sum), "#{provider} interval sum"
      end

      test.assert_equal 0, @malformed, "malformed events"
      test.assert_equal @written, @processed, "processed events"
      test.assert_stream_empty(@storage.processor_stream)
      test.assert_processed_ids_cleaned(@storage)
    end

    def safe_summary(by)
      EventMeter.summary(
        EVENT_NAME,
        version: VERSION,
        from: @base_time,
        to: @base_time + @written * START_STEP_SECONDS + 60,
        by: by
      )
    rescue EventMeter::DefinitionNotFoundError
      nil
    end

    def with_storage
      Dir.mktmpdir("event-meter-soak") do |root|
        @storage = StorageSet.build(config: config, namespace: @namespace, root: root, test: test)
        yield
      ensure
        @storage&.close
      end
    end

    def resource_snapshot
      snapshot = {
        process: {
          rss_kb: rss_kb,
          open_fds: open_fd_count,
          heap_live_slots: GC.stat.fetch(:heap_live_slots),
          heap_free_slots: GC.stat.fetch(:heap_free_slots),
          total_allocated_objects: GC.stat.fetch(:total_allocated_objects)
        }
      }

      snapshot[:files] = @storage.file_stats
      snapshot[:redis] = @storage.redis_stats if @storage.redis?
      snapshot[:postgres] = @storage.postgres_stats if @storage.postgres?
      snapshot
    end

    def emit(event, payload)
      $stdout.puts(JSON.generate({ event: event }.merge(payload)))
      $stdout.flush
    end

    def elapsed_seconds
      return 0 unless @started_at

      (monotonic_time - @started_at).round(3)
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def rss_kb
      Integer(`ps -o rss= -p #{Process.pid}`.strip)
    rescue ArgumentError, TypeError
      nil
    end

    def open_fd_count
      if Dir.exist?("/proc/self/fd")
        Dir.children("/proc/self/fd").length
      elsif Dir.exist?("/dev/fd")
        Dir.children("/dev/fd").length
      end
    rescue SystemCallError
      nil
    end

    def diff_hash(after, before)
      after.each_with_object({}) do |(key, value), diff|
        before_value = before[key]

        diff[key] =
          if value.is_a?(Hash) && before_value.is_a?(Hash)
            diff_hash(value, before_value)
          elsif value.is_a?(Numeric) && before_value.is_a?(Numeric)
            value - before_value
          end
      end.compact
    end
  end

  class ExpectedMetrics
    attr_reader :all, :providers

    def initialize
      @all = new_metrics
      @providers = PROVIDERS.to_h { |provider| [provider, new_metrics] }
      @previous_started_ms = {}
    end

    def add(event)
      params = event.fetch(:params)
      customer_id = params.fetch(:customer_id)
      provider = params.fetch(:provider)
      started_ms = (event.fetch(:started_at).to_f * 1000).to_i
      interval_ms = previous_interval(customer_id, started_ms)

      add_event_counts(all, event)
      add_event_counts(providers.fetch(provider), event)
      add_interval_metrics(providers.fetch(provider), interval_ms)
    end

    private

    def new_metrics
      {
        count: 0,
        success_count: 0,
        failure_count: 0,
        skipped_count: 0,
        duration_ms_sum: 0,
        interval_ms_count: 0,
        interval_ms_sum: 0
      }
    end

    def add_event_counts(metrics, event)
      metrics[:count] += 1
      metrics[:"#{event.fetch(:status)}_count"] += 1
      metrics[:duration_ms_sum] += event.fetch(:duration_ms)
    end

    def add_interval_metrics(metrics, interval_ms)
      return unless interval_ms

      metrics[:interval_ms_count] += 1
      metrics[:interval_ms_sum] += interval_ms
    end

    def previous_interval(customer_id, started_ms)
      previous_ms = @previous_started_ms[customer_id]
      @previous_started_ms[customer_id] = started_ms
      previous_ms && started_ms - previous_ms
    end
  end

  class DeleteFailingStream
    def initialize(stream, every:)
      @stream = stream
      @every = every
      @delete_calls = 0
    end

    def read(name:)
      @stream.read(name: name)
    end

    def delete
      @delete_calls += 1
      return false if (@delete_calls % @every).zero?

      @stream.delete
    end

    def release
      @stream.release if @stream.respond_to?(:release)
    end

    def close
      @stream.close if @stream.respond_to?(:close)
    end

    def method_missing(name, *args, &block)
      return @stream.public_send(name, *args, &block) if @stream.respond_to?(name)

      super
    end

    def respond_to_missing?(name, include_private = false)
      @stream.respond_to?(name, include_private) || super
    end
  end

  class StorageSet
    attr_reader :namespace, :processor_stream, :rollup, :writer_stream

    def self.build(config:, namespace:, root:, test:)
      new(config: config, namespace: namespace, root: root, test: test).tap(&:build)
    end

    def initialize(config:, namespace:, root:, test:)
      @config = config
      @namespace = namespace
      @root = root
      @test = test
      @connections = []
    end

    def build
      @processor_stream, @writer_stream = build_streams
      @rollup = build_rollup
    end

    def close
      [processor_stream, writer_stream, rollup, *@connections].uniq.each do |object|
        object.close if object.respond_to?(:close)
      rescue StandardError
        nil
      end

      cleanup_redis if redis?
      drop_postgres_tables if postgres?
    end

    def redis?
      @config.stream_kind == "redis" || @config.rollup_kind == "redis"
    end

    def redis_rollup?
      @config.rollup_kind == "redis"
    end

    def postgres?
      @config.rollup_kind == "postgres"
    end

    def file_rollup?
      @config.rollup_kind == "file"
    end

    def file_stats
      {
        root: file_tree_stats(@root),
        stream: file_tree_stats(@stream_path),
        rollup: file_tree_stats(@rollup_path)
      }
    end

    def redis_stats
      redis = redis_connection(track: false)
      keys = redis_keys(redis, "#{namespace}:*")

      {
        keys: keys.length,
        stream_length: redis_stream_length(redis),
        used_memory_bytes: redis.info("memory").fetch("used_memory").to_i
      }
    rescue StandardError => error
      { error: "#{error.class}: #{error.message}" }
    ensure
      close_connection(redis)
    end

    def postgres_stats
      connection = postgres_connection(track: false)

      {
        rollups: postgres_count(connection, "#{@table_prefix}_rollups"),
        strings: postgres_count(connection, "#{@table_prefix}_strings"),
        processed_entries: postgres_count(connection, "#{@table_prefix}_processed_entries"),
        total_bytes: postgres_total_bytes(connection)
      }
    rescue StandardError => error
      { error: "#{error.class}: #{error.message}" }
    ensure
      close_connection(connection)
    end

    private

    def build_streams
      case @config.stream_kind
      when "file"
        @stream_path = File.join(@root, "stream")
        processor = EventMeter::Stores::Stream::File.new(path: @stream_path, sync: :flush)
        writer = EventMeter::Stores::Stream::File.new(
          path: @stream_path,
          sync: :flush,
          clock: proc { Time.now.utc - 120 }
        )
        [processor, writer]
      when "redis"
        [
          EventMeter::Stores::Stream::Redis.new(redis: redis_connection, namespace: namespace),
          EventMeter::Stores::Stream::Redis.new(redis: redis_connection, namespace: namespace)
        ]
      else
        raise ArgumentError, "unknown stream storage: #{@config.stream_kind}"
      end
    end

    def build_rollup
      case @config.rollup_kind
      when "file"
        @rollup_path = File.join(@root, "rollup")
        EventMeter::Stores::Rollup::File.new(path: @rollup_path)
      when "redis"
        EventMeter::Stores::Rollup::Redis.new(redis: redis_connection, namespace: namespace)
      when "postgres"
        @table_prefix = "em_soak_#{Process.pid}_#{SecureRandom.hex(4)}"
        schema_connection = nil
        begin
          schema_connection = postgres_connection(track: false)
          EventMeter::Stores::Rollup::Postgres.install!(
            connection: schema_connection,
            table_prefix: @table_prefix
          )
        ensure
          close_connection(schema_connection)
        end

        EventMeter::Stores::Rollup::Postgres.new(
          connection: postgres_connection,
          namespace: namespace,
          table_prefix: @table_prefix
        )
      else
        raise ArgumentError, "unknown rollup storage: #{@config.rollup_kind}"
      end
    end

    def redis_connection(track: true)
      require "redis"

      url = ENV["EVENT_METER_REDIS_URL"].to_s.strip
      connection = url.empty? ? Redis.new : Redis.new(url: url)
      connection.tap(&:ping).tap { |redis| @connections << redis if track }
    rescue LoadError, Redis::BaseConnectionError, Errno::ECONNREFUSED => error
      @test.skip "Redis is unavailable: #{error.class}: #{error.message}"
    end

    def postgres_connection(track: true)
      EventMeterTestSupport::PostgresDatabase.connect(test: @test, url: postgres_url)
        .tap { |connection| @connections << connection if track }
    end

    def postgres_url
      @postgres_url ||= EventMeterTestSupport::PostgresDatabase.url(test: @test)
    end

    def cleanup_redis
      redis = redis_connection(track: false)
      keys = redis_keys(redis, "#{namespace}:*")
      keys.each_slice(500) { |slice| redis.del(*slice) unless slice.empty? }
    ensure
      close_connection(redis)
    end

    def drop_postgres_tables
      connection = postgres_connection(track: false)
      connection.exec(<<~SQL)
        DROP TABLE IF EXISTS
          #{@table_prefix}_processed_entries,
          #{@table_prefix}_strings,
          #{@table_prefix}_rollups
      SQL
    ensure
      close_connection(connection)
    end

    def close_connection(connection)
      return unless connection

      if connection.respond_to?(:finished?)
        connection.close unless connection.finished?
      elsif connection.respond_to?(:close)
        connection.close
      end
    rescue StandardError
      nil
    end

    def postgres_count(connection, table)
      connection.exec("SELECT count(*) AS count FROM #{table}").first.fetch("count").to_i
    end

    def postgres_total_bytes(connection)
      %w[rollups strings processed_entries].sum do |suffix|
        table = "#{@table_prefix}_#{suffix}"
        connection.exec_params("SELECT pg_total_relation_size($1::regclass) AS bytes", [table])
          .first.fetch("bytes").to_i
      end
    end

    def redis_stream_length(redis)
      redis.xlen([namespace, "stream", EventMeter::Keys.event_name(EVENT_NAME)].join(":"))
    rescue StandardError
      nil
    end

    def redis_keys(redis, pattern)
      keys = []
      redis.scan_each(match: pattern) { |key| keys << key }
      keys
    end

    def file_tree_stats(path)
      return { files: 0, directories: 0, bytes: 0 } unless path && File.exist?(path)

      files = 0
      directories = 0
      bytes = 0

      Dir.glob(File.join(path, "**", "*"), File::FNM_DOTMATCH).each do |entry|
        basename = File.basename(entry)
        next if basename == "." || basename == ".."

        if File.directory?(entry)
          directories += 1
        elsif File.file?(entry)
          files += 1
          bytes += File.size(entry)
        end
      end

      { path: path, files: files, directories: directories, bytes: bytes }
    end
  end

  def assert_stream_empty(stream)
    entries = stream.read(name: EVENT_NAME)
    assert_empty entries, "stream still has unread rows"
  ensure
    stream.release if stream.respond_to?(:release)
  end

  def assert_processed_ids_cleaned(storage)
    if storage.postgres?
      assert_equal 0, storage.postgres_stats.fetch(:processed_entries), "postgres processed ids"
    elsif storage.redis_rollup?
      redis = storage.send(:redis_connection, track: false)
      keys = storage.send(:redis_keys, redis, "#{storage.namespace}:processed:*")
      assert_empty keys, "redis processed ids"
    elsif storage.file_rollup?
      sidecars = Dir[File.join(storage.file_stats.dig(:rollup, :path).to_s, "**", "*.processed.json")]
      assert_empty sidecars, "file processed sidecars"
    end
  ensure
    storage.send(:close_connection, redis) if redis
  end
end

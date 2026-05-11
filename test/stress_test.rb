require "test_helper"
require "json"
require "rbconfig"
require "securerandom"

class EventMeterStressTest < EventMeterTest
  BASE_TIME = Time.utc(2026, 5, 6, 10, 0, 0)
  CRASH_BEFORE_STREAM_DELETE_EXIT = 88
  CUSTOMER_COUNT = 12
  RETRY_PROCESS_COUNT = 4
  START_STEP_SECONDS = 5
  WRITER_PROCESS_COUNT = 3
  WRITER_THREAD_COUNT = 4

  PROVIDERS = %w[postmark mailgun].freeze
  DELIVERY_MODES = %w[email sms].freeze
  QUEUES = %w[fast normal bulk].freeze

  def test_file_stream_with_file_rollup_survives_chaotic_writers_and_crashed_processor
    with_file_stream_factories do |stream_factory, writer_stream_factory|
      with_file_rollup_factory do |rollup_factory|
        run_chaos_stress(
          "file stream + file rollup",
          stream_factory: stream_factory,
          writer_stream_factory: writer_stream_factory,
          rollup_factory: rollup_factory
        )
      end
    end
  end

  def test_file_stream_with_redis_rollup_survives_chaotic_writers_and_crashed_processor
    with_redis_namespace("file-redis") do |namespace|
      with_file_stream_factories do |stream_factory, writer_stream_factory|
        run_chaos_stress(
          "file stream + redis rollup",
          namespace: namespace,
          stream_factory: stream_factory,
          writer_stream_factory: writer_stream_factory,
          rollup_factory: redis_rollup_factory(namespace),
          wait_for_crashed_lock: true
        )
      end
    end
  end

  def test_file_stream_with_postgres_rollup_survives_chaotic_writers_and_crashed_processor
    with_postgres_namespace("file-postgres") do |namespace, rollup_factory|
      with_file_stream_factories do |stream_factory, writer_stream_factory|
        run_chaos_stress(
          "file stream + postgres rollup",
          namespace: namespace,
          stream_factory: stream_factory,
          writer_stream_factory: writer_stream_factory,
          rollup_factory: rollup_factory,
          wait_for_crashed_lock: true
        )
      end
    end
  end

  def test_redis_stream_with_file_rollup_survives_chaotic_writers_and_crashed_processor
    with_redis_namespace("redis-file") do |namespace|
      with_file_rollup_factory do |rollup_factory|
        run_chaos_stress(
          "redis stream + file rollup",
          namespace: namespace,
          stream_factory: redis_stream_factory(namespace),
          rollup_factory: rollup_factory
        )
      end
    end
  end

  def test_redis_stream_with_redis_rollup_survives_chaotic_writers_and_crashed_processor
    with_redis_namespace("redis-redis") do |namespace|
      run_chaos_stress(
        "redis stream + redis rollup",
        namespace: namespace,
        stream_factory: redis_stream_factory(namespace),
        rollup_factory: redis_rollup_factory(namespace),
        wait_for_crashed_lock: true
      )
    end
  end

  def test_redis_stream_with_postgres_rollup_survives_chaotic_writers_and_crashed_processor
    with_redis_namespace("redis-postgres") do |namespace|
      with_postgres_rollup_factory(namespace) do |rollup_factory|
        run_chaos_stress(
          "redis stream + postgres rollup",
          namespace: namespace,
          stream_factory: redis_stream_factory(namespace),
          rollup_factory: rollup_factory,
          wait_for_crashed_lock: true
        )
      end
    end
  end

  private

  def run_chaos_stress(
    label,
    stream_factory:,
    rollup_factory:,
    writer_stream_factory: stream_factory,
    namespace: stress_namespace(label),
    wait_for_crashed_lock: false
  )
    skip "Process.fork is unavailable on this Ruby platform" unless Process.respond_to?(:fork)

    events = stress_events
    expected = expected_metrics(events)
    stream = stream_factory.call
    rollup = rollup_factory.call

    configure_event_meter(
      namespace: namespace,
      stream: stream,
      rollup: rollup
    )

    append_chaotic_events(label, events, writer_stream_factory)
    assert_crashed_processor_reached_stream_delete(label, namespace, stream_factory, rollup_factory)
    sleep 1.1 if wait_for_crashed_lock
    process_retry_storm(label, namespace, stream_factory, rollup_factory)

    assert_equal expected_delivery_result(processed: 0), process_delivery_pending.to_h, label
    assert_stream_empty(label)
    assert_processed_ids_cleaned(label, rollup_factory)
    assert_stress_reports(label, expected)
  ensure
    close_storage(stream)
    close_storage(rollup)
  end

  def append_chaotic_events(label, events, stream_factory)
    slices = event_slices_for_writers(events)
    process_slices = slices.first(WRITER_PROCESS_COUNT)
    thread_slices = slices.drop(WRITER_PROCESS_COUNT)

    pids = process_slices.each_with_index.map do |slice, index|
      fork_child("#{label} writer process #{index}") do
        append_event_slice(slice, stream_factory, close_after: index != 0)
      end
    end

    errors = Queue.new
    threads = thread_slices.each_with_index.map do |slice, index|
      Thread.new do
        append_event_slice(slice, stream_factory, close_after: true)
      rescue StandardError => error
        errors << "#{label} writer thread #{index}: #{error.class}: #{error.message}"
      end
    end

    threads.each(&:join)
    flunk errors.pop unless errors.empty?
    assert_successful_children(pids, "#{label} writer processes")
  end

  def assert_crashed_processor_reached_stream_delete(label, namespace, stream_factory, rollup_factory)
    pid = spawn_processor_child(
      "#{label} crashed processor",
      namespace,
      stream_factory,
      rollup_factory,
      crash_before_stream_delete: true
    )
    assert_child_exit(pid, CRASH_BEFORE_STREAM_DELETE_EXIT, "#{label} crashed processor")
  end

  def process_retry_storm(label, namespace, stream_factory, rollup_factory)
    pids = Array.new(RETRY_PROCESS_COUNT) do |index|
      spawn_processor_child(
        "#{label} retry processor #{index}",
        namespace,
        stream_factory,
        rollup_factory,
        crash_before_stream_delete: false
      )
    end

    assert_successful_children(pids, "#{label} retry processors")
  end

  def append_event_slice(events, stream_factory, close_after:)
    stream = stream_factory.call

    events.each do |event|
      stream.append(EventMeter::EventPayload.build(
        DELIVERY_EVENT,
        params: event.fetch(:params),
        status: event.fetch(:status),
        started_at: event.fetch(:started_at),
        duration_ms: event.fetch(:duration_ms)
      ))
    end
  ensure
    close_storage(stream) if close_after
  end

  def assert_stress_reports(label, expected)
    assert_summary(label, {}, expected.fetch(:all), intervals: false)

    expected.fetch(:providers).each do |provider, metrics|
      assert_summary(label, { provider: provider }, metrics, intervals: true)
    end

    expected.fetch(:compounds).each do |(provider, delivery_mode, queue), metrics|
      assert_summary(
        label,
        {
          provider: provider,
          delivery_mode: delivery_mode,
          queue: queue
        },
        metrics,
        intervals: true
      )
    end

    assert_series_count(label, expected.fetch(:all).fetch(:count))
  end

  def assert_summary(label, by, expected, intervals:)
    summary = EventMeter.summary(
      DELIVERY_EVENT,
      version: DELIVERY_VERSION,
      from: BASE_TIME,
      to: report_to_time,
      by: by
    )

    assert_equal expected.fetch(:count), summary.fetch(:count), "#{label} #{by.inspect} count"
    assert_equal expected.fetch(:success_count), summary.fetch(:success_count), "#{label} #{by.inspect} success_count"
    assert_equal expected.fetch(:failure_count), summary.fetch(:failure_count), "#{label} #{by.inspect} failure_count"
    assert_equal expected.fetch(:skipped_count), summary.fetch(:skipped_count), "#{label} #{by.inspect} skipped_count"
    assert_equal expected.fetch(:duration_ms_sum), summary.fetch(:duration_ms_sum), "#{label} #{by.inspect} duration sum"

    if intervals
      assert_equal expected.fetch(:interval_ms_count), summary.fetch(:interval_ms_count), "#{label} #{by.inspect} interval count"
      assert_equal expected.fetch(:interval_ms_sum), summary.fetch(:interval_ms_sum), "#{label} #{by.inspect} interval sum"
    else
      assert_equal 0, summary.fetch(:interval_ms_count), "#{label} #{by.inspect} interval count"
      assert_equal 0, summary.fetch(:interval_ms_sum), "#{label} #{by.inspect} interval sum"
    end
  end

  def assert_series_count(label, expected_count)
    series = EventMeter.series(
      DELIVERY_EVENT,
      version: DELIVERY_VERSION,
      from: BASE_TIME,
      to: report_to_time,
      every: :minute
    )

    assert_equal expected_count, series.sum { |bucket| bucket.fetch(:count) }, "#{label} series count"
  end

  def assert_stream_empty(label)
    entries = EventMeter.stream_storage.read(name: DELIVERY_EVENT)
    assert_empty entries, "#{label} stream"
  ensure
    EventMeter.stream_storage.release if EventMeter.stream_storage.respond_to?(:release)
  end

  def assert_processed_ids_cleaned(label, rollup_factory)
    case rollup_factory.spec.fetch("kind")
    when "file_rollup"
      pattern = File.join(rollup_factory.spec.fetch("path"), "rollups", "*", "*", "v*", "processed", "*.processed.json")
      assert_empty Dir[pattern], "#{label} file processed sidecars"
    when "redis_rollup"
      redis = new_redis_client
      keys = redis_keys(redis, "#{rollup_factory.spec.fetch("namespace")}:processed:*")
      assert_empty keys, "#{label} redis processed ids"
    when "postgres_rollup"
      connection = postgres_connection(rollup_factory.spec.fetch("url"))
      count = postgres_processed_count(connection, rollup_factory.spec.fetch("table_prefix"))
      assert_equal 0, count, "#{label} postgres processed ids"
    end
  ensure
    close_redis(redis) if defined?(redis)
    close_postgres(connection) if defined?(connection)
  end

  def event_slices_for_writers(events)
    groups = events.group_by { |event| event.fetch(:params).fetch(:customer_id) }
      .values
      .shuffle(random: Random.new(20_260_508))
    slices = Array.new(WRITER_PROCESS_COUNT + WRITER_THREAD_COUNT) { [] }

    groups.each_with_index do |group, index|
      slices[index % slices.length].concat(group.sort_by { |event| event.fetch(:started_at) })
    end

    slices.each { |slice| slice.sort_by! { |event| event.fetch(:started_at) } }
    slices.reject(&:empty?)
  end

  def stress_events
    Array.new(stress_count) do |index|
      customer_slot = index % CUSTOMER_COUNT
      params = {
        customer_id: 10_000 + customer_slot,
        provider: PROVIDERS.fetch(customer_slot % PROVIDERS.length),
        delivery_mode: DELIVERY_MODES.fetch(customer_slot % DELIVERY_MODES.length),
        queue: QUEUES.fetch(customer_slot % QUEUES.length)
      }

      {
        params: params,
        status: status_for(index),
        started_at: BASE_TIME + index * START_STEP_SECONDS,
        duration_ms: 25 + (index % 17)
      }
    end
  end

  def expected_metrics(events)
    all = new_metrics
    providers = PROVIDERS.to_h { |provider| [provider, new_metrics] }
    compounds = Hash.new { |hash, key| hash[key] = new_metrics }
    previous_started_ms = {}

    events.sort_by { |event| event.fetch(:started_at) }.each do |event|
      params = event.fetch(:params)
      customer_id = params.fetch(:customer_id)
      provider = params.fetch(:provider)
      compound_key = [
        provider,
        params.fetch(:delivery_mode),
        params.fetch(:queue)
      ]
      started_ms = (event.fetch(:started_at).to_f * 1000).to_i
      previous_ms = previous_started_ms[customer_id]
      interval_ms = previous_ms ? started_ms - previous_ms : nil

      add_event_counts(all, event)
      add_event_counts(providers.fetch(provider), event)
      add_event_counts(compounds[compound_key], event)
      add_interval_metrics(providers.fetch(provider), interval_ms)
      add_interval_metrics(compounds[compound_key], interval_ms)

      previous_started_ms[customer_id] = started_ms
    end

    {
      all: all,
      providers: providers,
      compounds: compounds
    }
  end

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

  def status_for(index)
    return "failure" if (index % 17).zero?
    return "skipped" if (index % 19).zero?

    "success"
  end

  def stress_count
    count = Integer(ENV.fetch("EVENT_METER_STRESS_COUNT", "240"))
    return count if count.positive?

    raise ArgumentError
  rescue ArgumentError, TypeError, RangeError
    raise ArgumentError, "EVENT_METER_STRESS_COUNT must be a positive integer"
  end

  def report_to_time
    BASE_TIME + stress_count * START_STEP_SECONDS + 60
  end

  def configure_event_meter(namespace:, stream:, rollup:)
    EventMeter.reset
    EventMeter.configure do |config|
      config.namespace = namespace
      config.stream_storage = stream
      config.rollup_storage = rollup
      config.lock_ttl = 1
    end
  end

  def fork_child(label, expected_exit: 0)
    fork do
      yield
      exit! expected_exit
    rescue Exception => error
      warn "#{label}: #{error.class}: #{error.message}"
      warn error.backtrace.first(12).join("\n") if error.backtrace
      exit! 1
    end
  end

  def spawn_processor_child(label, namespace, stream_factory, rollup_factory, crash_before_stream_delete:)
    Process.spawn(
      {
        "EVENT_METER_STRESS_PROCESSOR" => JSON.generate(
          "crash_before_stream_delete" => crash_before_stream_delete,
          "crash_exit_status" => CRASH_BEFORE_STREAM_DELETE_EXIT,
          "event_name" => DELIVERY_EVENT,
          "lock_ttl" => 1,
          "namespace" => namespace,
          "rollup" => rollup_factory.spec,
          "stream" => stream_factory.spec,
          "version" => DELIVERY_VERSION
        )
      },
      RbConfig.ruby,
      "-Ilib",
      "-rbundler/setup",
      "-e",
      processor_child_code,
      chdir: __dir__ + "/.."
    )
  rescue SystemCallError => error
    flunk "#{label} failed to spawn: #{error.class}: #{error.message}"
  end

  def processor_child_code
    <<~'RUBY'
      require "json"
      require "event_meter"

      spec = JSON.parse(ENV.fetch("EVENT_METER_STRESS_PROCESSOR"))

      def redis_client(storage)
        require "redis"

        url = storage.fetch("redis_url").to_s
        url.empty? ? Redis.new : Redis.new(url: url)
      end

      def build_stream(storage)
        case storage.fetch("kind")
        when "file_stream"
          EventMeter::Stores::Stream::File.new(
            path: storage.fetch("path"),
            sync: :flush
          )
        when "redis_stream"
          EventMeter::Stores::Stream::Redis.new(
            redis: redis_client(storage),
            namespace: storage.fetch("namespace")
          )
        else
          raise ArgumentError, "unknown stream storage: #{storage.inspect}"
        end
      end

      def build_rollup(storage)
        case storage.fetch("kind")
        when "file_rollup"
          EventMeter::Stores::Rollup::File.new(path: storage.fetch("path"))
        when "redis_rollup"
          EventMeter::Stores::Rollup::Redis.new(
            redis: redis_client(storage),
            namespace: storage.fetch("namespace")
          )
        when "postgres_rollup"
          require "pg"

          EventMeter::Stores::Rollup::Postgres.new(
            connection: PG.connect(storage.fetch("url")),
            namespace: storage.fetch("namespace"),
            table_prefix: storage.fetch("table_prefix")
          )
        else
          raise ArgumentError, "unknown rollup storage: #{storage.inspect}"
        end
      end

      def close_storage(storage)
        return unless storage

        storage.close if storage.respond_to?(:close)
        storage.redis.close if storage.respond_to?(:redis) && storage.redis.respond_to?(:close)
        storage.connection.close if storage.respond_to?(:connection) && !storage.connection.finished?
      rescue StandardError
        nil
      end

      class CrashBeforeStreamDelete
        def initialize(stream, exit_status)
          @stream = stream
          @exit_status = exit_status
        end

        def read(name:)
          @stream.read(name: name)
        end

        def delete
          Process.exit!(@exit_status)
        end

        def release
          @stream.release if @stream.respond_to?(:release)
        end

        def close
          @stream.close if @stream.respond_to?(:close)
        end
      end

      status = 0
      stream = nil
      rollup = nil

      begin
        stream = build_stream(spec.fetch("stream"))
        if spec.fetch("crash_before_stream_delete")
          stream = CrashBeforeStreamDelete.new(stream, spec.fetch("crash_exit_status"))
        end
        rollup = build_rollup(spec.fetch("rollup"))

        EventMeter.reset
        EventMeter.configure do |config|
          config.namespace = spec.fetch("namespace")
          config.stream_storage = stream
          config.rollup_storage = rollup
          config.lock_ttl = spec.fetch("lock_ttl")
        end

        EventMeter.process_pending(spec.fetch("event_name"), version: spec.fetch("version")) do |report|
          report.index_by(:customer_id)
          report.index_by(:provider)
          report.index_by(:provider, :delivery_mode, :queue)
          report.measure_interval_by(:customer_id, group_by: :provider)
          report.measure_interval_by(:customer_id, group_by: [:provider, :delivery_mode, :queue])
        end
      rescue StandardError => error
        warn "#{error.class}: #{error.message}"
        warn error.backtrace.first(12).join("\n") if error.backtrace
        status = 1
      ensure
        close_storage(stream)
        close_storage(rollup)
      end

      Process.exit!(status)
    RUBY
  end

  def assert_successful_children(pids, label)
    pids.each do |pid|
      assert_child_exit(pid, 0, label)
    end
  end

  def assert_child_exit(pid, expected_exit, label)
    _pid, status = Process.wait2(pid)

    assert status.exited?, "#{label} was signaled: #{status.inspect}"
    assert_equal expected_exit, status.exitstatus, "#{label} exit status"
  end

  def with_file_stream_factories
    Dir.mktmpdir("event-meter-stress-stream") do |root|
      past_clock = proc { Time.now.utc - 120 }

      stream_factory = storage_factory("kind" => "file_stream", "path" => root) {
        EventMeter::Stores::Stream::File.new(
          path: root,
          sync: :flush
        )
      }
      writer_stream_factory = storage_factory("kind" => "file_stream", "path" => root) {
        EventMeter::Stores::Stream::File.new(
          path: root,
          sync: :flush,
          clock: past_clock
        )
      }

      yield stream_factory, writer_stream_factory
    end
  end

  def with_file_rollup_factory
    Dir.mktmpdir("event-meter-stress-rollup") do |root|
      yield storage_factory("kind" => "file_rollup", "path" => root) {
        EventMeter::Stores::Rollup::File.new(path: root)
      }
    end
  end

  def with_redis_namespace(label)
    namespace = stress_namespace(label)
    redis = new_redis_client

    cleanup_redis(redis, namespace)
    yield namespace
  ensure
    cleanup_redis(redis, namespace) if redis && namespace
    close_redis(redis)
  end

  def redis_stream_factory(namespace)
    storage_factory(
      "kind" => "redis_stream",
      "namespace" => namespace,
      "redis_url" => redis_url
    ) {
      EventMeter::Stores::Stream::Redis.new(
        redis: new_redis_client,
        namespace: namespace
      )
    }
  end

  def redis_rollup_factory(namespace)
    storage_factory(
      "kind" => "redis_rollup",
      "namespace" => namespace,
      "redis_url" => redis_url
    ) {
      EventMeter::Stores::Rollup::Redis.new(
        redis: new_redis_client,
        namespace: namespace
      )
    }
  end

  def with_postgres_namespace(label)
    namespace = stress_namespace(label)

    with_postgres_rollup_factory(namespace) do |rollup_factory|
      yield namespace, rollup_factory
    end
  end

  def with_postgres_rollup_factory(namespace)
    url = postgres_url
    connection = postgres_connection(url)
    table_prefix = "em_stress_#{Process.pid}_#{SecureRandom.hex(4)}"

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    yield storage_factory(
      "kind" => "postgres_rollup",
      "namespace" => namespace,
      "table_prefix" => table_prefix,
      "url" => url
    ) {
      EventMeter::Stores::Rollup::Postgres.new(
        connection: postgres_connection(url),
        namespace: namespace,
        table_prefix: table_prefix
      )
    }
  ensure
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    close_postgres(connection)
  end

  def storage_factory(spec, &block)
    StorageFactory.new(spec: spec, builder: block)
  end

  def new_redis_client
    require "redis"

    url = redis_url
    client = url.empty? ? Redis.new : Redis.new(url: url)
    client.tap(&:ping)
  rescue LoadError, Redis::BaseConnectionError, Errno::ECONNREFUSED => error
    skip "Redis is unavailable: #{error.class}: #{error.message}"
  end

  def redis_url
    ENV["EVENT_METER_REDIS_URL"].to_s.strip
  end

  def postgres_url
    EventMeterTestSupport::PostgresDatabase.url(test: self)
  end

  def postgres_connection(url)
    EventMeterTestSupport::PostgresDatabase.connect(test: self, url: url)
  end

  def cleanup_redis(redis, namespace)
    keys = redis_keys(redis, "#{namespace}:*")
    keys.each_slice(500) { |slice| redis.del(*slice) unless slice.empty? }
  end

  def redis_keys(redis, pattern)
    keys = []
    redis.scan_each(match: pattern) { |key| keys << key }
    keys.sort
  end

  def postgres_processed_count(connection, table_prefix)
    connection.exec("SELECT count(*) AS count FROM #{table_prefix}_processed_entries").first.fetch("count").to_i
  end

  def drop_postgres_tables(connection, table_prefix)
    connection.exec(<<~SQL)
      DROP TABLE IF EXISTS
        #{table_prefix}_processed_entries,
        #{table_prefix}_strings,
        #{table_prefix}_rollups
    SQL
  end

  def close_storage(storage)
    return unless storage

    storage.close if storage.respond_to?(:close)
    close_redis(storage.redis) if storage.respond_to?(:redis)
    close_postgres(storage.connection) if storage.respond_to?(:connection)
  end

  def close_redis(redis)
    redis&.close if redis&.respond_to?(:close)
  rescue IOError
    nil
  end

  def close_postgres(connection)
    return unless connection
    return if connection.respond_to?(:finished?) && connection.finished?

    connection.close
  rescue StandardError
    nil
  end

  def stress_namespace(label)
    safe_label = label.to_s.gsub(/[^a-z0-9]+/i, "_")
    "event_meter:test:stress:#{safe_label}:#{Process.pid}:#{SecureRandom.hex(4)}"
  end

  StorageFactory = Struct.new(:spec, :builder, keyword_init: true) do
    def call
      builder.call
    end
  end
end

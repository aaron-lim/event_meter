require "test_helper"
require "event_meter/rails"

class EventMeterRailsTest < EventMeterTest
  def test_active_record_postgres_uses_one_checked_out_connection_for_transactions
    raw_connection = fake_raw_postgres_connection
    pool = fake_active_record_pool(raw_connection)
    connection = EventMeter::Stores::Rollup::ActiveRecordPostgres::Connection.new(
      fake_active_record_class(pool)
    )

    connection.exec("BEGIN")
    connection.exec_params("SELECT $1", [123])
    connection.exec("COMMIT")

    assert_equal 1, pool.checkouts
    assert_equal 1, pool.checkins
    assert_equal 0, pool.with_connection_calls
    assert_equal [
      [:exec, "BEGIN"],
      [:exec_params, "SELECT $1", [123]],
      [:exec, "COMMIT"]
    ], raw_connection.calls
  end

  def test_active_record_postgres_returns_transaction_connection_after_rollback
    raw_connection = fake_raw_postgres_connection
    pool = fake_active_record_pool(raw_connection)
    connection = EventMeter::Stores::Rollup::ActiveRecordPostgres::Connection.new(
      fake_active_record_class(pool)
    )

    connection.exec("BEGIN")
    connection.exec("ROLLBACK")

    assert_equal 1, pool.checkouts
    assert_equal 1, pool.checkins
    connection.exec_params("SELECT $1", [456])
    assert_equal 1, pool.with_connection_calls
  end

  def test_active_record_postgres_uses_pooled_connections_outside_transactions
    raw_connection = fake_raw_postgres_connection
    pool = fake_active_record_pool(raw_connection)
    connection = EventMeter::Stores::Rollup::ActiveRecordPostgres::Connection.new(
      fake_active_record_class(pool)
    )

    connection.exec_params("SELECT $1", [123])

    assert_equal 0, pool.checkouts
    assert_equal 0, pool.checkins
    assert_equal 1, pool.with_connection_calls
    assert_equal [[:exec_params, "SELECT $1", [123]]], raw_connection.calls
  end

  def test_active_record_postgres_install_uses_the_active_record_pool
    raw_connection = fake_raw_postgres_connection
    pool = fake_active_record_pool(raw_connection)

    EventMeter::Stores::Rollup::ActiveRecordPostgres.install!(
      connection_class: fake_active_record_class(pool),
      table_prefix: "meter_test"
    )

    assert_equal 1, pool.with_connection_calls
    assert_equal 1, raw_connection.calls.length
    assert_includes raw_connection.calls.first.last, "CREATE TABLE IF NOT EXISTS meter_test_rollups"
  end

  def test_event_meter_rails_configures_file_stream_and_active_record_postgres_rollup
    pool = fake_active_record_pool(fake_raw_postgres_connection)

    Dir.mktmpdir("event-meter-rails-config") do |path|
      EventMeter::Rails.configure(
        namespace: "billing_app:event_meter:v1",
        stream_storage: :file,
        stream_path: path,
        rollup_storage: :postgres,
        table_prefix: "meter_test",
        connection_class: fake_active_record_class(pool),
        auto_cleanup_history: true,
        cleanup_history_retention: 123,
        cleanup_history_interval: 45,
        summary_key_limit: 67
      )

      assert_equal "billing_app:event_meter:v1", EventMeter.configuration.namespace
      assert_equal true, EventMeter.configuration.auto_cleanup_history
      assert_equal 123, EventMeter.configuration.cleanup_history_retention
      assert_equal 45, EventMeter.configuration.cleanup_history_interval
      assert_equal 67, EventMeter.configuration.summary_key_limit
      assert_instance_of EventMeter::Stores::Stream::File, EventMeter.stream_storage
      assert_instance_of EventMeter::Stores::Rollup::ActiveRecordPostgres, EventMeter.rollup_storage
      assert_equal File.expand_path(path), EventMeter.stream_storage.path
      assert_equal "meter_test", EventMeter.rollup_storage.table_prefix
    end
  end

  def test_event_meter_rails_rejects_unknown_storage_choices
    pool = fake_active_record_pool(fake_raw_postgres_connection)

    Dir.mktmpdir("event-meter-rails-config") do |path|
      assert_raises(ArgumentError) do
        EventMeter::Rails.configure(
          namespace: "billing_app:event_meter:v1",
          stream_storage: :redis,
          stream_path: path,
          rollup_storage: :postgres,
          connection_class: fake_active_record_class(pool)
        )
      end

      assert_raises(ArgumentError) do
        EventMeter::Rails.configure(
          namespace: "billing_app:event_meter:v1",
          stream_storage: :file,
          stream_path: path,
          rollup_storage: :redis,
          connection_class: fake_active_record_class(pool)
        )
      end
    end
  end

  private

  def fake_raw_postgres_connection
    Class.new do
      attr_reader :calls

      def initialize
        @calls = []
      end

      def exec(sql)
        @calls << [:exec, sql]
        []
      end

      def exec_params(sql, params)
        @calls << [:exec_params, sql, params]
        []
      end
    end.new
  end

  def fake_active_record_pool(raw_connection)
    Class.new do
      attr_reader :checkouts, :checkins, :with_connection_calls

      def initialize(raw_connection)
        @connection = Struct.new(:raw_connection).new(raw_connection)
        @checkouts = 0
        @checkins = 0
        @with_connection_calls = 0
      end

      def checkout
        @checkouts += 1
        @connection
      end

      def checkin(connection)
        @checkins += 1
        raise "unexpected connection" unless connection.equal?(@connection)
      end

      def with_connection
        @with_connection_calls += 1
        yield @connection
      end
    end.new(raw_connection)
  end

  def fake_active_record_class(pool)
    Class.new do
      define_singleton_method(:connection_pool) { pool }
    end
  end
end

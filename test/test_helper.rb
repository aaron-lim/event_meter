$LOAD_PATH.unshift File.expand_path("../lib", __dir__)

require "minitest/autorun"
require "minitest/mock"
require "tmpdir"
require "event_meter"
require_relative "support/memory_stream_storage"
require_relative "support/memory_rollup_storage"
require_relative "support/postgres_database"

class EventMeterTest < Minitest::Test
  DELIVERY_EVENT = "invoice_delivery"
  DELIVERY_VERSION = 1

  def setup
    EventMeter.reset
    EventMeter.configure do |config|
      config.namespace = "event_meter:test"
      config.stream_storage = memory_stream_storage
      config.rollup_storage = memory_rollup_storage(namespace: config.namespace)
    end
  end

  def configure_delivery_event
    # Kept as a readable marker in tests: recording no longer needs report config.
  end

  def configure_delivery_indexes
    # Kept as a readable marker in tests: indexes are defined while processing.
  end

  def define_delivery_report(report)
    report.index_by(:customer_id)
    report.index_by(:provider)
    report.index_by(:provider, :delivery_mode, :queue)
    report.measure_interval_by(:customer_id, group_by: :provider)
    report.measure_interval_by(:customer_id, group_by: [:provider, :delivery_mode, :queue])
  end

  def define_delivery_indexes(report)
    report.index_by(:customer_id)
    report.index_by(:provider)
    report.index_by(:provider, :delivery_mode, :queue)
  end

  def process_delivery_pending
    EventMeter.process_pending(DELIVERY_EVENT, version: DELIVERY_VERSION) do |report|
      define_delivery_report(report)
    end
  end

  def process_delivery_indexes_pending
    EventMeter.process_pending(DELIVERY_EVENT, version: DELIVERY_VERSION) do |report|
      define_delivery_indexes(report)
    end
  end

  def delivery_report_definition
    EventMeter::ReportDefinition.build(DELIVERY_EVENT, version: DELIVERY_VERSION) do |report|
      define_delivery_report(report)
    end
  end

  def delivery_indexes_definition
    EventMeter::ReportDefinition.build(DELIVERY_EVENT, version: DELIVERY_VERSION) do |report|
      define_delivery_indexes(report)
    end
  end

  def expected_delivery_result(processed:, skipped: 0, malformed: 0, complete: true, locked: false)
    {
      event_name: DELIVERY_EVENT,
      version: DELIVERY_VERSION,
      processed: processed,
      skipped_already_processed: skipped,
      malformed: malformed,
      complete: complete,
      locked: locked
    }
  end

  def record_delivery(attributes = {}, status: "success", started_at: nil, duration_ms: nil)
    append_event(
      DELIVERY_EVENT,
      attributes,
      status: status,
      started_at: started_at,
      duration_ms: duration_ms
    )
  end

  def append_event(name, attributes = {}, status: "success", started_at: nil, duration_ms: nil)
    EventMeter.stream_storage.append(EventMeter::EventPayload.build(
      name,
      params: attributes,
      status: status,
      started_at: started_at,
      duration_ms: duration_ms
    ))
  end

  def utc(year, month, day, hour, minute, second = 0)
    Time.utc(year, month, day, hour, minute, second)
  end

  def memory_stream_storage
    EventMeterTestSupport::MemoryStreamStorage.new
  end

  def memory_rollup_storage(namespace:)
    EventMeterTestSupport::MemoryRollupStorage.new(namespace: namespace)
  end
end

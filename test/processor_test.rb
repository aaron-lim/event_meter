require "test_helper"

class EventMeterProcessorTest < EventMeterTest
  def test_process_pending_without_entries_is_a_clean_no_op
    result = process_delivery_pending.to_h

    assert_equal expected_delivery_result(processed: 0), result
  end

  def test_process_pending_processes_whatever_the_stream_returns
    stream = Class.new do
      attr_reader :delete_calls, :read_calls

      def initialize
        @delete_calls = 0
        @read_calls = 0
      end

      def read(name:)
        @read_calls += 1
        [["entry-1", nil]]
      end

      def delete
        @delete_calls += 1
      end
    end.new

    EventMeter.configure do |config|
      config.stream_storage = stream
      config.rollup_storage = memory_rollup_storage(namespace: "event_meter:test")
    end

    result = process_delivery_pending

    assert_equal expected_delivery_result(processed: 1, malformed: 1), result.to_h
    assert_equal 1, stream.read_calls
    assert_equal 1, stream.delete_calls
  end

  def test_wrong_named_payloads_are_malformed_instead_of_poisoning_the_stream
    stream = Class.new do
      attr_reader :deleted

      def initialize(payload)
        @payload = payload
        @deleted = false
      end

      def read(name:)
        [["wrong-name-1", @payload]]
      end

      def delete
        @deleted = true
      end
    end.new(EventMeter::EventPayload.build(
      "receipt_delivery",
      params: { customer_id: 44, provider: "postmark" },
      status: "success",
      started_at: utc(2026, 5, 6, 1, 0),
      duration_ms: 100
    ).to_h)

    EventMeter.configure do |config|
      config.stream_storage = stream
      config.rollup_storage = memory_rollup_storage(namespace: "event_meter:test")
    end

    result = process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1)
    )

    assert_equal expected_delivery_result(processed: 1, malformed: 1), result.to_h
    assert_equal 0, summary.fetch(:count)
    assert_equal true, stream.deleted
  end

  def test_rollup_storage_keeps_existing_lock_when_nested_lock_is_rejected
    store = EventMeter.rollup_storage
    inner_result = nil

    outer_result = store.with_lock(ttl: 30) do
      inner_result = store.with_lock(ttl: 30) { true }
    end

    assert_equal true, outer_result
    assert_equal false, inner_result
    assert_equal true, store.with_lock(ttl: 30) { true }
  end
end

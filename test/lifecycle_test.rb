require "test_helper"

class EventMeterLifecycleTest < EventMeterTest
  def test_success_records_calculated_duration
    configure_delivery_event
    event = nil
    result = nil

    Time.stub(:now, utc(2026, 5, 6, 1, 0, 0)) do
      event = EventMeter.start("invoice_delivery",
        customer_id: 44,
        provider: "postmark",
        worker_id: "worker-1"
      )
    end

    Time.stub(:now, utc(2026, 5, 6, 1, 0, 2)) do
      result = event.success(changed: true)
    end

    process_delivery_pending

    refute event.error?
    assert result.recorded?
    refute result.error?
    assert_equal "invoice_delivery", result.payload.fetch("name")
    assert_equal "success", result.payload.fetch("status")
    assert_equal "2026-05-06T01:00:00.000000Z", result.payload.fetch("started_at")
    assert_equal 2_000, result.payload.fetch("duration_ms")
    assert_equal({
      "customer_id" => 44,
      "provider" => "postmark",
      "worker_id" => "worker-1",
      "changed" => true
    }, result.payload.fetch("params"))
    assert_equal 1, EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION, by: { customer_id: 44 }).fetch(:count)
  end

  def test_start_merges_attribute_objects_and_keyword_attributes
    configure_delivery_event
    attributes = Struct.new(:to_h).new({ customer_id: 44 })
    event = nil
    result = nil

    Time.stub(:now, utc(2026, 5, 6, 1, 0, 0)) do
      event = EventMeter.start("invoice_delivery", attributes, provider: "postmark")
      result = event.success
    end

    process_delivery_pending

    assert_equal({
      "customer_id" => 44,
      "provider" => "postmark"
    }, result.payload.fetch("params"))
  end

  def test_start_rejects_attributes_that_cannot_be_converted_to_hash
    configure_delivery_event

    event = EventMeter.start("invoice_delivery", Object.new)
    result = event.success

    assert event.error?
    assert_instance_of TypeError, event.error
    assert_equal "event attributes must respond to to_h", event.error.message
    assert result.error?
    assert_same event.error, result.error
    assert_nil result.payload
  end

  def test_start_rejects_attribute_objects_that_do_not_return_hashes
    configure_delivery_event
    attributes = Struct.new(:to_h).new([])

    event = EventMeter.start("invoice_delivery", attributes)
    result = event.success

    assert event.error?
    assert_instance_of TypeError, event.error
    assert_equal "event attributes#to_h must return a Hash", event.error.message
    assert result.error?
    assert_same event.error, result.error
  end

  def test_skip_and_failure_record_expected_outcomes
    configure_delivery_event
    skipped = nil
    failure = nil
    skipped_result = nil
    failure_result = nil

    Time.stub(:now, utc(2026, 5, 6, 1, 0, 0)) do
      skipped = EventMeter.start("invoice_delivery", customer_id: 44, provider: "postmark")
      failure = EventMeter.start("invoice_delivery", customer_id: 45, provider: "postmark")
    end

    Time.stub(:now, utc(2026, 5, 6, 1, 0, 1)) do
      skipped_result = skipped.skip("customer_paused")
      failure_result = failure.failure(StandardError.new("timeout"), retryable: true)
    end

    process_delivery_pending

    assert_equal "skipped", skipped_result.payload.fetch("status")
    assert_equal "customer_paused", skipped_result.payload.dig("params", "skip_reason")
    assert_equal 1_000, skipped_result.payload.fetch("duration_ms")

    assert_equal "failure", failure_result.payload.fetch("status")
    assert_equal "StandardError", failure_result.payload.dig("params", "error_class")
    assert_equal "timeout", failure_result.payload.dig("params", "error_message")
    assert_equal true, failure_result.payload.dig("params", "retryable")
    assert_equal 1_000, failure_result.payload.fetch("duration_ms")
    assert_equal 1, EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION, by: { customer_id: 44 }).fetch(:skipped_count)
    assert_equal 1, EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION, by: { customer_id: 45 }).fetch(:failure_count)
  end

  def test_failure_accepts_plain_error_messages
    configure_delivery_event

    event = EventMeter.start("invoice_delivery", customer_id: 44, provider: "postmark")
    result = event.failure("timeout")

    process_delivery_pending

    assert_equal "failure", result.payload.fetch("status")
    assert_equal "String", result.payload.dig("params", "error_class")
    assert_equal "timeout", result.payload.dig("params", "error_message")
  end

  def test_event_fields_are_kept_separate_from_user_params
    configure_delivery_event
    event = nil
    result = nil

    Time.stub(:now, utc(2026, 5, 6, 1, 0, 0)) do
      event = EventMeter.start("invoice_delivery",
        customer_id: 44,
        provider: "postmark",
        status: "user_start_status",
        duration_ms: "user_start_duration"
      )
      result = event.success(duration_ms: "user_finish_duration", status: "user_finish_status")
    end

    second_result = event.failure

    process_delivery_pending

    assert second_result.error?
    assert_instance_of EventMeter::AlreadyRecordedError, second_result.error
    assert_equal "event has already been recorded", second_result.error.message
    assert_nil second_result.payload
    assert_equal "success", result.payload.fetch("status")
    assert_equal 0, result.payload.fetch("duration_ms")
    assert_equal "user_finish_status", result.payload.dig("params", "status")
    assert_equal "user_finish_duration", result.payload.dig("params", "duration_ms")
    assert_equal 1, EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION, by: { provider: "postmark" }).fetch(:count)
    refute_respond_to event, :recorded?
  end

  def test_finish_reports_missing_storage_without_raising
    EventMeter.reset

    event = EventMeter.start("invoice_delivery", customer_id: 44)
    result = event.success(message_id: "msg_123")

    refute event.error?
    assert result.error?
    assert_instance_of EventMeter::ConfigurationError, result.error
    assert_equal "configure stream_storage, or set config.redis to use Redis storage", result.error.message
    assert_equal({
      "name" => "invoice_delivery",
      "status" => "success",
      "params" => {
        "customer_id" => 44,
        "message_id" => "msg_123"
      }
    }, result.payload.slice("name", "status", "params"))
  end

  def test_finish_reports_storage_errors_without_raising
    EventMeter.configure do |config|
      config.stream_storage = Class.new do
        def append(_payload)
          raise "stream offline"
        end
      end.new
    end

    event = EventMeter.start("invoice_delivery", customer_id: 44)
    result = event.success

    refute event.error?
    assert result.error?
    assert_equal "stream offline", result.error.message
    assert_equal "invoice_delivery", result.payload.fetch("name")
    assert_equal "success", result.payload.fetch("status")
  end

  def test_finish_reports_invalid_final_attributes_without_raising
    configure_delivery_event

    event = EventMeter.start("invoice_delivery", customer_id: 44)
    result = event.skip("disabled", Object.new)

    refute event.error?
    assert result.error?
    assert_instance_of TypeError, result.error
    assert_equal "event attributes must respond to to_h", result.error.message
    assert_nil result.payload
  end

  def test_start_accepts_nil_attributes
    configure_delivery_event
    event = nil
    result = nil

    Time.stub(:now, utc(2026, 5, 6, 1, 0, 0)) do
      event = EventMeter.start("invoice_delivery", nil)
      result = event.success
    end

    process_delivery_pending

    assert_equal "invoice_delivery", result.payload.fetch("name")
    assert_equal "success", result.payload.fetch("status")
    refute result.payload.key?("params")
    assert_equal 1, EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION).fetch(:count)
  end

  def test_lifecycle_clamps_negative_duration_to_zero
    configure_delivery_event
    event = nil
    result = nil

    Time.stub(:now, utc(2026, 5, 6, 1, 0, 2)) do
      event = EventMeter.start("invoice_delivery", customer_id: 44, provider: "postmark")
    end

    Time.stub(:now, utc(2026, 5, 6, 1, 0, 0)) do
      result = event.success
    end

    process_delivery_pending

    assert_equal 0, result.payload.fetch("duration_ms")
    assert_equal 0, EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION, by: { provider: "postmark" }).fetch(:duration_ms_min)
  end

  def test_event_payload_rejects_duration_values_that_are_not_integers
    error = assert_raises(ArgumentError) do
      EventMeter::EventPayload.build("invoice_delivery",
        params: {},
        status: "success",
        started_at: utc(2026, 5, 6, 1, 0),
        duration_ms: Float::NAN
      )
    end

    assert_equal "duration_ms must be an integer", error.message
  end

  def test_event_payload_rejects_started_at_values_that_are_not_times
    error = assert_raises(ArgumentError) do
      EventMeter::EventPayload.build("invoice_delivery",
        params: {},
        status: "success",
        started_at: Object.new,
        duration_ms: 100
      )
    end

    assert_equal "started_at must be a Time or parseable time string", error.message
  end

  def test_event_payload_rejects_blank_status
    error = assert_raises(ArgumentError) do
      EventMeter::EventPayload.build("invoice_delivery",
        params: {},
        status: "",
        started_at: utc(2026, 5, 6, 1, 0),
        duration_ms: 100
      )
    end

    assert_equal "event status cannot be blank", error.message
  end

  def test_completed_event_recording_is_not_public_api
    refute_respond_to EventMeter, :record
    refute_respond_to EventMeter.start("invoice_delivery"), :failed
  end
end

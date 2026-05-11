require "test_helper"

class EventMeterInstrumentationSafetyTest < EventMeterTest
  BadString = Class.new do
    def to_s
      raise "to_s failed"
    end
  end

  BadHash = Class.new do
    def to_h
      raise "to_h failed"
    end
  end

  BadError = Class.new(StandardError) do
    def message
      raise "message failed"
    end
  end

  def test_start_never_raises_for_bad_event_names_or_attributes
    cases = [
      -> { EventMeter.start(nil) },
      -> { EventMeter.start("") },
      -> { EventMeter.start(BadString.new) },
      -> { EventMeter.start("invoice_delivery", Object.new) },
      -> { EventMeter.start("invoice_delivery", BadHash.new) }
    ]

    cases.each do |start_event|
      event = assert_instrumentation_does_not_raise(&start_event)

      assert_instance_of EventMeter::Event, event
      assert event.error?
    end
  end

  def test_start_never_raises_when_the_clock_fails
    event = nil

    Time.stub(:now, -> { raise "clock failed" }) do
      event = assert_instrumentation_does_not_raise do
        EventMeter.start("invoice_delivery", customer_id: 44)
      end
    end

    assert_instance_of EventMeter::Event, event
    assert event.error?
    assert_equal "clock failed", event.error.message
  end

  def test_success_skip_and_failure_never_raise_for_bad_finish_attributes
    cases = [
      -> { EventMeter.start("invoice_delivery").success(Object.new) },
      -> { EventMeter.start("invoice_delivery").success(BadHash.new) },
      -> { EventMeter.start("invoice_delivery").skip(BadString.new) },
      -> { EventMeter.start("invoice_delivery").skip("disabled", Object.new) },
      -> { EventMeter.start("invoice_delivery").failure(BadError.new("ignored")) },
      -> { EventMeter.start("invoice_delivery").failure("timeout", Object.new) }
    ]

    cases.each do |finish_event|
      result = assert_instrumentation_does_not_raise(&finish_event)

      assert_instance_of EventMeter::WriteResult, result
      assert result.error?
    end
  end

  def test_success_skip_and_failure_never_raise_when_storage_is_missing
    EventMeter.reset

    %i[success skip failure].each do |method_name|
      event = EventMeter.start("invoice_delivery", customer_id: 44)
      result = assert_instrumentation_does_not_raise { event.public_send(method_name) }

      assert_instance_of EventMeter::WriteResult, result
      assert result.error?
      assert_instance_of EventMeter::ConfigurationError, result.error
    end
  end

  def test_success_skip_and_failure_never_raise_when_storage_fails
    EventMeter.configure do |config|
      config.stream_storage = Class.new do
        def append(_payload)
          raise "stream failed"
        end
      end.new
    end

    %i[success skip failure].each do |method_name|
      event = EventMeter.start("invoice_delivery", customer_id: 44)
      result = assert_instrumentation_does_not_raise { event.public_send(method_name) }

      assert_instance_of EventMeter::WriteResult, result
      assert result.error?
      assert_equal "stream failed", result.error.message
    end
  end

  def test_success_skip_and_failure_never_raise_after_start_failed
    event = EventMeter.start("invoice_delivery", Object.new)

    %i[success skip failure].each do |method_name|
      result = assert_instrumentation_does_not_raise { event.public_send(method_name) }

      assert_instance_of EventMeter::WriteResult, result
      assert result.error?
      assert_same event.error, result.error
    end
  end

  def test_double_finish_never_raises
    event = EventMeter.start("invoice_delivery", customer_id: 44)

    assert_instrumentation_does_not_raise { event.success }
    second_result = assert_instrumentation_does_not_raise { event.failure }

    assert_instance_of EventMeter::WriteResult, second_result
    assert second_result.error?
    assert_instance_of EventMeter::AlreadyRecordedError, second_result.error
  end

  private

  def assert_instrumentation_does_not_raise
    yield
  rescue StandardError => error
    flunk "instrumentation raised #{error.class}: #{error.message}"
  end
end

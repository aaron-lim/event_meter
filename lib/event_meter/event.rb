require "time"

require_relative "errors"
require_relative "event_payload"
require_relative "hash_input"
require_relative "write_result"

module EventMeter
  class Event
    INVALID_EVENT_NAME = "event_meter.invalid"

    attr_reader :name, :started_at, :finished_at, :status, :error

    def self.start(name, attributes, keyword_attributes, started_at:)
      new(name, attributes, keyword_attributes, started_at: started_at)
    end

    def self.failed(error)
      new(INVALID_EVENT_NAME, error: error)
    end

    def initialize(name, attributes = nil, keyword_attributes = {}, started_at: nil, error: nil)
      @started_at = nil
      @finished_at = nil
      @status = nil
      @recorded = false
      @error = nil

      if error
        @name = INVALID_EVENT_NAME
        @base_attributes = {}
        @error = error
        return
      end

      @started_at = started_at&.utc
      @name = normalize_name(name)
      @base_attributes = normalize_start_attributes(attributes, keyword_attributes)
    rescue StandardError => error
      @name = fallback_name(name)
      @base_attributes = {}
      @error = error
    end

    def success(attributes = {})
      finish("success") { attributes }
    end

    def skip(reason_or_attributes = nil, attributes = {})
      finish("skipped") do
        normalize_reason_attributes(reason_or_attributes, attributes, :skip_reason)
      end
    end

    def failure(error_or_attributes = nil, attributes = {})
      finish("failure") do
        normalize_error_attributes(error_or_attributes, attributes)
      end
    end

    def error?
      !error.nil?
    end

    private

    def finish(status)
      if recorded?
        return WriteResult.failed(
          payload: nil,
          error: AlreadyRecordedError.new("event has already been recorded")
        )
      end

      @started_at ||= current_time
      @status = status
      @finished_at = current_time

      return WriteResult.failed(payload: nil, error: error) if error?

      payload = nil
      payload_hash = nil
      attributes = yield

      payload = EventPayload.build(
        name,
        params: record_attributes(attributes),
        status: status,
        started_at: started_at,
        duration_ms: duration_ms
      )
      payload_hash = payload.to_h

      EventMeter.stream_storage.append(payload)
      @recorded = true
      WriteResult.recorded(payload_hash)
    rescue StandardError => error
      WriteResult.failed(payload: payload_hash, error: error)
    end

    def record_attributes(attributes)
      @base_attributes.merge(normalize_attributes(attributes))
    end

    def duration_ms
      [((finished_at.to_f - started_at.to_f) * 1000).round, 0].max
    end

    def normalize_reason_attributes(reason_or_attributes, attributes, key)
      attributes = normalize_attributes(attributes)

      case reason_or_attributes
      when nil
        attributes
      when Hash
        normalize_attributes(reason_or_attributes).merge(attributes)
      else
        { key => reason_or_attributes.to_s }.merge(attributes)
      end
    end

    def normalize_error_attributes(error_or_attributes, attributes)
      attributes = normalize_attributes(attributes)

      case error_or_attributes
      when nil
        attributes
      when Hash
        normalize_attributes(error_or_attributes).merge(attributes)
      else
        {
          error_class: error_or_attributes.class.name,
          error_message: error_message(error_or_attributes)
        }.merge(attributes)
      end
    end

    def normalize_attributes(attributes)
      HashInput.coerce(attributes, "event attributes")
    end

    def normalize_start_attributes(attributes, keyword_attributes)
      normalize_attributes(attributes).merge(normalize_attributes(keyword_attributes))
    end

    def normalize_name(value)
      name = value.to_s
      raise ArgumentError, "event name cannot be blank" if name.strip.empty?

      name
    end

    def fallback_name(value)
      name = safe_string(value)
      return INVALID_EVENT_NAME if name.strip.empty?

      name
    end

    def safe_string(value)
      value.to_s
    rescue StandardError
      INVALID_EVENT_NAME
    end

    def error_message(error)
      return error.message if error.respond_to?(:message)

      error.to_s
    end

    def recorded?
      @recorded
    end

    def current_time
      Time.now.utc
    end
  end
end

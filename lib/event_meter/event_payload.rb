require "json"
require "time"

require_relative "hash_input"

module EventMeter
  class EventPayload
    attr_reader :name, :status, :started_at, :duration_ms, :params

    def self.build(name, params:, status: nil, started_at: nil, duration_ms: nil)
      new(
        name: name,
        params: params,
        status: status,
        started_at: started_at,
        duration_ms: duration_ms
      )
    end

    def self.load(hash)
      hash = stringify(hash)

      new(
        name: hash.fetch("name"),
        params: hash.fetch("params", {}),
        status: hash["status"],
        started_at: hash.fetch("started_at"),
        duration_ms: hash["duration_ms"]
      )
    end

    def initialize(name:, params:, status:, started_at:, duration_ms:)
      now = Time.now.utc

      @name = normalize_name(name)
      @status = normalize_status(status || "success")
      @started_at = parse_time(started_at || now)
      @duration_ms = integer_or_nil(duration_ms)
      @params = stringify(params)
    end

    def to_h
      {
        "name" => name,
        "status" => status,
        "started_at" => started_at.iso8601(6),
        "duration_ms" => duration_ms,
        "params" => params
      }.delete_if { |_key, value| value.nil? || value == {} }
    end

    def to_json(*args)
      to_h.to_json(*args)
    end

    def started_ms
      (started_at.to_f * 1000).to_i
    end

    private

    def self.stringify(hash)
      HashInput.coerce(hash, "event params").each_with_object({}) do |(key, value), result|
        result[key.to_s] = value
      end
    end

    def stringify(hash)
      self.class.stringify(hash)
    end

    def parse_time(value)
      return value.utc if value.respond_to?(:utc)
      raise ArgumentError unless value.respond_to?(:to_str)

      Time.parse(value.to_str).utc
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "started_at must be a Time or parseable time string"
    end

    def normalize_name(value)
      name = value.to_s
      raise ArgumentError, "event name cannot be blank" if name.strip.empty?

      name
    end

    def normalize_status(value)
      status = value.to_s
      raise ArgumentError, "event status cannot be blank" if status.strip.empty?

      status
    end

    def integer_or_nil(value)
      return nil if value.nil?

      [Integer(value), 0].max
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "duration_ms must be an integer"
    end
  end
end

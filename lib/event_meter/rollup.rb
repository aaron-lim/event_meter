require "time"

module EventMeter
  class Rollup
    MIN_FIELDS = %w[duration_ms_min interval_ms_min started_at_ms_min].freeze
    MAX_FIELDS = %w[duration_ms_max interval_ms_max started_at_ms_max].freeze
    MAX_RENDERABLE_TIMESTAMP_MS = 253_402_300_799_999 # 9999-12-31T23:59:59.999Z

    attr_reader :fields

    def self.from_hash(hash)
      new(hash || {})
    end

    def self.combine(raw_rollups)
      raw_rollups.reduce(new) do |combined, raw|
        combined.merge!(from_hash(raw))
      end
    end

    def self.min_field?(field)
      MIN_FIELDS.include?(field.to_s)
    end

    def self.max_field?(field)
      MAX_FIELDS.include?(field.to_s)
    end

    def initialize(fields = {})
      @fields = {}
      fields.each { |key, value| @fields[key.to_s] = numeric_value(value) }
    end

    def increment(field, value = 1)
      fields[field.to_s] = fields.fetch(field.to_s, 0) + numeric_value(value)
    end

    def add_duration(duration_ms)
      return if duration_ms.nil?

      add_metric("duration_ms", duration_ms)
    end

    def add_interval(interval_ms)
      return if interval_ms.nil?

      add_metric("interval_ms", interval_ms)
    end

    def add_started_at(started_ms)
      return if started_ms.nil?

      value = numeric_value(started_ms)
      fields["started_at_ms_min"] = [fields["started_at_ms_min"], value].compact.min
      fields["started_at_ms_max"] = [fields["started_at_ms_max"], value].compact.max
    end

    def merge!(other)
      other.fields.each do |field, value|
        if self.class.min_field?(field)
          fields[field] = [fields[field], value].compact.min
        elsif self.class.max_field?(field)
          fields[field] = [fields[field], value].compact.max
        else
          increment(field, value)
        end
      end

      self
    end

    def to_h(seconds: nil)
      count = fields.fetch("count", 0)
      duration_count = fields.fetch("duration_ms_count", 0)
      interval_count = fields.fetch("interval_ms_count", 0)
      started_at_min_ms = fields["started_at_ms_min"]
      started_at_max_ms = fields["started_at_ms_max"]
      rate_window_seconds = seconds || inferred_rate_window_seconds(started_at_min_ms, started_at_max_ms)

      {
        count: count,
        success_count: fields.fetch("success_count", 0),
        failure_count: fields.fetch("failure_count", 0),
        skipped_count: fields.fetch("skipped_count", 0),
        started_at_min: iso_time(started_at_min_ms),
        started_at_max: iso_time(started_at_max_ms),
        rate_window_seconds: rate_window_seconds,
        per_second: rate(count, rate_window_seconds),
        per_minute: rate(count * 60, rate_window_seconds),
        duration_ms_count: duration_count,
        duration_ms_sum: fields.fetch("duration_ms_sum", 0),
        duration_ms_avg: average(fields.fetch("duration_ms_sum", 0), duration_count),
        duration_ms_min: fields["duration_ms_min"],
        duration_ms_max: fields["duration_ms_max"],
        interval_ms_count: interval_count,
        interval_ms_sum: fields.fetch("interval_ms_sum", 0),
        interval_ms_avg: average(fields.fetch("interval_ms_sum", 0), interval_count),
        interval_ms_min: fields["interval_ms_min"],
        interval_ms_max: fields["interval_ms_max"]
      }.delete_if { |_key, value| value.nil? }
    end

    private

    def inferred_rate_window_seconds(started_at_min_ms, started_at_max_ms)
      return nil unless started_at_min_ms && started_at_max_ms

      (started_at_max_ms - started_at_min_ms) / 1000.0
    end

    def rate(count, seconds)
      return nil unless seconds&.positive?

      count / seconds.to_f
    end

    def average(sum, count)
      return nil unless count.positive?

      sum / count.to_f
    end

    def iso_time(milliseconds)
      return nil unless milliseconds
      return nil if milliseconds.abs > MAX_RENDERABLE_TIMESTAMP_MS

      Time.at(milliseconds / 1000.0).utc.iso8601(6)
    rescue ArgumentError, RangeError
      nil
    end

    def add_metric(prefix, value)
      value = numeric_value(value)
      return if value.negative?

      increment("#{prefix}_count")
      increment("#{prefix}_sum", value)
      fields["#{prefix}_min"] = [fields["#{prefix}_min"], value].compact.min
      fields["#{prefix}_max"] = [fields["#{prefix}_max"], value].compact.max
    end

    def numeric_value(value)
      Integer(value)
    rescue ArgumentError, TypeError, RangeError
      0
    end
  end
end

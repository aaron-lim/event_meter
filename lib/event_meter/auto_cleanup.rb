require "time"

module EventMeter
  class AutoCleanup
    WATERMARK_KEY_SUFFIX = "auto_cleanup:history:last_run"

    attr_reader :configuration, :rollup_storage

    def initialize(configuration:, rollup_storage:)
      @configuration = configuration
      @rollup_storage = rollup_storage
    end

    def run
      return false unless configuration.auto_cleanup_history
      return false unless rollup_storage.respond_to?(:cleanup_history)

      with_cleanup_lock { cleanup_if_due }
    rescue StandardError => error
      report_error(error)
      false
    end

    private

    def with_cleanup_lock
      return yield unless rollup_storage.respond_to?(:with_lock)

      result = false
      locked = rollup_storage.with_lock(ttl: configuration.lock_ttl) do
        result = yield
      end

      locked ? result : false
    end

    def cleanup_if_due
      return false unless cleanup_due?

      result = rollup_storage.cleanup_history(
        before: cleanup_before,
        events: nil,
        interval_state: true
      )
      write_watermark
      result
    end

    def cleanup_due?
      last_cleanup_at.nil? ||
        last_cleanup_at <= current_time - configuration.cleanup_history_interval
    end

    def last_cleanup_at
      return unless rollup_storage.respond_to?(:cleanup_watermark)

      parse_time(rollup_storage.cleanup_watermark(watermark_key))
    end

    def write_watermark
      return unless rollup_storage.respond_to?(:write_cleanup_watermark)

      rollup_storage.write_cleanup_watermark(watermark_key, current_time.iso8601(6))
    end

    def cleanup_before
      current_time - configuration.cleanup_history_retention
    end

    def watermark_key
      [configuration.namespace, WATERMARK_KEY_SUFFIX].join(":")
    end

    def current_time
      Time.now.utc
    end

    def parse_time(value)
      return if value.nil?

      Time.parse(value.to_s).utc
    rescue ArgumentError, TypeError, RangeError
      nil
    end

    def report_error(error)
      handler = configuration.auto_cleanup_error_handler
      handler&.call(error)
    rescue StandardError
      nil
    end
  end
end

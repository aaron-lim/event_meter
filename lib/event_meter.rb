require "time"

require_relative "event_meter/version"
require_relative "event_meter/errors"
require_relative "event_meter/hash_input"
require_relative "event_meter/configuration"
require_relative "event_meter/write_result"
require_relative "event_meter/event"
require_relative "event_meter/event_payload"
require_relative "event_meter/index_key"
require_relative "event_meter/path_name"
require_relative "event_meter/report_definition"
require_relative "event_meter/rollup"
require_relative "event_meter/time_buckets"
require_relative "event_meter/keys"
require_relative "event_meter/reports"
require_relative "event_meter/processor"
require_relative "event_meter/auto_cleanup"
require_relative "event_meter/stores/namespace"
require_relative "event_meter/stores/cleanup_helpers"
require_relative "event_meter/stores/file_helpers"
require_relative "event_meter/stores/redis_lock"
require_relative "event_meter/stores/stream/file"
require_relative "event_meter/stores/stream/redis"
require_relative "event_meter/stores/rollup/file"
require_relative "event_meter/stores/rollup/postgres"
require_relative "event_meter/stores/rollup/active_record_postgres"
require_relative "event_meter/stores/rollup/redis"

module EventMeter
  class << self
    def start(name, attributes = nil, **keyword_attributes)
      Event.start(name, attributes, keyword_attributes, started_at: Time.now.utc)
    rescue StandardError => error
      Event.failed(error)
    end

    def configure
      yield configuration if block_given?
      configuration
    end

    def configuration
      @configuration ||= Configuration.new
    end

    def reset
      @configuration = Configuration.new
    end

    def process_pending(name, version:, &block)
      definition = build_report_definition(name, version: version, &block)

      result = Processor.new(
        configuration: configuration,
        report_definition: definition,
        stream_storage: stream_storage,
        rollup_storage: rollup_storage_for(definition)
      ).process

      auto_cleanup_history unless result.locked
      result
    end

    def cleanup_history(before:, events: nil, interval_state: true)
      namespaced_rollup_storage.cleanup_history(
        before: time_value(before),
        events: Array(events).compact.map(&:to_s),
        interval_state: interval_state
      )
    end

    def summary(name, version:, from: nil, to: nil, by: {})
      reports(name, version: version).summary(name, version: version, from: from, to: to, by: by)
    end

    def series(name, version:, from: nil, to: nil, every: :minute, by: {})
      reports(name, version: version).series(name, version: version, from: from, to: to, every: every, by: by)
    end

    def compare(name, version:, before:, after:, by: {})
      reports(name, version: version).compare(name, version: version, before: before, after: after, by: by)
    end

    def report_definition(name, version:)
      storage = rollup_storage_for_name(name, version: version)
      stored = storage.report_definition(name: name, version: version)
      raise DefinitionNotFoundError, "no definition stored for #{name} v#{version}" unless stored

      ReportDefinition.from_h(stored).to_h
    end

    def stream_storage
      configuration.stream_storage
    end

    def rollup_storage
      configuration.rollup_storage
    end

    private

    def auto_cleanup_history
      AutoCleanup.new(
        configuration: configuration,
        rollup_storage: namespaced_rollup_storage
      ).run
    end

    def time_value(value)
      return value.utc if value.respond_to?(:utc)
      raise ArgumentError unless value.respond_to?(:to_str)

      Time.parse(value.to_str).utc
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "time must be a Time or parseable time string"
    end

    def reports(name, version:)
      Reports.new(
        configuration: configuration,
        rollup_storage: rollup_storage_for_name(name, version: version)
      )
    end

    def build_report_definition(name, version:, &block)
      ReportDefinition.build(name, version: version) do |definition|
        block.call(definition) if block
      end
    end

    def rollup_storage_for(definition)
      rollup_storage_for_name(definition.name, version: definition.version)
    end

    def rollup_storage_for_name(name, version:)
      storage = namespaced_rollup_storage
      return storage.for_report(name: name, version: version) if storage.respond_to?(:for_report)

      storage
    end

    def namespaced_rollup_storage
      storage = rollup_storage
      return storage.for_namespace(configuration.namespace) if storage.respond_to?(:for_namespace)

      storage
    end
  end
end

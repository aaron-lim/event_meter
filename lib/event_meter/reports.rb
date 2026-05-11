require "time"

module EventMeter
  class Reports
    DEFAULT_SERIES_SECONDS = 3600

    def initialize(configuration:, rollup_storage:)
      @configuration = configuration
      @rollup_storage = rollup_storage
    end

    def summary(name, version:, from: nil, to: nil, by: {})
      raise ArgumentError, "pass both from: and to:, or neither" if from.nil? != to.nil?
      from = time_value(from) if from
      to = time_value(to) if to
      validate_window!(from, to) if from && to

      definition = report_definition(name, version)
      index = definition.index_for!(by)
      keys, seconds = summary_keys(definition, from: from, to: to, index: index)

      Rollup.combine(@rollup_storage.hgetall_many(keys)).to_h(seconds: seconds)
    end

    def series(name, version:, from: nil, to: nil, every: :minute, by: {})
      every = TimeBuckets.normalize(every)
      to = to ? time_value(to) : TimeBuckets.time(Time.now.utc, every) + TimeBuckets.seconds(every)
      from = time_value(from) if from
      from ||= to - DEFAULT_SERIES_SECONDS
      validate_window!(from, to)

      definition = report_definition(name, version)
      index = definition.index_for!(by)
      buckets = TimeBuckets.between(from, to, every)
      keys = buckets.map { |bucket| rollup_key(definition, every, bucket, index) }
      raw_rollups = @rollup_storage.hgetall_many(keys)
      seconds = TimeBuckets.seconds(every)

      buckets.zip(raw_rollups).map do |bucket, raw|
        Rollup.from_hash(raw).to_h(seconds: seconds).merge(bucket: bucket.iso8601)
      end
    end

    def compare(name, version:, before:, after:, by: {})
      before_from, before_to = comparison_window(before)
      after_from, after_to = comparison_window(after)

      {
        before: summary(name, version: version, from: before_from, to: before_to, by: by),
        after: summary(name, version: version, from: after_from, to: after_to, by: by)
      }
    end

    private

    def summary_keys(definition, from:, to:, index:)
      if from && to
        buckets = TimeBuckets.between(from, to, :minute)
        keys = buckets.map { |bucket| rollup_key(definition, :minute, bucket, index) }
        seconds = buckets.length * TimeBuckets.seconds(:minute)

        [keys, seconds]
      else
        pattern = Keys.rollup_pattern(
          namespace: @configuration.namespace,
          name: definition.name,
          version: definition.version,
          every: :hour,
          index: index
        )

        keys = keys_matching(pattern)
        validate_summary_key_count!(keys)
        [keys, nil]
      end
    end

    def keys_matching(pattern)
      limit = @configuration.summary_key_limit
      storage_key_limit = limit && limit + 1
      method = @rollup_storage.method(:keys_matching)

      if method.parameters.any? { |kind, name| kind == :keyrest || name == :limit }
        @rollup_storage.keys_matching(pattern, limit: storage_key_limit)
      else
        @rollup_storage.keys_matching(pattern)
      end
    end

    def validate_summary_key_count!(keys)
      limit = @configuration.summary_key_limit
      return unless limit && keys.length > limit

      raise ArgumentError, "summary without a time window matched more than #{limit} rollup buckets; pass from: and to:"
    end

    def rollup_key(definition, every, bucket, index)
      Keys.rollup(
        namespace: @configuration.namespace,
        name: definition.name,
        version: definition.version,
        every: every,
        bucket: bucket,
        index: index
      )
    end

    def report_definition(name, version)
      stored = @rollup_storage.report_definition(name: name, version: version)
      raise DefinitionNotFoundError, "no definition stored for #{name} v#{version}" unless stored

      ReportDefinition.from_h(stored)
    end

    def validate_window!(from, to)
      return if to > from

      raise ArgumentError, "to must be after from"
    end

    def comparison_window(window)
      unless window.respond_to?(:begin) && window.respond_to?(:end)
        raise ArgumentError, "comparison windows must be ranges"
      end

      from = window.begin
      to = window.end
      raise ArgumentError, "comparison windows must have a start and end" if from.nil? || to.nil?

      [from, to]
    end

    def time_value(value)
      return value.utc if value.respond_to?(:utc)
      raise ArgumentError unless value.respond_to?(:to_str)

      Time.parse(value.to_str).utc
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "time must be a Time or parseable time string"
    end

  end
end

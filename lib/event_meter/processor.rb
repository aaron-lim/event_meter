module EventMeter
  class Processor
    ROLLUP_BUCKETS = %i[minute hour].freeze

    Result = Struct.new(
      :event_name,
      :version,
      :processed,
      :skipped_already_processed,
      :malformed,
      :complete,
      :locked,
      keyword_init: true
    ) do
      def self.empty(definition)
        new(
          event_name: definition.name,
          version: definition.version,
          processed: 0,
          skipped_already_processed: 0,
          malformed: 0,
          complete: true,
          locked: false
        )
      end

      def self.locked(definition)
        new(
          event_name: definition.name,
          version: definition.version,
          processed: 0,
          skipped_already_processed: 0,
          malformed: 0,
          complete: false,
          locked: true
        )
      end

      def self.processed(definition, processed:, skipped_already_processed:, malformed:, complete:)
        new(
          event_name: definition.name,
          version: definition.version,
          processed: processed,
          skipped_already_processed: skipped_already_processed,
          malformed: malformed,
          complete: complete,
          locked: false
        )
      end

      def to_h
        {
          event_name: event_name,
          version: version,
          processed: processed,
          skipped_already_processed: skipped_already_processed,
          malformed: malformed,
          complete: complete,
          locked: locked
        }
      end
    end

    def initialize(configuration:, report_definition:, stream_storage:, rollup_storage:)
      @configuration = configuration
      @report_definition = report_definition
      @stream_storage = stream_storage
      @rollup_storage = rollup_storage_for(report_definition, rollup_storage)
    end

    def process
      with_stream_lock do
        process_with_rollup_lock
      end
    end

    private

    def with_stream_lock
      return yield unless @stream_storage.respond_to?(:with_lock)

      result = nil
      lock_acquired = @stream_storage.with_lock(ttl: @configuration.lock_ttl) do
        result = yield
      end

      return result if lock_acquired

      Result.locked(@report_definition)
    end

    def process_with_rollup_lock
      # Counts and durations are merge-safe and use processed-entry retry
      # guards. Only interval metrics need the rollup-wide lock because they
      # advance shared "previous start time" state.
      return process_unlocked unless @report_definition.intervals.any?

      result = nil
      lock_acquired = @rollup_storage.with_lock(ttl: @configuration.lock_ttl) do
        result = process_unlocked
      end

      return result if lock_acquired

      Result.locked(@report_definition)
    end

    def rollup_storage_for(definition, storage)
      return storage if scoped_for?(definition, storage)
      return storage.for_report(name: definition.name, version: definition.version) if storage.respond_to?(:for_report)

      storage
    end

    def scoped_for?(definition, storage)
      return false unless storage.respond_to?(:report_name) && storage.respond_to?(:version)

      storage.report_name.to_s == definition.name.to_s && storage.version.to_i == definition.version.to_i
    end

    def process_unlocked
      @rollup_storage.ensure_definition(@report_definition)

      entries = @stream_storage.read(name: @report_definition.name)
      return Result.empty(@report_definition) if entries.empty?

      entry_ids = entries.map(&:first)
      process_entries(entries, entry_ids)
    rescue StandardError
      release_stream if entries
      raise
    end

    def process_entries(entries, entry_ids)
      pending_entries = unprocessed_entries(entries, entry_ids)
      skipped_count = entries.length - pending_entries.length
      batch = build_batch(pending_entries)

      @rollup_storage.apply(batch) unless batch.empty?
      stream_deleted = !!@stream_storage.delete
      @rollup_storage.forget_processed_ids(entry_ids) if stream_deleted && @rollup_storage.respond_to?(:forget_processed_ids)

      Result.processed(
        @report_definition,
        processed: pending_entries.length,
        skipped_already_processed: skipped_count,
        malformed: batch.malformed,
        complete: stream_deleted
      )
    end

    def release_stream
      @stream_storage.release if @stream_storage.respond_to?(:release)
    rescue StandardError
      nil
    end

    def unprocessed_entries(entries, entry_ids)
      processed_ids = @rollup_storage.processed_ids(entry_ids)
      processed_lookup = processed_ids.to_h { |id| [id, true] }
      seen = {}

      entries.reject do |id, _raw_payload|
        duplicate = seen.key?(id)
        seen[id] = true

        duplicate || processed_lookup.key?(id)
      end
    end

    def build_batch(entries)
      Batch.new.tap do |batch|
        entries.each do |id, raw_payload|
          payload = load_payload(raw_payload)
          unless payload
            batch.entry_ids << id
            batch.malformed += 1
            next
          end

          if payload.name == @report_definition.name
            record_event(batch, payload)
            record_intervals(batch, payload)
          else
            batch.malformed += 1
          end

          batch.entry_ids << id
        end
      end
    end

    def load_payload(raw_payload)
      EventPayload.load(raw_payload)
    rescue ArgumentError, KeyError, TypeError
      nil
    end

    def record_event(batch, payload)
      @report_definition.indexes_for(payload).each do |index|
        add_event_rollup(batch, payload, index)
      end
    end

    def add_event_rollup(batch, payload, index)
      ROLLUP_BUCKETS.each do |every|
        rollup = rollup_for(batch, payload, every, index)
        rollup.increment("count")
        rollup.increment("#{payload.status}_count")
        rollup.add_started_at(payload.started_ms)
        rollup.add_duration(payload.duration_ms)
      end
    end

    def record_intervals(batch, payload)
      state_updates = {}

      @report_definition.intervals.each do |definition|
        value = payload.params[definition.param.to_s]
        next if value.nil?
        next unless definition.group_by.all? { |param| ReportDefinition.indexable_value?(payload.params, param) }

        state_key = interval_state_key(payload.name, definition, value)
        previous_ms = previous_interval_ms(batch, state_key)
        current_ms = payload.started_ms
        next if previous_ms && current_ms <= previous_ms

        state_updates[state_key] = current_ms
        next unless previous_ms

        index = definition.group_index.key_for(payload.params)
        built_index = ReportDefinition::BuiltIndex.new(index: definition.group_index, key: index)
        interval_ms = current_ms - previous_ms

        ROLLUP_BUCKETS.each do |every|
          rollup_for(batch, payload, every, built_index).add_interval(interval_ms)
        end
      end

      state_updates.each do |state_key, current_ms|
        batch.interval_state[state_key] = current_ms
        batch.state_updates[state_key] = current_ms
      end
    end

    def previous_interval_ms(batch, state_key)
      batch.interval_state.fetch(state_key) do
        stored_value = @rollup_storage.get(state_key)
        batch.interval_state[state_key] = interval_timestamp(stored_value)
      end
    end

    def interval_timestamp(value)
      return nil if value.nil?

      timestamp = Integer(value)
      timestamp >= 0 ? timestamp : nil
    rescue ArgumentError, TypeError, RangeError
      nil
    end

    def rollup_for(batch, payload, every, index)
      bucket = TimeBuckets.time(payload.started_at, every)
      batch.rollups[rollup_key(payload.name, every, bucket, index)]
    end

    def rollup_key(name, every, bucket, index)
      Keys.rollup(
        namespace: @configuration.namespace,
        name: name,
        version: @report_definition.version,
        every: every,
        bucket: bucket,
        index: index
      )
    end

    def interval_state_key(name, definition, value)
      Keys.interval_state(
        namespace: @configuration.namespace,
        name: name,
        version: @report_definition.version,
        definition: definition,
        value: value
      )
    end

    class Batch
      attr_reader :rollups, :state_updates, :entry_ids, :interval_state
      attr_accessor :malformed

      def initialize
        @rollups = Hash.new { |hash, key| hash[key] = Rollup.new }
        @state_updates = {}
        @entry_ids = []
        @interval_state = {}
        @malformed = 0
      end

      def empty?
        entry_ids.empty?
      end
    end
  end
end

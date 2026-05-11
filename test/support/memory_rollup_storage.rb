module EventMeterTestSupport
  class MemoryRollupStorage
    include EventMeter::Stores::CleanupHelpers

    attr_reader :namespace, :hashes, :strings, :processed, :definitions

    def initialize(namespace:)
      @namespace = namespace
      @hashes = Hash.new { |hash, key| hash[key] = {} }
      @strings = {}
      @processed = {}
      @definitions = {}
      @locked = false
    end

    def for_report(name:, version:)
      self
    end

    def ensure_definition(definition)
      key = definition_key(definition.name, definition.version)
      stored = definitions[key]

      if stored
        stored_definition = EventMeter::ReportDefinition.from_h(stored)
        unless stored_definition.fingerprint == definition.fingerprint
          raise EventMeter::DefinitionChangedError, "#{definition.name} v#{definition.version} changed; bump version"
        end
      else
        definitions[key] = definition.to_h
      end
    end

    def report_definition(name:, version:)
      definitions[definition_key(name, version)]
    end

    def processed_ids(ids)
      ids.select { |id| processed.key?(id) }
    end

    def forget_processed_ids(ids)
      ids.each { |id| processed.delete(id) }
    end

    def apply(batch)
      existing = hgetall_many(batch.rollups.keys)

      batch.rollups.each.with_index do |(key, rollup), index|
        combined = EventMeter::Rollup.from_hash(existing[index]).merge!(rollup)
        hashes[key] = combined.fields.transform_values(&:to_s)
      end

      batch.state_updates.each do |key, value|
        strings[key] = [strings[key]&.to_i, value.to_i].compact.max.to_s
      end

      timestamp = Time.now.utc.iso8601(6)
      batch.entry_ids.each { |id| processed[id] = timestamp }
    end

    def hgetall_many(keys)
      keys.map { |key| hashes[key].dup }
    end

    def keys_matching(pattern, limit: nil)
      keys = hashes.keys.select { |key| File.fnmatch?(pattern, key) }.sort
      limit ? keys.first(limit) : keys
    end

    def get(key)
      strings[key]
    end

    def cleanup_watermark(key)
      strings[key]
    end

    def write_cleanup_watermark(key, value)
      strings[key] = value.to_s
    end

    def with_lock(ttl:)
      lock_acquired = false
      return false if @locked

      @locked = true
      lock_acquired = true

      yield
      true
    ensure
      @locked = false if lock_acquired
    end

    def cleanup_history(before:, events:, interval_state:)
      filter = event_filter(events)
      {
        rollup_keys_deleted: cleanup_rollups(before, filter),
        interval_state_keys_deleted: interval_state ? cleanup_interval_state(before, filter) : 0,
        processed_entries_deleted: cleanup_processed_entries(before, filter)
      }
    end

    private

    def definition_key(name, version)
      EventMeter::Keys.definition(namespace: namespace, name: name, version: version)
    end

    def cleanup_rollups(before, event_filter)
      keys = hashes.keys.select { |key| rollup_key_old?(key, before, event_filter) }
      keys.each { |key| hashes.delete(key) }
      keys.length
    end

    def cleanup_interval_state(before, event_filter)
      before_ms = (before.to_f * 1000).to_i
      keys = strings.keys.grep(/\A#{Regexp.escape(namespace)}:state:/).select do |key|
        state_key_old?(key, before_ms, event_filter, strings[key])
      end

      keys.each { |key| strings.delete(key) }
      keys.length
    end

    def cleanup_processed_entries(before, event_filter)
      return 0 if event_filter

      ids = processed.select do |_id, timestamp|
        processed_entry_old?(timestamp, before)
      end.keys

      ids.each { |id| processed.delete(id) }
      ids.length
    end

    def processed_entry_old?(timestamp, before)
      Time.parse(timestamp).utc < before
    rescue ArgumentError, TypeError
      true
    end
  end
end

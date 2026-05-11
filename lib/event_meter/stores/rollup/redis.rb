require "json"
require "time"

require_relative "../../index_key"
require_relative "../../rollup"
require_relative "../cleanup_helpers"
require_relative "../namespace"
require_relative "../redis_lock"

module EventMeter
  module Stores
    module Rollup
      class Redis
        include CleanupHelpers
        include Namespace
        include RedisLock

        DEFAULT_ROLLUP_TTL = 31 * 24 * 60 * 60

        MIN_FIELD_SCRIPT = <<~LUA
          local current = redis.call("hget", KEYS[1], ARGV[1])
          local current_number = tonumber(current)
          local value = tonumber(ARGV[2])

          if value == nil then
            return redis.error_reply("ERR event_meter rollup min value must be numeric")
          end

          if current == false or current_number == nil or current_number > value then
            redis.call("hset", KEYS[1], ARGV[1], ARGV[2])
          end

          return 1
        LUA
        MAX_FIELD_SCRIPT = <<~LUA
          local current = redis.call("hget", KEYS[1], ARGV[1])
          local current_number = tonumber(current)
          local value = tonumber(ARGV[2])

          if value == nil then
            return redis.error_reply("ERR event_meter rollup max value must be numeric")
          end

          if current == false or current_number == nil or current_number < value then
            redis.call("hset", KEYS[1], ARGV[1], ARGV[2])
          end

          return 1
        LUA
        SET_MAX_SCRIPT = <<~LUA
          local current = redis.call("get", KEYS[1])
          local current_number = tonumber(current)
          local value = tonumber(ARGV[1])

          if value == nil then
            return redis.error_reply("ERR event_meter state value must be numeric")
          end

          if current == false or current_number == nil or current_number < value then
            redis.call("set", KEYS[1], ARGV[1])
          end

          return 1
        LUA

        attr_reader :redis, :lock_redis, :namespace, :report_name, :version, :lock_scope, :rollup_ttl

        def initialize(redis:, namespace:, lock_redis: nil, report_name: nil, version: nil, lock_scope: nil, rollup_ttl: DEFAULT_ROLLUP_TTL)
          @redis = redis
          @lock_redis = lock_redis || redis
          @namespace = normalize_namespace(namespace)
          @report_name = report_name&.to_s
          @version = version&.to_i
          @lock_scope = lock_scope
          @rollup_ttl = positive_integer(rollup_ttl, "rollup_ttl")
        end

        def for_report(name:, version:)
          name = name.to_s
          version = version.to_i

          self.class.new(
            redis: redis,
            lock_redis: lock_redis,
            namespace: namespace,
            report_name: name,
            version: version,
            lock_scope: "#{Keys.event_name(name)}:#{Keys.version_key(version)}",
            rollup_ttl: rollup_ttl
          )
        end

        def ensure_definition(definition)
          key = definition_key(definition.name, definition.version)
          payload = JSON.generate(definition.to_h)
          stored = redis.get(key)

          if stored
            ensure_same_definition!(stored, definition)
          else
            redis.set(key, payload, nx: true)
            ensure_same_definition!(redis.get(key), definition)
          end
        end

        def report_definition(name:, version:)
          payload = redis.get(definition_key(name, version))
          payload && JSON.parse(payload)
        rescue JSON::ParserError, TypeError
          nil
        end

        def processed_ids(ids)
          ensure_scoped!
          return [] if ids.empty?

          values = redis.pipelined do |pipe|
            ids.each { |id| pipe.get(processed_key(id)) }
          end

          ids.zip(values).filter_map { |id, value| id if value }
        end

        def forget_processed_ids(ids)
          ensure_scoped!
          keys = ids.map { |id| processed_key(id) }
          delete_keys(keys)
        end

        def apply(batch)
          ensure_scoped!

          redis.multi do |transaction|
            batch.rollups.each { |key, rollup| apply_rollup(transaction, key, rollup) }

            batch.state_updates.each do |key, value|
              transaction.eval(SET_MAX_SCRIPT, keys: [key], argv: [value])
              transaction.expire(key, rollup_ttl)
            end

            processed_at = Time.now.utc.iso8601(6)
            batch.entry_ids.each do |id|
              key = processed_key(id)
              transaction.set(key, processed_at)
              transaction.expire(key, rollup_ttl)
            end
          end
        end

        def hgetall_many(keys)
          return [] if keys.empty?

          redis.pipelined do |pipe|
            keys.each { |key| pipe.hgetall(key) }
          end
        end

        def keys_matching(pattern, limit: nil)
          limit = positive_integer(limit, "limit") if limit
          keys = []

          redis.scan_each(match: namespace_glob(pattern)) do |key|
            next unless key_matches?(key, pattern)

            keys << key
            break if limit && keys.length >= limit
          end

          keys.sort
        end

        def get(key)
          redis.get(key)
        end

        def cleanup_watermark(key)
          redis.get(key)
        end

        def write_cleanup_watermark(key, value)
          redis.set(key, value)
        end

        def with_lock(ttl:)
          with_redis_lock(lock_key, ttl: ttl) { yield }
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

        def apply_rollup(transaction, key, rollup)
          rollup.fields.each do |field, value|
            if EventMeter::Rollup.min_field?(field)
              transaction.eval(MIN_FIELD_SCRIPT, keys: [key], argv: [field, value])
            elsif EventMeter::Rollup.max_field?(field)
              transaction.eval(MAX_FIELD_SCRIPT, keys: [key], argv: [field, value])
            else
              transaction.hincrby(key, field, value)
            end
          end

          transaction.expire(key, rollup_ttl)
        end

        def positive_integer(value, name)
          integer = Integer(value)
          return integer if integer.positive?

          raise ArgumentError, "#{name} must be positive"
        rescue ArgumentError, TypeError, RangeError
          raise ArgumentError, "#{name} must be positive"
        end

        def lock_key
          [namespace, "process_lock", lock_scope].compact.join(":")
        end

        def processed_key(id)
          Keys.processed(
            namespace: namespace,
            name: report_name,
            version: version,
            id: id
          )
        end

        def ensure_scoped!
          return if report_name && version&.positive?

          raise ConfigurationError, "redis rollup storage must be scoped with for_report"
        end

        def definition_key(name, version)
          Keys.definition(namespace: namespace, name: name, version: version)
        end

        def ensure_same_definition!(stored, definition)
          stored_definition = ReportDefinition.from_h(JSON.parse(stored))
          return if stored_definition.fingerprint == definition.fingerprint

          raise DefinitionChangedError, "#{definition.name} v#{definition.version} changed; bump version"
        rescue JSON::ParserError, TypeError
          raise DefinitionChangedError, "#{definition.name} v#{definition.version} stored definition is invalid"
        end

        def cleanup_rollups(before, event_filter)
          keys = scan_keys(namespace_glob("#{namespace}:rollup:*")).select do |key|
            rollup_key_old?(key, before, event_filter)
          end
          delete_keys(keys)
          keys.length
        end

        def cleanup_interval_state(before, event_filter)
          before_ms = (before.to_f * 1000).to_i
          keys = scan_keys(namespace_glob("#{namespace}:state:*")).select do |key|
            state_key_matches_event?(key, event_filter)
          end
          return 0 if keys.empty?

          values = redis.pipelined { |pipe| keys.each { |key| pipe.get(key) } }
          expired = keys.zip(values).filter_map do |key, value|
            key if state_key_old?(key, before_ms, event_filter, value)
          end

          delete_keys(expired)
          expired.length
        end

        def cleanup_processed_entries(before, event_filter)
          keys = scan_keys(namespace_glob("#{namespace}:processed:*")).select do |key|
            processed_key_matches_event?(key, event_filter)
          end
          return 0 if keys.empty?

          values = redis.pipelined { |pipe| keys.each { |key| pipe.get(key) } }
          expired = keys.zip(values).filter_map do |key, value|
            key if processed_entry_old?(value, before)
          end

          delete_keys(expired)
          expired.length
        end

        def processed_key_matches_event?(key, event_filter)
          prefix = "#{namespace}:processed:"
          return false unless key.start_with?(prefix)

          event_name = key.delete_prefix(prefix).split(":", 2).first
          event_name && (!event_filter || event_filter.include?(event_name))
        end

        def state_key_matches_event?(key, event_filter)
          prefix = "#{namespace}:state:"
          return false unless key.start_with?(prefix)

          event_name = key.delete_prefix(prefix).split(":", 3).first
          event_name && (!event_filter || event_filter.include?(event_name))
        end

        def processed_entry_old?(timestamp, before)
          Time.parse(timestamp).utc < before
        rescue ArgumentError, TypeError, RangeError
          true
        end

        def scan_keys(pattern)
          keys = []
          redis.scan_each(match: pattern) { |key| keys << key }
          keys
        end

        def delete_keys(keys)
          keys.each_slice(500) do |slice|
            redis.del(*slice)
          end
        end

        def namespace_glob(pattern)
          prefix = "#{namespace}:"
          return pattern unless pattern.start_with?(prefix)

          "#{glob_escape(namespace)}:#{pattern.delete_prefix(prefix)}"
        end

        def glob_escape(value)
          value.to_s.gsub(/[\\*\?\[\]]/) { |character| "\\#{character}" }
        end

        def key_matches?(key, pattern)
          prefix = "#{namespace}:"
          return ::File.fnmatch?(pattern, key) unless pattern.start_with?(prefix)
          return false unless key.start_with?(prefix)

          ::File.fnmatch?(pattern.delete_prefix(prefix), key.delete_prefix(prefix))
        end
      end
    end
  end
end

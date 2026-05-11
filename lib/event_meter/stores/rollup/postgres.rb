require "digest"
require "json"
require "monitor"
require "securerandom"
require "time"

require_relative "../../errors"
require_relative "../../rollup"
require_relative "../cleanup_helpers"
require_relative "../lock_refresher"
require_relative "../namespace"

module EventMeter
  module Stores
    module Rollup
      class Postgres
        include CleanupHelpers
        include Namespace

        MAX_QUERY_PARAMS = 1_000
        SCOPED_QUERY_PARAMS = 3
        MAX_ENTRY_ID_QUERY_PARAMS = MAX_QUERY_PARAMS - SCOPED_QUERY_PARAMS
        KEY_VALUE_PARAM_COUNT = 2
        MAX_KEY_VALUE_ROWS = MAX_QUERY_PARAMS / KEY_VALUE_PARAM_COUNT
        LOCK_ID_MASK = 0x7fff_ffff_ffff_ffff
        LOCK_REFRESH_RATIO = 2.0
        BIGINT_MAX = "9223372036854775807"
        BIGINT_MIN_ABS = "9223372036854775808"
        BIGINT_DIGITS = 19

        attr_reader :connection, :connection_lock, :namespace, :table_prefix,
          :report_name, :version, :lock_scope

        def self.schema_sql(table_prefix: "event_meter")
          validate_identifier!(table_prefix)

          <<~SQL
            CREATE TABLE IF NOT EXISTS #{table_prefix}_rollups (
              key text PRIMARY KEY,
              fields jsonb NOT NULL DEFAULT '{}'::jsonb,
              updated_at timestamptz NOT NULL DEFAULT now()
            );

            CREATE TABLE IF NOT EXISTS #{table_prefix}_strings (
              key text PRIMARY KEY,
              value text NOT NULL,
              updated_at timestamptz NOT NULL DEFAULT now()
            );

            CREATE TABLE IF NOT EXISTS #{table_prefix}_processed_entries (
              namespace text NOT NULL,
              event_name text NOT NULL,
              version integer NOT NULL,
              entry_id text NOT NULL,
              created_at timestamptz NOT NULL DEFAULT now(),
              PRIMARY KEY (namespace, event_name, version, entry_id)
            );

            CREATE INDEX IF NOT EXISTS #{table_prefix}_processed_created_at_idx
            ON #{table_prefix}_processed_entries (created_at);

            CREATE INDEX IF NOT EXISTS #{table_prefix}_rollups_key_prefix_idx
            ON #{table_prefix}_rollups (key text_pattern_ops);

            CREATE INDEX IF NOT EXISTS #{table_prefix}_strings_key_prefix_idx
            ON #{table_prefix}_strings (key text_pattern_ops);
          SQL
        end

        def self.install!(connection:, table_prefix: "event_meter")
          connection.exec(schema_sql(table_prefix: table_prefix))
        end

        def initialize(connection:, namespace:, table_prefix: "event_meter", report_name: nil, version: nil,
          lock_scope: nil, connection_lock: nil, lock_connection: nil, lock_connection_lock: nil)
          self.class.validate_identifier!(table_prefix)

          @connection = connection
          @connection_lock = connection_lock || Monitor.new
          @lock_connection = lock_connection
          @lock_connection_lock = lock_connection_lock
          @namespace = normalize_namespace(namespace)
          @table_prefix = table_prefix
          @report_name = report_name&.to_s
          @version = version&.to_i
          @lock_scope = lock_scope
        end

        def for_report(name:, version:)
          name = name.to_s
          version = version.to_i

          self.class.new(
            connection: connection,
            connection_lock: connection_lock,
            lock_connection: @lock_connection,
            lock_connection_lock: @lock_connection_lock,
            namespace: namespace,
            table_prefix: table_prefix,
            report_name: name,
            version: version,
            lock_scope: "#{Keys.event_name(name)}:#{Keys.version_key(version)}"
          )
        end

        def ensure_definition(definition)
          key = definition_key(definition.name, definition.version)
          payload = JSON.generate(definition.to_h)

          transaction do
            row = exec_params("SELECT value FROM #{strings_table} WHERE key = $1 FOR UPDATE", [key]).first
            if row
              ensure_same_definition!(row.fetch("value"), definition)
            else
              insert_string_once(key, payload)
              stored = exec_params("SELECT value FROM #{strings_table} WHERE key = $1 FOR UPDATE", [key]).first
              raise DefinitionChangedError, "#{definition.name} v#{definition.version} definition was not stored" unless stored

              ensure_same_definition!(stored.fetch("value"), definition)
            end
          end
        end

        def report_definition(name:, version:)
          row = exec_params("SELECT value FROM #{strings_table} WHERE key = $1", [definition_key(name, version)]).first
          row && JSON.parse(row.fetch("value"))
        rescue JSON::ParserError, TypeError
          nil
        end

        def processed_ids(ids)
          ensure_scoped!
          return [] if ids.empty?

          rows = ids.each_slice(MAX_ENTRY_ID_QUERY_PARAMS).flat_map do |slice|
            exec_params(
              <<~SQL,
                SELECT entry_id
                FROM #{processed_table}
                WHERE namespace = $1
                  AND event_name = $2
                  AND version = $3
                  AND entry_id IN (#{placeholders(slice, start: 4)})
              SQL
              scoped_params + slice
            )
          end
          rows.map { |row| row.fetch("entry_id") }
        end

        def forget_processed_ids(ids)
          ensure_scoped!
          delete_processed_entries(ids)
        end

        def apply(batch)
          ensure_scoped!

          transaction do
            merge_rollups(batch.rollups)
            upsert_max_strings(batch.state_updates)
            mark_processed_entries(batch.entry_ids)
          end
        end

        def hgetall_many(keys)
          return [] if keys.empty?

          rows = keys.each_slice(MAX_QUERY_PARAMS).flat_map do |slice|
            exec_params(
              "SELECT key, fields::text AS fields FROM #{rollups_table} WHERE key IN (#{placeholders(slice)})",
              slice
            )
          end
          by_key = rows.to_h { |row| [row.fetch("key"), parse_hash(row.fetch("fields"))] }
          keys.map { |key| by_key.fetch(key, {}) }
        end

        def keys_matching(pattern, limit: nil)
          limit = positive_integer(limit, "limit") if limit
          rows = exec_params(
            "SELECT key FROM #{rollups_table} WHERE key LIKE $1 ESCAPE '\\' ORDER BY key",
            [like_prefix_for_pattern(pattern)]
          )

          keys = rows.map { |row| row.fetch("key") }.select { |key| key_matches?(key, pattern) }
          limit ? keys.first(limit) : keys
        end

        def get(key)
          row = exec_params("SELECT value FROM #{strings_table} WHERE key = $1", [key]).first
          row&.fetch("value")
        end

        def cleanup_watermark(key)
          get(key)
        end

        def write_cleanup_watermark(key, value)
          upsert_string(key, value)
        end

        def with_lock(ttl:)
          ttl = positive_integer(ttl, "lock ttl")
          ensure_independent_lock_connection!
          token = SecureRandom.hex(16)
          locked = acquire_lock_lease(token, ttl)
          return false unless locked

          refresher = start_lock_refresher(token, ttl)
          yield
          true
        ensure
          stop_lock_refresher(refresher)
          release_lock_lease(token) if locked
        end

        def cleanup_history(before:, events:, interval_state:)
          transaction do
            filter = event_filter(events)
            {
              rollup_keys_deleted: cleanup_rollups(before, filter),
              interval_state_keys_deleted: interval_state ? cleanup_interval_state(before, filter) : 0,
              processed_entries_deleted: cleanup_processed_entries(before, filter)
            }
          end
        end

        def self.validate_identifier!(value)
          return if value.to_s.match?(/\A[a-zA-Z_][a-zA-Z0-9_]*\z/)

          raise ArgumentError, "invalid PostgreSQL identifier: #{value.inspect}"
        end

        private

        def positive_integer(value, name)
          integer = Integer(value)
          return integer if integer.positive?

          raise ArgumentError, "#{name} must be positive"
        rescue ArgumentError, TypeError, RangeError
          raise ArgumentError, "#{name} must be positive"
        end

        def lock_connection
          @lock_connection ||= default_lock_connection
        end

        def lock_connection_lock
          @lock_connection_lock ||= default_lock_connection_lock
        end

        def default_lock_connection
          cloned = clone_connection(connection)
          return cloned if cloned

          raise ConfigurationError, "postgres rollup storage requires a separate lock_connection"
        end

        def clone_connection(source)
          return unless source.respond_to?(:conninfo_hash) && source.class.respond_to?(:connect)

          source.class.connect(source.conninfo_hash.compact)
        rescue StandardError
          nil
        end

        def default_lock_connection_lock
          lock_connection.equal?(connection) ? connection_lock : Monitor.new
        end

        def ensure_independent_lock_connection!
          return unless lock_connection.equal?(connection)

          raise ConfigurationError, "postgres rollup storage lock_connection must be separate from connection"
        end

        def acquire_lock_lease(token, ttl)
          now_ms = current_time_ms
          value = lock_lease_value(token, now_ms + ttl * 1000)
          rows = lock_exec_params(<<~SQL, [lock_key, value, now_ms])
            INSERT INTO #{strings_table} (key, value, updated_at)
            VALUES ($1, $2, now())
            ON CONFLICT (key)
            DO UPDATE SET value = EXCLUDED.value, updated_at = now()
            WHERE #{expired_lock_value_sql("#{strings_table}.value", "$3")}
            RETURNING value
          SQL

          rows.any? { |row| lock_lease_token(row.fetch("value")) == token }
        end

        def refresh_lock_lease(token, ttl)
          expires_ms = current_time_ms + ttl * 1000
          rows = lock_exec_params(<<~SQL, [lock_key, lock_lease_value(token, expires_ms), token])
            UPDATE #{strings_table}
            SET value = $2, updated_at = now()
            WHERE key = $1
              AND split_part(value, ':', 2) = $3
            RETURNING value
          SQL

          rows.any?
        end

        def release_lock_lease(token)
          lock_exec_params(<<~SQL, [lock_key, token])
            DELETE FROM #{strings_table}
            WHERE key = $1
              AND split_part(value, ':', 2) = $2
          SQL
        end

        def start_lock_refresher(token, ttl)
          LockRefresher.new(
            interval: ttl.to_f / LOCK_REFRESH_RATIO,
            refresh: -> { refresh_lock_lease(token, ttl) },
            failure_message: "postgres lock refresh failed",
            thread_name: "event_meter postgres lock refresher"
          ).start
        end

        def stop_lock_refresher(refresher)
          return unless refresher

          refresher.stop
        end

        def lock_lease_value(token, expires_ms)
          "#{expires_ms}:#{token}"
        end

        def lock_lease_token(value)
          value.to_s.split(":", 2).fetch(1, nil)
        end

        def expired_lock_value_sql(value_sql, now_param)
          expires_sql = "split_part(#{value_sql}, ':', 1)"
          "#{safe_bigint(expires_sql)} IS NULL OR #{safe_bigint(expires_sql)} <= #{now_param}"
        end

        def lock_key
          [namespace, "process_lock", lock_scope].compact.join(":")
        end

        def current_time_ms
          (Time.now.utc.to_f * 1000).to_i
        end

        def cleanup_rollups(before, event_filter)
          cleanup_event_filter_slices(event_filter, reserved_params: 4).sum do |filter|
            cleanup_rollup_slice(before, filter)
          end
        end

        def cleanup_interval_state(before, event_filter)
          cleanup_event_filter_slices(event_filter, reserved_params: 3).sum do |filter|
            cleanup_interval_state_slice(before, filter)
          end
        end

        def cleanup_processed_entries(before, event_filter)
          if event_filter
            return 0 if event_filter.empty?

            return delete_filtered_processed_entries(before, event_filter)
          end

          rows = exec_params(<<~SQL, [namespace, before.iso8601])
            WITH deleted AS (
              DELETE FROM #{processed_table}
              WHERE namespace = $1
                AND created_at < $2::timestamptz
              RETURNING entry_id
            )
            SELECT count(*) AS count FROM deleted
          SQL

          rows.first.fetch("count").to_i
        end

        def cleanup_rollup_slice(before, event_filter)
          prefix = "#{namespace}:rollup:"
          suffix = key_suffix_sql("$1")
          params = [
            prefix,
            like_prefix(prefix),
            TimeBuckets.id(before, :minute),
            TimeBuckets.id(before, :hour)
          ]
          event_clause, event_params = event_filter_sql(suffix, event_filter, start: 5)

          count_deleted_rows(rollups_table, <<~SQL, params + event_params)
            key LIKE $2 ESCAPE '\\'
              AND #{event_clause}
              AND (
                (split_part(#{suffix}, ':', 3) = 'minute' AND split_part(#{suffix}, ':', 4) < $3)
                OR
                (split_part(#{suffix}, ':', 3) = 'hour' AND split_part(#{suffix}, ':', 4) < $4)
              )
          SQL
        end

        def cleanup_interval_state_slice(before, event_filter)
          prefix = "#{namespace}:state:"
          before_ms = (before.to_f * 1000).to_i

          if event_filter
            suffix = key_suffix_sql("$1")
            event_clause, event_params = event_filter_sql(suffix, event_filter, start: 4)
            params = [prefix, like_prefix(prefix), before_ms] + event_params
            like_param = "$2"
            before_param = "$3"
          else
            event_clause = "TRUE"
            params = [like_prefix(prefix), before_ms]
            like_param = "$1"
            before_param = "$2"
          end

          count_deleted_rows(strings_table, <<~SQL, params)
            key LIKE #{like_param} ESCAPE '\\'
              AND #{event_clause}
              AND (#{safe_bigint("value")} IS NULL OR #{safe_bigint("value")} < #{before_param})
          SQL
        end

        def count_deleted_rows(table_name, where_sql, params)
          rows = exec_params(<<~SQL, params)
            WITH deleted AS (
              DELETE FROM #{table_name}
              WHERE #{where_sql}
              RETURNING key
            )
            SELECT count(*) AS count FROM deleted
          SQL

          rows.first&.fetch("count", 0).to_i
        end

        def cleanup_event_filter_slices(event_filter, reserved_params:)
          return [nil] unless event_filter

          event_filter.each_slice(MAX_QUERY_PARAMS - reserved_params).to_a
        end

        def event_filter_sql(suffix_sql, event_filter, start:)
          return ["TRUE", []] unless event_filter

          [
            "split_part(#{suffix_sql}, ':', 1) IN (#{placeholders(event_filter, start: start)})",
            event_filter
          ]
        end

        def key_suffix_sql(prefix_param)
          "substring(key from char_length(#{prefix_param}::text) + 1)"
        end

        def merge_rollups(rollups)
          rollups.each_slice(MAX_KEY_VALUE_ROWS) do |slice|
            merge_rollup_rows(slice)
          end
        end

        def merge_rollup_rows(rows)
          return if rows.empty?

          params = rows.flat_map { |key, rollup| [key, rollup_json(rollup)] }

          exec_params(<<~SQL, params)
            INSERT INTO #{rollups_table} (key, fields, updated_at)
            VALUES #{key_value_rows_sql(rows.length, value_cast: "jsonb")}
            ON CONFLICT (key)
            DO UPDATE SET fields = #{merged_rollup_fields_sql}, updated_at = now()
          SQL
        end

        def upsert_max_strings(values_by_key)
          values_by_key.each_slice(MAX_KEY_VALUE_ROWS) do |slice|
            upsert_max_string_rows(slice)
          end
        end

        def upsert_max_string_rows(rows)
          return if rows.empty?

          params = rows.flat_map { |key, value| [key, value.to_s] }

          exec_params(<<~SQL, params)
            INSERT INTO #{strings_table} (key, value, updated_at)
            VALUES #{key_value_rows_sql(rows.length)}
            ON CONFLICT (key)
            DO UPDATE SET
              value = COALESCE(
                GREATEST(
                  #{safe_bigint("#{strings_table}.value")},
                  #{safe_bigint("EXCLUDED.value")}
                ),
                #{safe_bigint("#{strings_table}.value")},
                #{safe_bigint("EXCLUDED.value")}
              )::text,
              updated_at = now()
          SQL
        end

        def mark_processed_entries(ids)
          ids.each_slice(MAX_ENTRY_ID_QUERY_PARAMS) do |slice|
            exec_params(<<~SQL, scoped_params + slice)
              INSERT INTO #{processed_table} (namespace, event_name, version, entry_id, created_at)
              SELECT $1, $2, $3, entry_id, now()
              FROM (VALUES #{single_column_rows_sql(slice.length, start: 4)}) AS entries(entry_id)
              ON CONFLICT (namespace, event_name, version, entry_id) DO NOTHING
            SQL
          end
        end

        def rollup_json(rollup)
          JSON.generate(rollup.fields.transform_values(&:to_s))
        end

        def upsert_string(key, value)
          exec_params(<<~SQL, [key, value])
            INSERT INTO #{strings_table} (key, value, updated_at)
            VALUES ($1, $2, now())
            ON CONFLICT (key)
            DO UPDATE SET value = EXCLUDED.value, updated_at = now()
          SQL
        end

        def insert_string_once(key, value)
          exec_params(<<~SQL, [key, value])
            INSERT INTO #{strings_table} (key, value, updated_at)
            VALUES ($1, $2, now())
            ON CONFLICT (key) DO NOTHING
          SQL
        end

        def delete_processed_entries(ids)
          return if ids.empty?

          ids.each_slice(MAX_ENTRY_ID_QUERY_PARAMS) do |slice|
            exec_params(<<~SQL, scoped_params + slice)
              DELETE FROM #{processed_table}
              WHERE namespace = $1
                AND event_name = $2
                AND version = $3
                AND entry_id IN (#{placeholders(slice, start: 4)})
            SQL
          end
        end

        def delete_filtered_processed_entries(before, event_filter)
          rows = event_filter.each_slice(MAX_QUERY_PARAMS - 2).flat_map do |slice|
            exec_params(<<~SQL, [namespace, before.iso8601] + slice)
              WITH deleted AS (
                DELETE FROM #{processed_table}
                WHERE namespace = $1
                  AND created_at < $2::timestamptz
                  AND event_name IN (#{placeholders(slice, start: 3)})
                RETURNING entry_id
              )
              SELECT count(*) AS count FROM deleted
            SQL
          end

          rows.sum { |row| row.fetch("count").to_i }
        end

        def delete_by_key(table_name, keys)
          return if keys.empty?

          keys.each_slice(MAX_QUERY_PARAMS) do |slice|
            exec_params("DELETE FROM #{table_name} WHERE key IN (#{placeholders(slice)})", slice)
          end
        end

        def transaction
          connection_lock.synchronize do
            exec("BEGIN")
            result = yield
            exec("COMMIT")
            result
          rescue StandardError
            exec("ROLLBACK") rescue nil
            raise
          end
        end

        def advisory_lock
          row = exec_params("SELECT pg_try_advisory_lock($1::bigint) AS locked", [lock_id]).first
          truthy?(row.fetch("locked"))
        end

        def advisory_unlock
          exec_params("SELECT pg_advisory_unlock($1::bigint)", [lock_id])
        end

        def exec(sql)
          connection_lock.synchronize do
            result = connection.exec(sql)
            result.respond_to?(:to_a) ? result.to_a : []
          end
        end

        def exec_params(sql, params)
          connection_lock.synchronize do
            result = connection.exec_params(sql, params)
            result.respond_to?(:to_a) ? result.to_a : []
          end
        end

        def lock_exec_params(sql, params)
          lock_connection_lock.synchronize do
            result = lock_connection.exec_params(sql, params)
            result.respond_to?(:to_a) ? result.to_a : []
          end
        end

        def placeholders(values, start: 1)
          values.each_index.map { |index| "$#{index + start}" }.join(", ")
        end

        def key_value_rows_sql(count, value_cast: nil)
          count.times.map do |index|
            key = "$#{index * KEY_VALUE_PARAM_COUNT + 1}"
            value = "$#{index * KEY_VALUE_PARAM_COUNT + 2}"
            value = "#{value}::#{value_cast}" if value_cast

            "(#{key}, #{value}, now())"
          end.join(", ")
        end

        def single_column_rows_sql(count, start:)
          count.times.map { |index| "($#{index + start})" }.join(", ")
        end

        def merged_rollup_fields_sql
          <<~SQL
            (
              SELECT COALESCE(jsonb_object_agg(field, value), '{}'::jsonb)
              FROM (
                SELECT
                  field,
                  #{merged_rollup_value_sql} AS value
                FROM (#{rollup_number_rows_sql}) numbers
              ) merged
            )
          SQL
        end

        def rollup_number_rows_sql
          <<~SQL
            SELECT
              COALESCE(existing.key, incoming.key) AS field,
              #{safe_bigint("existing.value")} AS existing_value,
              #{safe_bigint("incoming.value")} AS incoming_value
            FROM jsonb_each_text(#{safe_json_object("#{rollups_table}.fields")}) AS existing(key, value)
            FULL OUTER JOIN jsonb_each_text(#{safe_json_object("EXCLUDED.fields")}) AS incoming(key, value)
              USING (key)
          SQL
        end

        def merged_rollup_value_sql
          <<~SQL
            CASE
              WHEN field IN (#{rollup_min_fields_sql}) THEN #{rollup_min_value_sql}
              WHEN field IN (#{rollup_max_fields_sql}) THEN #{rollup_max_value_sql}
              ELSE #{rollup_sum_value_sql}
            END
          SQL
        end

        def rollup_min_fields_sql
          quoted_sql_strings(EventMeter::Rollup::MIN_FIELDS)
        end

        def rollup_max_fields_sql
          quoted_sql_strings(EventMeter::Rollup::MAX_FIELDS)
        end

        def rollup_min_value_sql
          "COALESCE(LEAST(existing_value, incoming_value), existing_value, incoming_value, 0)::text"
        end

        def rollup_max_value_sql
          "COALESCE(GREATEST(existing_value, incoming_value), existing_value, incoming_value, 0)::text"
        end

        def rollup_sum_value_sql
          "(COALESCE(existing_value, 0) + COALESCE(incoming_value, 0))::text"
        end

        def safe_bigint(sql)
          digits = safe_bigint_digits(sql)
          negative = "left(#{sql}, 1) = '-'"
          bounded = <<~SQL
            (
              char_length(#{digits}) < #{BIGINT_DIGITS}
              OR (
                char_length(#{digits}) = #{BIGINT_DIGITS}
                AND (
                  (#{negative} AND #{digits} <= '#{BIGINT_MIN_ABS}')
                  OR ((#{negative}) IS NOT TRUE AND #{digits} <= '#{BIGINT_MAX}')
                )
              )
            )
          SQL
          value = "((CASE WHEN #{negative} AND #{digits} <> '0' THEN '-' ELSE '' END) || #{digits})"

          "(CASE WHEN #{sql} ~ '^-?[0-9]+$' AND #{bounded} THEN #{value}::bigint END)"
        end

        def safe_bigint_digits(sql)
          "COALESCE(NULLIF(regexp_replace(ltrim(#{sql}, '-'), '^0+', ''), ''), '0')"
        end

        def safe_json_object(sql)
          "CASE WHEN jsonb_typeof(#{sql}) = 'object' THEN #{sql} ELSE '{}'::jsonb END"
        end

        def quoted_sql_strings(values)
          values.map { |value| "'#{value.to_s.gsub("'", "''")}'" }.join(", ")
        end

        def parse_json(value)
          value.is_a?(String) ? JSON.parse(value) : value
        rescue JSON::ParserError, TypeError
          nil
        end

        def parse_hash(value)
          parsed = parse_json(value)
          parsed.is_a?(Hash) ? parsed : {}
        end

        def truthy?(value)
          value == true || value == "t" || value == "true" || value == "1"
        end

        def prefix_before_wildcard(pattern)
          pattern.split("*", 2).first
        end

        def like_prefix_for_pattern(pattern)
          prefix = "#{namespace}:"
          return like_prefix(prefix_before_wildcard(pattern)) unless pattern.start_with?(prefix)

          "#{escape_like(namespace)}:#{like_prefix(prefix_before_wildcard(pattern.delete_prefix(prefix)))}"
        end

        def like_prefix(value)
          "#{escape_like(value)}%"
        end

        def escape_like(value)
          value.to_s.gsub(/[\\%_]/) { |character| "\\#{character}" }
        end

        def key_matches?(key, pattern)
          prefix = "#{namespace}:"
          return ::File.fnmatch?(pattern, key) unless pattern.start_with?(prefix)
          return false unless key.start_with?(prefix)

          ::File.fnmatch?(pattern.delete_prefix(prefix), key.delete_prefix(prefix))
        end

        def lock_id
          @lock_id ||= begin
            digest = Digest::SHA256.hexdigest("#{table_prefix}:#{namespace}:process_lock:#{lock_scope}")
            digest[0, 16].to_i(16) & LOCK_ID_MASK
          end
        end

        def definition_key(name, version)
          Keys.definition(namespace: namespace, name: name, version: version)
        end

        def scoped_params
          [namespace, Keys.event_name(report_name), version]
        end

        def ensure_scoped!
          return if report_name && version&.positive?

          raise ConfigurationError, "postgres rollup storage must be scoped with for_report"
        end

        def ensure_same_definition!(stored, definition)
          stored_definition = ReportDefinition.from_h(JSON.parse(stored))
          return if stored_definition.fingerprint == definition.fingerprint

          raise DefinitionChangedError, "#{definition.name} v#{definition.version} changed; bump version"
        rescue JSON::ParserError, TypeError
          raise DefinitionChangedError, "#{definition.name} v#{definition.version} stored definition is invalid"
        end

        def rollups_table
          "#{table_prefix}_rollups"
        end

        def strings_table
          "#{table_prefix}_strings"
        end

        def processed_table
          "#{table_prefix}_processed_entries"
        end
      end
    end
  end
end

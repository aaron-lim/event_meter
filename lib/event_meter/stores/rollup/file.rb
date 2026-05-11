require "digest"
require "fileutils"
require "json"
require "time"

require_relative "../../rollup"
require_relative "../cleanup_helpers"
require_relative "../file_helpers"
require_relative "../namespace"

module EventMeter
  module Stores
    module Rollup
      class File
        include CleanupHelpers
        include FileHelpers
        include Namespace

        APPLIED_KEY = "_applied"
        BATCHES_KEY = "batches"
        PROCESSED_IDS_KEY = "processed_ids"
        STREAM_FILE_KEY = "stream_file"

        attr_reader :path, :namespace, :report_name, :version

        def initialize(path:, namespace: nil, report_name: nil, version: nil)
          @path = normalize_file_store_path(path)
          @namespace = normalize_namespace(namespace) if namespace
          @report_name = report_name
          @version = version

          FileUtils.mkdir_p(rollup_path) if scoped?
        end

        def for_namespace(namespace)
          namespace = normalize_namespace(namespace)
          return self if self.namespace == namespace

          if self.namespace
            raise ConfigurationError, "file rollup storage namespace #{self.namespace.inspect} does not match #{namespace.inspect}"
          end

          @namespace = namespace
          FileUtils.mkdir_p(rollup_path) if scoped?
          self
        end

        def for_report(name:, version:)
          self.class.new(
            path: path,
            namespace: namespace,
            report_name: name.to_s,
            version: version
          )
        end

        def ensure_definition(definition)
          update_json_file(definition_path, {}) do |stored|
            if stored.empty?
              stored.merge!(definition.to_h)
            else
              ensure_same_definition!(stored, definition)
            end
          end
        end

        def report_definition(name:, version:)
          return nil unless scoped_for?(name, version)

          hash_value(read_json_file(definition_path))
        end

        def processed_ids(ids)
          ensure_scoped!

          ids.select do |id|
            processed_sidecar_for(id).processed?(id)
          end
        end

        def apply(batch)
          ensure_scoped!
          return if batch.empty?

          batch_id = transaction_id(batch.entry_ids)
          applied_paths = []
          applied_paths.concat(apply_rollups(batch_id, batch))
          applied_paths.concat(apply_string_updates(batch_id, batch))
          mark_processed_entries(batch, batch_id, applied_paths.uniq)
        end

        def forget_processed_ids(ids)
          ensure_scoped!

          sidecars = ids.map { |id| processed_sidecar_for(id) }.uniq(&:path)
          applied_paths_by_batch = {}

          sidecars.each do |sidecar|
            sidecar.batch_paths.each do |batch_id, paths|
              applied_paths_by_batch[batch_id] ||= []
              applied_paths_by_batch[batch_id].concat(paths)
            end

            sidecar.delete
          end

          applied_paths_by_batch.each do |batch_id, relative_paths|
            forget_applied_marker(batch_id, relative_paths.uniq)
          end
        end

        def hgetall_many(keys)
          ensure_scoped!

          keys.map do |key|
            rollup = rollup_key_parts(key)
            next {} unless rollup

            data = read_json_file(rollup_bucket_path(rollup.fetch(:every), rollup.fetch(:bucket)))
            hash_value(data[rollup.fetch(:index)]).dup
          end
        end

        def keys_matching(pattern, limit: nil)
          ensure_scoped!

          keys = rollup_keys.select { |key| key_matches?(key, pattern) }.sort
          limit ? keys.first(positive_integer(limit, "limit")) : keys
        end

        def get(key)
          ensure_scoped!

          read_json_file(shard_path("strings", key))[key]
        end

        def cleanup_watermark(key)
          read_json_file(cleanup_state_path)[key]
        end

        def write_cleanup_watermark(key, value)
          update_json_file(cleanup_state_path, {}) do |data|
            data[key] = value.to_s
          end
        end

        def with_lock(ttl:)
          FileUtils.mkdir_p(::File.dirname(lock_path))

          ::File.open(lock_path, ::File::RDWR | ::File::CREAT, 0o600) do |file|
            return false unless file.flock(::File::LOCK_EX | ::File::LOCK_NB)

            yield
            true
          ensure
            file&.flock(::File::LOCK_UN)
          end
        end

        def cleanup_history(before:, events:, interval_state:)
          ensure_namespace!
          return cleanup_all_report_histories(before: before, events: events, interval_state: interval_state) unless scoped?

          cleanup_scoped_history(before: before, events: events, interval_state: interval_state)
        end

        private

        def apply_rollups(batch_id, batch)
          batch.rollups.group_by { |key, _rollup| rollup_bucket_file_for_key(key) }.filter_map do |file, entries|
            next unless file

            apply_once(file, batch_id) do |data|
              entries.each do |key, rollup|
                parts = rollup_key_parts(key)
                next unless parts

                index = parts.fetch(:index)
                merged = EventMeter::Rollup.from_hash(hash_value(data[index])).merge!(rollup)
                data[index] = merged.fields.transform_values(&:to_s)
              end
            end
          end
        end

        def apply_string_updates(batch_id, batch)
          batch.state_updates.group_by { |key, _value| shard_path("strings", key) }.map do |file, entries|
            apply_once(file, batch_id) do |data|
              entries.each do |key, value|
                data[key] = [integer_value(data[key]), integer_value(value)].compact.max.to_s
              end
            end
          end
        end

        def apply_once(file, batch_id)
          update_json_file(file, {}) do |data|
            applied = applied_hash(data)
            next if applied.key?(batch_id)

            yield data
            applied[batch_id] = current_time.iso8601(6)
            data[APPLIED_KEY] = applied
          end

          relative_path(file)
        end

        def mark_processed_entries(batch, batch_id, applied_paths)
          timestamp = current_time.iso8601(6)

          batch.entry_ids.group_by { |id| processed_sidecar_for(id) }.each do |sidecar, ids|
            sidecar.mark(ids, batch_id: batch_id, applied_paths: applied_paths, timestamp: timestamp)
          end
        end

        def forget_applied_marker(batch_id, relative_paths)
          relative_paths.each do |relative_path|
            file = absolute_rollup_file(relative_path)
            next unless file.start_with?("#{rollup_path}/")
            next unless ::File.exist?(file)

            update_json_file(file, {}) do |data|
              applied = applied_hash(data)
              applied.delete(batch_id)

              if applied.empty?
                data.delete(APPLIED_KEY)
              else
                data[APPLIED_KEY] = applied
              end
            end
          end
        end

        def cleanup_scoped_history(before:, events:, interval_state:)
          filter = event_filter(events)
          result = {
            rollup_keys_deleted: cleanup_rollups(before, filter),
            interval_state_keys_deleted: interval_state ? cleanup_interval_state(before, filter) : 0,
            processed_entries_deleted: cleanup_processed_sidecars(before, filter)
          }
          cleanup_old_applied_markers(before)

          result
        end

        def cleanup_all_report_histories(before:, events:, interval_state:)
          definition_files.each_with_object(empty_cleanup_result) do |definition_file, total|
            definition = read_json_file(definition_file)
            next if definition.empty?

            result = self.class.new(
              path: path,
              namespace: namespace,
              report_name: definition.fetch("name"),
              version: definition.fetch("version")
            ).cleanup_history(before: before, events: events, interval_state: interval_state)

            merge_cleanup_result(total, result)
          rescue KeyError, ArgumentError, TypeError
            total
          end
        end

        def cleanup_rollups(before, event_filter)
          return 0 if filtered_out?(event_filter)

          deleted = 0

          rollup_bucket_files.each do |file|
            next unless rollup_file_old?(file, before)

            data = read_json_file(file)
            deleted += data.keys.reject { |key| metadata_key?(key) }.length
            FileUtils.rm_f(file)
            FileUtils.rm_f(lock_file_path(file))
          end

          deleted
        end

        def cleanup_interval_state(before, event_filter)
          before_ms = (before.to_f * 1000).to_i
          deleted = 0

          shard_files("strings").each do |file|
            update_json_file(file, {}) do |data|
              data.keys.grep(/\A#{Regexp.escape(namespace)}:state:/).each do |key|
                if state_key_old?(key, before_ms, event_filter, data[key])
                  data.delete(key)
                  deleted += 1
                end
              end
            end
          end

          deleted
        end

        def cleanup_processed_sidecars(before, event_filter)
          return 0 if filtered_out?(event_filter)

          sidecar_files.sum do |file|
            sidecar = ProcessedSidecar.new(path: file)
            next 0 unless sidecar.old?(before)

            count = sidecar.processed_count
            sidecar.delete
            count
          end
        end

        def cleanup_old_applied_markers(before)
          each_data_file do |file|
            update_json_file(file, {}) do |data|
              applied = applied_hash(data)
              applied.delete_if { |_batch_id, timestamp| processed_entry_old?(timestamp, before) }

              if applied.empty?
                data.delete(APPLIED_KEY)
              else
                data[APPLIED_KEY] = applied
              end
            end
          end
        end

        def processed_entry_old?(timestamp, before)
          Time.parse(timestamp).utc < before
        rescue ArgumentError, TypeError, RangeError
          true
        end

        def rollup_keys
          rollup_bucket_files.flat_map do |file|
            parts = rollup_file_parts(file)
            next [] unless parts

            read_json_file(file).keys.filter_map do |index|
              next if metadata_key?(index)

              Keys.rollup(
                namespace: namespace,
                name: report_name,
                version: version,
                every: parts.fetch(:every),
                bucket: parts.fetch(:time),
                index: IndexStruct.new(index)
              )
            end
          end
        end

        def rollup_file_old?(file, before)
          parts = rollup_file_parts(file)
          return false unless parts

          bucket_end_time(parts.fetch(:every).to_s, parts.fetch(:bucket)) <= before
        rescue ArgumentError, TypeError
          false
        end

        def rollup_file_parts(file)
          relative = relative_path(file)
          match = relative.match(%r{\Ahashes/(minute|hour)/(\d+)\.json\z})
          return unless match

          every = match[1].to_sym
          bucket = match[2]

          {
            every: every,
            bucket: bucket,
            time: bucket_time(every, bucket)
          }
        end

        def bucket_time(every, bucket)
          case every.to_s
          when "minute"
            minute_bucket_time(bucket)
          when "hour"
            hour_bucket_time(bucket)
          else
            raise ArgumentError, "unsupported rollup bucket: #{every.inspect}"
          end
        end

        def rollup_bucket_file_for_key(key)
          parts = rollup_key_parts(key)
          return unless parts

          rollup_bucket_path(parts.fetch(:every), parts.fetch(:bucket))
        end

        def rollup_key_parts(key)
          prefix = "#{namespace}:rollup:"
          return unless key.start_with?(prefix)

          event_name, version_key, every, bucket, index = key.delete_prefix(prefix).split(":", 5)
          return unless event_name == Keys.event_name(report_name)
          return unless version_key == Keys.version_key(version)
          return unless %w[minute hour].include?(every)
          return unless bucket && index

          {
            every: every.to_sym,
            bucket: bucket,
            index: index
          }
        end

        def update_json_file(file, default)
          FileUtils.mkdir_p(::File.dirname(file))

          ::File.open(lock_file_path(file), ::File::RDWR | ::File::CREAT, 0o600) do |lock|
            lock.flock(::File::LOCK_EX)
            data = read_json_file(file, default)
            yield data
            write_or_remove_json(file, data)
          ensure
            lock&.flock(::File::LOCK_UN)
          end
        end

        def read_json_file(file, default = {})
          return default.dup unless ::File.exist?(file)

          data = JSON.parse(::File.read(file))
          data.is_a?(Hash) ? data : default.dup
        rescue JSON::ParserError, Errno::ENOENT, SystemCallError, IOError
          default.dup
        end

        def write_or_remove_json(file, data)
          if data.empty?
            FileUtils.rm_f(file)
          else
            atomic_write_json(file, data)
          end
        end

        def applied_hash(data)
          hash = hash_value(data[APPLIED_KEY])
          data[APPLIED_KEY] = hash
          hash
        end

        def hash_value(value)
          value.is_a?(Hash) ? value : {}
        end

        def integer_value(value)
          Integer(value)
        rescue ArgumentError, TypeError, RangeError
          nil
        end

        def positive_integer(value, name)
          integer = Integer(value)
          return integer if integer.positive?

          raise ArgumentError, "#{name} must be positive"
        rescue ArgumentError, TypeError, RangeError
          raise ArgumentError, "#{name} must be positive"
        end

        def transaction_id(entry_ids)
          Digest::SHA256.hexdigest(entry_ids.map(&:to_s).sort.join("\n"))
        end

        def processed_sidecar_for(entry_id)
          ProcessedSidecar.new(path: processed_sidecar_path(stream_file_name_for(entry_id)))
        end

        def stream_file_name_for(entry_id)
          value = entry_id.to_s
          separator = value.index(":")
          return value[0...separator] if separator&.positive?

          "entries-#{Digest::SHA256.hexdigest(value)[0, 16]}"
        end

        def safe_file_name(value)
          value = value.to_s
          return value if value.match?(/\A[a-zA-Z0-9._-]+\z/)

          IndexKey.escape(value)
        end

        def processed_sidecar_path(stream_file_name)
          ::File.join(processed_path, "#{safe_file_name(stream_file_name)}.processed.json")
        end

        def shard_path(section, key)
          ::File.join(rollup_path, section, "shards", "#{shard_id(key)}.json")
        end

        def shard_id(key)
          Digest::SHA256.hexdigest(key.to_s)[0, 2]
        end

        def rollup_bucket_path(every, bucket)
          ::File.join(rollup_path, "hashes", every.to_s, "#{bucket}.json")
        end

        def rollup_bucket_files
          Dir.glob(::File.join(rollup_path, "hashes", "*", "*.json")).sort
        end

        def shard_files(section)
          Dir.glob(::File.join(rollup_path, section, "shards", "*.json")).sort
        end

        def sidecar_files
          Dir.glob(::File.join(processed_path, "*.processed.json")).sort
        end

        def each_data_file(&block)
          (rollup_bucket_files + shard_files("strings")).each(&block)
        end

        def definition_files
          Dir.glob(::File.join(path, "rollups", "*", "*", "v*", "definition.json")).sort
        end

        def filtered_out?(event_filter)
          event_filter && !event_filter.include?(Keys.event_name(report_name))
        end

        def metadata_key?(key)
          key.to_s.start_with?("_")
        end

        def current_time
          Time.now.utc
        end

        def lock_file_path(file)
          "#{file}.lock"
        end

        def cleanup_state_path
          @cleanup_state_path ||= ::File.join(path, "cleanup.json")
        end

        def relative_path(file)
          file.delete_prefix("#{rollup_path}/")
        end

        def absolute_rollup_file(relative_path)
          ::File.expand_path(::File.join(rollup_path, relative_path.to_s))
        end

        def empty_cleanup_result
          {
            rollup_keys_deleted: 0,
            interval_state_keys_deleted: 0,
            processed_entries_deleted: 0
          }
        end

        def merge_cleanup_result(total, result)
          total.each_key do |key|
            total[key] += result.fetch(key, 0)
          end

          total
        end

        def ensure_same_definition!(stored, definition)
          stored_definition = ReportDefinition.from_h(stored)
          return if stored_definition.fingerprint == definition.fingerprint

          raise DefinitionChangedError, "#{definition.name} v#{definition.version} changed; bump version"
        end

        def key_matches?(key, pattern)
          prefix = "#{namespace}:"
          return ::File.fnmatch?(pattern, key) unless pattern.start_with?(prefix)
          return false unless key.start_with?(prefix)

          ::File.fnmatch?(pattern.delete_prefix(prefix), key.delete_prefix(prefix))
        end

        def rollup_path
          @rollup_path ||= ::File.join(
            path,
            "rollups",
            PathName.event(namespace),
            PathName.event(report_name),
            PathName.version(version)
          )
        end

        def processed_path
          @processed_path ||= ::File.join(rollup_path, "processed")
        end

        def scoped?
          namespace && report_name && version
        end

        def scoped_for?(name, version)
          scoped? && report_name == name.to_s && self.version.to_i == version.to_i
        end

        def ensure_scoped!
          ensure_namespace!
          return if scoped?

          raise ConfigurationError, "file rollup storage must be scoped with for_report"
        end

        def ensure_namespace!
          return if namespace

          raise ConfigurationError, "file rollup storage must be configured with namespace"
        end

        def definition_path
          @definition_path ||= ::File.join(rollup_path, "definition.json")
        end

        def lock_path
          @lock_path ||= if scoped?
            ::File.join(rollup_path, "process.lock")
          else
            ::File.join(path, "process.lock")
          end
        end

        IndexStruct = Struct.new(:key)

        class ProcessedSidecar
          include FileHelpers

          attr_reader :path

          def initialize(path:)
            @path = path
          end

          def processed?(entry_id)
            hash_value(read[PROCESSED_IDS_KEY]).key?(entry_id.to_s)
          end

          def mark(entry_ids, batch_id:, applied_paths:, timestamp:)
            update do |data|
              data[STREAM_FILE_KEY] ||= stream_file
              processed = hash_value(data[PROCESSED_IDS_KEY])
              entry_ids.each { |id| processed[id.to_s] = timestamp }
              data[PROCESSED_IDS_KEY] = processed

              batches = hash_value(data[BATCHES_KEY])
              batches[batch_id] = {
                "processed_at" => timestamp,
                "applied_paths" => applied_paths
              }
              data[BATCHES_KEY] = batches
            end
          end

          def batch_paths
            hash_value(read[BATCHES_KEY]).transform_values do |batch|
              Array(hash_value(batch)["applied_paths"]).map(&:to_s)
            end
          end

          def processed_count
            hash_value(read[PROCESSED_IDS_KEY]).length
          end

          def old?(before)
            timestamps = hash_value(read[PROCESSED_IDS_KEY]).values
            return false if timestamps.empty?

            timestamps.all? { |timestamp| processed_entry_old?(timestamp, before) }
          end

          def delete
            ::FileUtils.rm_f(path)
            ::FileUtils.rm_f(lock_path)
          end

          private

          def update
            ::FileUtils.mkdir_p(::File.dirname(path))

            ::File.open(lock_path, ::File::RDWR | ::File::CREAT, 0o600) do |lock|
              lock.flock(::File::LOCK_EX)
              data = read
              yield data
              write(data)
            ensure
              lock&.flock(::File::LOCK_UN)
            end
          end

          def read
            return {} unless ::File.exist?(path)

            data = ::JSON.parse(::File.read(path))
            data.is_a?(Hash) ? data : {}
          rescue ::JSON::ParserError, Errno::ENOENT, SystemCallError, IOError
            {}
          end

          def write(data)
            atomic_write_json(path, data)
          end

          def hash_value(value)
            value.is_a?(Hash) ? value : {}
          end

          def stream_file
            ::File.basename(path).delete_suffix(".processed.json")
          end

          def lock_path
            "#{path}.lock"
          end

          def processed_entry_old?(timestamp, before)
            ::Time.parse(timestamp).utc < before
          rescue ArgumentError, TypeError, RangeError
            true
          end
        end
      end
    end
  end
end

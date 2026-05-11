require "time_bucket_stream"

require_relative "../file_helpers"

module EventMeter
  module Stores
    module Stream
      class File
        include FileHelpers

        SYNC_MODES = %i[none flush fsync].freeze

        attr_reader :path, :stream_options, :sync

        def initialize(path:, sync: :flush, **stream_options)
          @path = normalize_file_store_path(path)
          @sync = sync.respond_to?(:to_sym) ? sync.to_sym : sync
          @stream_options = stream_options
          validate_sync!
          @streams = {}
          @read_ids = []
        end

        def append(payload)
          hash = payload.to_h
          stream_for(hash.fetch("name")).append(hash)
        end

        def read(name:)
          release
          @read_name = name.to_s
          @batch = stream_for(name).read
          @read_ids = @batch.entries.map(&:first)
          @batch.entries
        end

        def delete
          return false unless @batch

          @batch.delete
          deleted_claims?
        ensure
          @read_ids = []
          @read_name = nil
          @batch = nil
        end

        def release
          @batch&.release
        ensure
          @read_ids = []
          @read_name = nil
          @batch = nil
        end

        def close
          release
          @streams.each_value(&:close)
        end

        private

        def validate_sync!
          return if SYNC_MODES.include?(sync)

          raise ArgumentError, "unsupported file sync mode: #{sync.inspect}"
        end

        def stream_for(name)
          @streams[name.to_s] ||= TimeBucketStream.new(
            path: ::File.join(path, "streams", PathName.event(name)),
            sync: sync,
            **stream_options
          )
        end

        def deleted_claims?
          log_names = @read_ids.filter_map { |id| stream_file_name(id) }.uniq
          return true if log_names.empty?

          log_names.none? do |log_name|
            ::File.exist?(::File.join(stream_path_for(@read_name), "processing", log_name))
          end
        end

        def stream_file_name(entry_id)
          value = entry_id.to_s
          separator = value.index(":")
          value[0...separator] if separator&.positive?
        end

        def stream_path_for(name)
          ::File.join(path, "streams", PathName.event(name))
        end
      end
    end
  end
end

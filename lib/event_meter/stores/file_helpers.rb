require "fileutils"
require "json"
require "securerandom"

module EventMeter
  module Stores
    module FileHelpers
      private

      def atomic_write_json(path, value)
        atomic_write(path, JSON.generate(value))
      end

      def normalize_file_store_path(value, name: "path")
        path = value.to_s
        raise ArgumentError, "#{name} cannot be blank" if path.strip.empty?

        ::File.expand_path(path)
      end

      def atomic_write(path, contents)
        temporary_path = temporary_path_for(path)

        ::File.open(temporary_path, ::File::WRONLY | ::File::CREAT | ::File::TRUNC, 0o600) do |file|
          file.write(contents)
          file.flush
          file.fsync
        end

        ::File.rename(temporary_path, path)
        fsync_directory(::File.dirname(path))
      ensure
        FileUtils.rm_f(temporary_path) if temporary_path && ::File.exist?(temporary_path)
      end

      def fsync_directory(path)
        ::File.open(path, ::File::RDONLY) { |directory| directory.fsync }
      rescue SystemCallError
        nil
      end

      def temporary_path_for(path)
        "#{path}.#{Process.pid}.#{Thread.current.object_id}.#{SecureRandom.hex(4)}.tmp"
      end
    end
  end
end

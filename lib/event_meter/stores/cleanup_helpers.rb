require "time"

require_relative "../index_key"

module EventMeter
  module Stores
    module CleanupHelpers
      private

      def event_filter(events)
        events = Array(events).compact

        events.empty? ? nil : events.map { |event| IndexKey.escape(event) }
      end

      def rollup_key_old?(key, before, filter)
        prefix = "#{namespace}:rollup:"
        return false unless key.start_with?(prefix)

        event_name, _version, every, bucket_id = key.delete_prefix(prefix).split(":", 5)
        return false unless event_name && every && bucket_id
        return false if filter && !filter.include?(event_name)

        bucket_end_time(every, bucket_id) <= before
      rescue ArgumentError, TypeError
        false
      end

      def state_key_old?(key, before_ms, filter, value)
        prefix = "#{namespace}:state:"
        return false unless key.start_with?(prefix)

        event_name = key.delete_prefix(prefix).split(":", 3).first
        return false if filter && !filter.include?(event_name)

        Integer(value) < before_ms
      rescue ArgumentError, TypeError, RangeError
        true
      end

      def bucket_end_time(every, bucket_id)
        case every
        when "minute"
          minute_bucket_time(bucket_id) + 60
        when "hour"
          hour_bucket_time(bucket_id) + 3600
        else
          raise ArgumentError, "unsupported rollup bucket: #{every.inspect}"
        end
      end

      def minute_bucket_time(bucket_id)
        raise ArgumentError, "malformed minute bucket" unless bucket_id.to_s.match?(/\A\d{12}\z/)

        Time.utc(
          bucket_id[0, 4].to_i,
          bucket_id[4, 2].to_i,
          bucket_id[6, 2].to_i,
          bucket_id[8, 2].to_i,
          bucket_id[10, 2].to_i
        )
      end

      def hour_bucket_time(bucket_id)
        raise ArgumentError, "malformed hour bucket" unless bucket_id.to_s.match?(/\A\d{10}\z/)

        Time.utc(
          bucket_id[0, 4].to_i,
          bucket_id[4, 2].to_i,
          bucket_id[6, 2].to_i,
          bucket_id[8, 2].to_i
        )
      end
    end
  end
end

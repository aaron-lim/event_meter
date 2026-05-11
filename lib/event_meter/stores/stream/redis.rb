require "json"

require_relative "../namespace"
require_relative "../redis_lock"

module EventMeter
  module Stores
    module Stream
      class Redis
        include Namespace
        include RedisLock

        attr_reader :redis, :lock_redis, :namespace, :redis_read_limit

        def initialize(redis:, namespace:, lock_redis: nil, redis_read_limit: nil)
          @redis = redis
          @lock_redis = lock_redis || redis
          @namespace = normalize_namespace(namespace)
          @redis_read_limit = normalize_redis_read_limit(redis_read_limit)
          @read_ids = []
        end

        def append(payload)
          hash = payload.to_h
          redis.xadd(stream_key(hash.fetch("name")), { "payload" => hash.to_json })
        end

        def read(name:)
          range_options = redis_read_limit ? { count: redis_read_limit } : {}
          @read_key = stream_key(name)
          rows = redis.xrange(@read_key, "-", "+", **range_options)
          @read_ids = rows.map(&:first)

          rows.map do |id, fields|
            [id, parse_payload(fields)]
          end
        end

        def delete
          redis.xdel(@read_key, *@read_ids) if @read_key && !@read_ids.empty?
        ensure
          @read_ids = []
          @read_key = nil
        end

        def with_lock(ttl:)
          with_redis_lock(lock_key, ttl: ttl) { yield }
        end

        private

        def stream_key(name)
          [namespace, "stream", Keys.event_name(name)].join(":")
        end

        def lock_key
          [namespace, "stream_lock"].join(":")
        end

        def parse_payload(fields)
          JSON.parse(fields.fetch("payload"))
        rescue JSON::ParserError, KeyError, TypeError
          nil
        end

        def normalize_redis_read_limit(limit)
          return nil if limit.nil?

          limit = Integer(limit)
          return limit if limit.positive?

          raise ArgumentError, "redis_read_limit must be positive"
        rescue ArgumentError, TypeError, RangeError
          raise ArgumentError, "redis_read_limit must be positive"
        end
      end
    end
  end
end

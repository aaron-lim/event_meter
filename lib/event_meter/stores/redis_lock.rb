require "securerandom"

require_relative "lock_refresher"

module EventMeter
  module Stores
    module RedisLock
      LOCK_REFRESH_RATIO = 2.0

      private

      def with_redis_lock(key, ttl:)
        ttl = redis_lock_ttl(ttl)
        token = SecureRandom.hex(16)
        acquired = lock_redis.set(key, token, nx: true, ex: ttl)
        return false unless acquired

        refresher = start_redis_lock_refresher(key, token, ttl)
        yield
        true
      ensure
        stop_redis_lock_refresher(refresher)
        release_redis_lock(key, token) if acquired
      end

      def release_redis_lock(key, token)
        lock_redis.eval(<<~LUA, keys: [key], argv: [token])
          if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
          end

          return 0
        LUA
      end

      def refresh_redis_lock(key, token, ttl)
        result = lock_redis.eval(<<~LUA, keys: [key], argv: [token, ttl])
          if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("expire", KEYS[1], ARGV[2])
          end

          return 0
        LUA

        result == true || result.to_s == "1"
      end

      def start_redis_lock_refresher(key, token, ttl)
        LockRefresher.new(
          interval: ttl.to_f / LOCK_REFRESH_RATIO,
          refresh: -> { refresh_redis_lock(key, token, ttl) },
          failure_message: "redis lock refresh failed",
          thread_name: "event_meter redis lock refresher"
        ).start
      end

      def stop_redis_lock_refresher(refresher)
        return unless refresher

        refresher.stop
      end

      def redis_lock_ttl(ttl)
        ttl = Integer(ttl)
        return ttl if ttl.positive?

        raise ArgumentError, "lock ttl must be positive"
      rescue ArgumentError, TypeError, RangeError
        raise ArgumentError, "lock ttl must be positive"
      end

      def lock_redis
        redis
      end
    end
  end
end

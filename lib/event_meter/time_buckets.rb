module EventMeter
  module TimeBuckets
    SIZES = {
      minute: 60,
      hour: 3600
    }.freeze

    module_function

    def id(time, size)
      time = time.utc

      case normalize(size)
      when :minute
        time.strftime("%Y%m%d%H%M")
      when :hour
        time.strftime("%Y%m%d%H")
      end
    end

    def time(value, size)
      value = value.utc

      case normalize(size)
      when :minute
        Time.utc(value.year, value.month, value.day, value.hour, value.min)
      when :hour
        Time.utc(value.year, value.month, value.day, value.hour)
      end
    end

    def seconds(size)
      SIZES.fetch(normalize(size))
    end

    def between(from, to, size)
      step = seconds(size)
      current = time(from, size)
      buckets = []

      while current < to
        buckets << current
        current += step
      end

      buckets
    end

    def normalize(size)
      size = size.to_sym if size.respond_to?(:to_sym)
      return size if SIZES.key?(size)

      raise ArgumentError, "unsupported bucket size: #{size.inspect}"
    end
  end
end

module EventMeter
  module HashInput
    module_function

    def coerce(value, label)
      return {} if value.nil?

      unless value.respond_to?(:to_h)
        raise TypeError, "#{label} must respond to to_h"
      end

      hash = value.to_h
      unless hash.is_a?(Hash)
        raise TypeError, "#{label}#to_h must return a Hash"
      end

      hash.dup
    end
  end
end

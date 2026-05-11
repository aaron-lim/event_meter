module EventMeter
  module Stores
    module Namespace
      private

      def normalize_namespace(value)
        namespace = value.to_s
        raise ArgumentError, "namespace cannot be blank" if namespace.strip.empty?

        namespace
      end
    end
  end
end

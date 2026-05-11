module EventMeter
  class WriteResult
    attr_reader :payload, :error

    def self.recorded(payload)
      new(payload: payload, error: nil)
    end

    def self.failed(payload:, error:)
      new(payload: payload, error: error)
    end

    def initialize(payload:, error:)
      @payload = payload
      @error = error
    end

    def recorded?
      error.nil?
    end

    def error?
      !recorded?
    end
  end
end

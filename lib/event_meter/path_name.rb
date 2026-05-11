require "digest"

module EventMeter
  module PathName
    WINDOWS_RESERVED_NAMES = %w[
      con prn aux nul
      com1 com2 com3 com4 com5 com6 com7 com8 com9
      lpt1 lpt2 lpt3 lpt4 lpt5 lpt6 lpt7 lpt8 lpt9
    ].freeze

    module_function

    def event(name)
      original = name.to_s
      readable = original
        .downcase
        .gsub(/[^a-z0-9_-]+/, "-")
        .gsub(/-+/, "-")
        .gsub(/\A-|-+\z/, "")

      readable = "event" if readable.empty?
      readable = "event-#{readable}" if WINDOWS_RESERVED_NAMES.include?(readable)
      readable = readable[0, 80].gsub(/-+\z/, "")

      "#{readable}-#{Digest::SHA256.hexdigest(original)[0, 16]}"
    end

    def version(version)
      version = Integer(version)
      raise ArgumentError if version <= 0

      "v#{version}"
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "report version must be a positive integer"
    end
  end
end

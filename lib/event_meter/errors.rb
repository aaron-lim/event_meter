module EventMeter
  class Error < StandardError; end
  class UnsupportedQueryError < Error; end
  class AlreadyRecordedError < Error; end
  class ConfigurationError < Error; end
  class DefinitionChangedError < Error; end
  class DefinitionNotFoundError < Error; end
  class LockLostError < Error; end
end

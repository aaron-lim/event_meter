require "digest"
require "json"

require_relative "hash_input"

module EventMeter
  class ReportDefinition
    Index = Struct.new(:params, keyword_init: true) do
      def matches?(by)
        params == ReportDefinition.normalize_params(by.keys)
      end

      def key_for(values)
        IndexKey.build(params, values)
      end

      def to_h
        params.map(&:to_s)
      end
    end

    Interval = Struct.new(:param, :group_by, keyword_init: true) do
      def initialize(param:, group_by: [])
        super(
          param: ReportDefinition.normalize_param(param),
          group_by: ReportDefinition.normalize_params(group_by)
        )
      end

      def group_index
        Index.new(params: group_by)
      end

      def to_h
        {
          "param" => param.to_s,
          "group_by" => group_by.map(&:to_s)
        }
      end
    end

    BuiltIndex = Struct.new(:index, :key, keyword_init: true)

    attr_reader :name, :version, :indexes, :intervals

    def self.build(name, version:)
      definition = new(name: name, version: version)
      yield definition if block_given?
      definition
    end

    def self.from_h(hash)
      hash = HashInput.coerce(hash, "report definition")

      new(name: hash.fetch("name"), version: hash.fetch("version")).tap do |definition|
        definition.send(:replace_indexes, hash.fetch("indexes", []))
        definition.send(:replace_intervals, hash.fetch("intervals", []))
        definition.send(:validate_fingerprint!, hash.fetch("fingerprint"))
      end
    end

    def initialize(name:, version:)
      @name = normalize_name(name)
      @version = normalize_version(version)
      @indexes = [Index.new(params: [])]
      @intervals = []
    end

    def index_by(*params)
      normalized = self.class.normalize_params(params)
      return self if indexes.any? { |index| index.params == normalized }

      indexes << Index.new(params: normalized)
      self
    end

    def measure_interval_by(param, group_by: [])
      interval = Interval.new(param: param, group_by: group_by)
      intervals << interval unless intervals.any? { |existing| existing == interval }
      index_by(*interval.group_by) unless interval.group_by.empty?
      self
    end

    def indexes_for(payload)
      indexes.filter_map do |index|
        next unless index.params.all? { |param| self.class.indexable_value?(payload.params, param) }

        BuiltIndex.new(index: index, key: index.key_for(payload.params))
      end
    end

    def index_for!(by)
      normalized_by = normalize_by(by)
      index = indexes.find { |candidate| candidate.matches?(normalized_by) }

      unless index
        raise UnsupportedQueryError, "no index configured for #{name} v#{version} by #{normalized_by.keys.inspect}"
      end

      BuiltIndex.new(index: index, key: index.key_for(normalized_by))
    end

    def fingerprint
      Digest::SHA256.hexdigest(JSON.generate(canonical_h))
    end

    def to_h
      canonical_h.merge("fingerprint" => fingerprint)
    end

    def self.normalize_params(params)
      params = params.first if params.is_a?(Array) && params.length == 1 && params.first.is_a?(Array)
      Array(params).map { |param| normalize_param(param) }.uniq.sort
    end

    def self.normalize_param(param)
      return param.to_sym if param.respond_to?(:to_sym) && !param.to_s.strip.empty?

      raise ArgumentError, "report params must be strings or symbols"
    end

    def self.indexable_value?(values, param)
      values.key?(param.to_s) && !values[param.to_s].nil?
    end

    private

    def replace_indexes(raw_indexes)
      @indexes = []
      Array(raw_indexes).each { |params| index_by(*Array(params)) }
      index_by if @indexes.empty?
    end

    def replace_intervals(raw_intervals)
      @intervals = []
      Array(raw_intervals).each do |raw_interval|
        interval = HashInput.coerce(raw_interval, "interval")
        measure_interval_by(
          interval.fetch("param"),
          group_by: interval.fetch("group_by", [])
        )
      end
    end

    def validate_fingerprint!(stored_fingerprint)
      return if stored_fingerprint == fingerprint

      raise DefinitionChangedError, "#{name} v#{version} definition fingerprint does not match stored metadata"
    end

    def canonical_h
      {
        "name" => name,
        "version" => version,
        "indexes" => indexes.map(&:to_h).sort,
        "intervals" => intervals.map(&:to_h).sort_by { |interval| [interval.fetch("param"), interval.fetch("group_by")] }
      }
    end

    def normalize_name(value)
      name = value.to_s
      raise ArgumentError, "report name cannot be blank" if name.strip.empty?

      name
    end

    def normalize_version(value)
      version = Integer(value)
      return version if version.positive?

      raise ArgumentError
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "report version must be a positive integer"
    end

    def normalize_by(by)
      HashInput.coerce(by, "by").each_with_object({}) do |(key, value), hash|
        raise ArgumentError, "by values cannot be nil" if value.nil?

        hash[self.class.normalize_param(key)] = value
      end
    end
  end
end

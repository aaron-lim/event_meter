module EventMeter
  module Keys
    module_function

    def rollup(namespace:, name:, version:, every:, bucket:, index:)
      [
        namespace,
        "rollup",
        event_name(name),
        version_key(version),
        every,
        TimeBuckets.id(bucket, every),
        index.key
      ].join(":")
    end

    def rollup_pattern(namespace:, name:, version:, every:, index:)
      [
        namespace,
        "rollup",
        event_name(name),
        version_key(version),
        every,
        "*",
        index.key
      ].join(":")
    end

    def interval_state(namespace:, name:, version:, definition:, value:)
      [
        namespace,
        "state",
        event_name(name),
        version_key(version),
        "interval",
        IndexKey.escape(definition.param),
        IndexKey.escape(value)
      ].join(":")
    end

    def definition(namespace:, name:, version:)
      [namespace, "definition", event_name(name), version_key(version)].join(":")
    end

    def processed(namespace:, name:, version:, id:)
      [
        namespace,
        "processed",
        event_name(name),
        version_key(version),
        IndexKey.escape(id)
      ].join(":")
    end

    def event_name(name)
      IndexKey.escape(name)
    end

    def version_key(version)
      PathName.version(version)
    end
  end
end

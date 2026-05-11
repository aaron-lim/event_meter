module EventMeterTestSupport
  class MemoryStreamStorage
    attr_reader :stream, :deleted_ids

    def initialize
      @next_id = 0
      @stream = []
      @deleted_ids = []
      @read_ids = []
    end

    def append(payload)
      @next_id += 1
      id = "#{@next_id}-0"

      stream << [id, payload.to_h]
      id
    end

    def read(name:)
      @read_ids = stream.filter_map do |id, payload|
        id if payload && payload["name"] == name.to_s
      end

      stream.select { |id, _payload| @read_ids.include?(id) }
    end

    def delete
      deleted_ids.concat(@read_ids)
      stream.reject! { |id, _payload| @read_ids.include?(id) }
    ensure
      @read_ids = []
    end
  end
end

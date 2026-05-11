require "cgi/escape"

module EventMeter
  module IndexKey
    module_function

    def build(params, values)
      return "all" if params.empty?

      params.map do |param|
        "#{escape(param)}=#{escape(values.fetch(param) { values.fetch(param.to_s) })}"
      end.join("|")
    end

    def escape(value)
      CGI.escape(value.to_s)
    end
  end
end

require "test_helper"
require "event_meter/cli"
require "stringio"

class EventMeterCLITest < EventMeterTest
  def test_postgres_schema_prints_install_sql
    out = StringIO.new
    err = StringIO.new

    status = EventMeter::CLI.call(
      ["postgres", "schema", "--table-prefix", "meter_test"],
      out: out,
      err: err
    )

    assert_equal 0, status
    assert_empty err.string
    assert_includes out.string, "CREATE TABLE IF NOT EXISTS meter_test_rollups"
    assert_includes out.string, "CREATE INDEX IF NOT EXISTS meter_test_processed_created_at_idx"
  end

  def test_postgres_install_requires_a_database_url
    out = StringIO.new
    err = StringIO.new

    status = without_database_url do
      EventMeter::CLI.call(["postgres", "install"], out: out, err: err)
    end

    assert_equal 1, status
    assert_empty out.string
    assert_includes err.string, "missing database URL"
  end

  private

  def without_database_url
    previous = ENV.delete("DATABASE_URL")
    yield
  ensure
    ENV["DATABASE_URL"] = previous if previous
  end
end

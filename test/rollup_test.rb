require "test_helper"

class EventMeterRollupTest < EventMeterTest
  def test_started_at_ignores_nil_values
    rollup = EventMeter::Rollup.new

    rollup.add_started_at(nil)
    rollup.add_started_at(1_777_777_777_000)

    assert_equal 1_777_777_777_000, rollup.fields.fetch("started_at_ms_min")
    assert_equal 1_777_777_777_000, rollup.fields.fetch("started_at_ms_max")
  end

  def test_interval_ignores_negative_values
    rollup = EventMeter::Rollup.new

    rollup.add_interval(125)
    rollup.add_interval(-25)

    assert_equal 1, rollup.fields.fetch("interval_ms_count")
    assert_equal 125, rollup.fields.fetch("interval_ms_sum")
    assert_equal 125, rollup.fields.fetch("interval_ms_min")
    assert_equal 125, rollup.fields.fetch("interval_ms_max")
  end

  def test_combine_preserves_min_max_fields_and_sums_counters
    combined = EventMeter::Rollup.combine([
      {
        "count" => "2",
        "duration_ms_sum" => "40",
        "duration_ms_min" => "10",
        "duration_ms_max" => "30",
        "interval_ms_count" => "1",
        "interval_ms_sum" => "300",
        "interval_ms_min" => "300",
        "interval_ms_max" => "300"
      },
      {
        "count" => "3",
        "duration_ms_sum" => "90",
        "duration_ms_min" => "5",
        "duration_ms_max" => "50",
        "interval_ms_count" => "2",
        "interval_ms_sum" => "500",
        "interval_ms_min" => "200",
        "interval_ms_max" => "300"
      }
    ])

    assert_equal 5, combined.fields.fetch("count")
    assert_equal 130, combined.fields.fetch("duration_ms_sum")
    assert_equal 5, combined.fields.fetch("duration_ms_min")
    assert_equal 50, combined.fields.fetch("duration_ms_max")
    assert_equal 3, combined.fields.fetch("interval_ms_count")
    assert_equal 800, combined.fields.fetch("interval_ms_sum")
    assert_equal 200, combined.fields.fetch("interval_ms_min")
    assert_equal 300, combined.fields.fetch("interval_ms_max")
  end
end

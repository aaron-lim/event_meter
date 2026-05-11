require "test_helper"

class EventMeterTimeBucketsTest < EventMeterTest
  def test_builds_minute_and_hour_bucket_ids
    time = utc(2026, 5, 6, 1, 2, 3)

    assert_equal "202605060102", EventMeter::TimeBuckets.id(time, :minute)
    assert_equal "2026050601", EventMeter::TimeBuckets.id(time, :hour)
  end

  def test_builds_bucket_times_and_ranges
    time = utc(2026, 5, 6, 1, 2, 3)

    assert_equal utc(2026, 5, 6, 1, 2), EventMeter::TimeBuckets.time(time, :minute)
    assert_equal utc(2026, 5, 6, 1, 0), EventMeter::TimeBuckets.time(time, :hour)
    assert_equal [
      utc(2026, 5, 6, 1, 0),
      utc(2026, 5, 6, 1, 1)
    ], EventMeter::TimeBuckets.between(utc(2026, 5, 6, 1, 0), utc(2026, 5, 6, 1, 2), :minute)
  end

  def test_rejects_unknown_bucket_sizes
    assert_raises(ArgumentError) do
      EventMeter::TimeBuckets.seconds(:day)
    end
  end
end

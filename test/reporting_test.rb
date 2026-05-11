require "test_helper"

class EventMeterReportingTest < EventMeterTest
  def test_summary_does_not_process_pending_events_by_default
    configure_delivery_event
    process_delivery_pending

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0, 0), duration_ms: 2_000)

    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1),
      by: { provider: "postmark" }
    )

    assert_equal 0, summary.fetch(:count)
    assert_equal 1, EventMeter.stream_storage.stream.length
  end

  def test_process_pending_updates_reports_and_deletes_stream_rows
    configure_delivery_event

    record_delivery({
      customer_id: 44,
      provider: "postmark",
      delivery_mode: "background_worker",
      queue: "mailers"
    }, started_at: utc(2026, 5, 6, 1, 0, 0), duration_ms: 2_000)
    record_delivery({
      customer_id: 45,
      provider: "postmark",
      delivery_mode: "background_worker",
      queue: "mailers"
    }, status: "failure", started_at: utc(2026, 5, 6, 1, 1, 0), duration_ms: 3_000)

    result = process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 3),
      by: { provider: "postmark" }
    )

    assert_equal expected_delivery_result(processed: 2), result.to_h
    assert_equal 2, summary[:count]
    assert_equal 1, summary[:success_count]
    assert_equal 1, summary[:failure_count]
    assert_equal 2, summary[:duration_ms_count]
    assert_equal 5_000, summary[:duration_ms_sum]
    assert_equal 2_500.0, summary[:duration_ms_avg]
    assert_equal 2_000, summary[:duration_ms_min]
    assert_equal 3_000, summary[:duration_ms_max]
    assert_equal "2026-05-06T01:00:00.000000Z", summary[:started_at_min]
    assert_equal "2026-05-06T01:01:00.000000Z", summary[:started_at_max]
    assert_equal 180.0, summary[:rate_window_seconds]
    assert_in_delta 2.0 / 180.0, summary[:per_second]
    assert_in_delta 2.0 / 3.0, summary[:per_minute]
    assert_empty EventMeter.stream_storage.stream
    assert_equal ["1-0", "2-0"], EventMeter.stream_storage.deleted_ids
  end

  def test_summary_without_window_uses_retained_hour_rollups_and_observed_time_span
    configure_delivery_event

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0, 0))
    record_delivery({
      customer_id: 45,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 3, 0, 0))

    process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION, by: { provider: "postmark" })

    assert_equal 2, summary[:count]
    assert_equal "2026-05-06T01:00:00.000000Z", summary[:started_at_min]
    assert_equal "2026-05-06T03:00:00.000000Z", summary[:started_at_max]
    assert_equal 7200.0, summary[:rate_window_seconds]
    assert_in_delta 2.0 / 7200.0, summary[:per_second]
    assert_in_delta 2.0 / 120.0, summary[:per_minute]
  end

  def test_summary_without_window_rejects_too_many_retained_rollup_buckets
    configure_delivery_event

    EventMeter.configure { |config| config.summary_key_limit = 1 }
    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0, 0))
    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 3, 0, 0))
    process_delivery_pending

    error = assert_raises(ArgumentError) do
      EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION, by: { provider: "postmark" })
    end

    assert_includes error.message, "pass from: and to:"
  end

  def test_summary_treats_corrupt_rollup_numeric_values_as_zero
    configure_delivery_event
    process_delivery_pending
    key = EventMeter::Keys.rollup(
      namespace: EventMeter.configuration.namespace,
      name: "invoice_delivery",
      version: DELIVERY_VERSION,
      every: :minute,
      bucket: utc(2026, 5, 6, 1, 0),
      index: delivery_report_definition.index_for!(provider: "postmark")
    )

    EventMeter.rollup_storage.hashes[key] = {
      "count" => { "bad" => true },
      "duration_ms_sum" => "bad",
      "duration_ms_count" => "1"
    }

    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1),
      by: { provider: "postmark" }
    )

    assert_equal 0, summary.fetch(:count)
    assert_equal 1, summary.fetch(:duration_ms_count)
    assert_equal 0.0, summary.fetch(:duration_ms_avg)
  end

  def test_summary_ignores_unrenderable_corrupt_timestamps
    configure_delivery_event
    process_delivery_pending
    key = EventMeter::Keys.rollup(
      namespace: EventMeter.configuration.namespace,
      name: "invoice_delivery",
      version: DELIVERY_VERSION,
      every: :minute,
      bucket: utc(2026, 5, 6, 1, 0),
      index: delivery_report_definition.index_for!(provider: "postmark")
    )

    EventMeter.rollup_storage.hashes[key] = {
      "count" => "1",
      "started_at_ms_min" => "9" * 400,
      "started_at_ms_max" => "9" * 400
    }

    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1),
      by: { provider: "postmark" }
    )

    assert_equal 1, summary.fetch(:count)
    refute summary.key?(:started_at_min)
    refute summary.key?(:started_at_max)
  end

  def test_summary_requires_from_and_to_together
    configure_delivery_event

    assert_raises(ArgumentError) do
      EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION, from: utc(2026, 5, 6, 1, 0))
    end

    assert_raises(ArgumentError) do
      EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION, to: utc(2026, 5, 6, 1, 0))
    end
  end

  def test_summary_rejects_unparseable_time_values_with_a_clear_error
    configure_delivery_event

    error = assert_raises(ArgumentError) do
      EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION, from: Object.new, to: utc(2026, 5, 6, 1, 0))
    end

    assert_equal "time must be a Time or parseable time string", error.message
  end

  def test_reports_accept_parseable_time_strings_and_nil_by
    configure_delivery_event

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)

    process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: "2026-05-06T01:00:00Z",
      to: "2026-05-06T01:01:00Z",
      by: nil
    )
    series = EventMeter.series("invoice_delivery", version: DELIVERY_VERSION,
      from: "2026-05-06T01:00:00Z",
      to: "2026-05-06T01:01:00Z",
      by: nil
    )

    assert_equal 1, summary.fetch(:count)
    assert_equal 1, series.first.fetch(:count)
  end

  def test_reports_reject_by_values_that_are_not_hash_like
    configure_delivery_event
    process_delivery_pending

    error = assert_raises(TypeError) do
      EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION, by: Object.new)
    end

    assert_equal "by must respond to to_h", error.message
  end

  def test_reports_reject_by_objects_that_do_not_return_hashes
    configure_delivery_event
    process_delivery_pending
    by = Struct.new(:to_h).new([])

    error = assert_raises(TypeError) do
      EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION, by: by)
    end

    assert_equal "by#to_h must return a Hash", error.message
  end

  def test_event_names_with_separators_are_safe_in_storage_keys
    append_event("sync:run",
      {
        account_type: "google_calendar"
      },
      started_at: utc(2026, 5, 6, 1, 0),
      duration_ms: 200
    )

    EventMeter.process_pending("sync:run", version: DELIVERY_VERSION) do |report|
      report.index_by(:account_type)
    end
    summary = EventMeter.summary("sync:run", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1),
      by: { account_type: "google_calendar" }
    )

    assert_equal 1, summary.fetch(:count)
    assert_equal 200, summary.fetch(:duration_ms_sum)
  end

  def test_nil_index_values_do_not_merge_with_empty_string_values
    append_event("invoice_delivery",
      { provider: nil },
      started_at: utc(2026, 5, 6, 1, 0),
      duration_ms: 100
    )
    append_event("invoice_delivery",
      { provider: "" },
      started_at: utc(2026, 5, 6, 1, 0),
      duration_ms: 200
    )

    process_delivery_pending
    blank_provider = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1),
      by: { provider: "" }
    )

    assert_equal 2, EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION).fetch(:count)
    assert_equal 1, blank_provider.fetch(:count)
    assert_equal 200, blank_provider.fetch(:duration_ms_sum)
    assert_raises(ArgumentError) { EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION, by: { provider: nil }) }
  end

  def test_summary_and_series_require_forward_time_windows
    configure_delivery_event

    assert_raises(ArgumentError) do
      EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 0)
      )
    end

    assert_raises(ArgumentError) do
      EventMeter.series("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 1),
        to: utc(2026, 5, 6, 1, 0)
      )
    end
  end

  def test_compare_requires_bounded_ranges
    configure_delivery_event

    assert_raises(ArgumentError) do
      EventMeter.compare("invoice_delivery", version: DELIVERY_VERSION,
        before: Object.new,
        after: utc(2026, 5, 6, 1, 0)..utc(2026, 5, 6, 2, 0)
      )
    end

    assert_raises(ArgumentError) do
      EventMeter.compare("invoice_delivery", version: DELIVERY_VERSION,
        before: utc(2026, 5, 6, 1, 0)..,
        after: utc(2026, 5, 6, 2, 0)..utc(2026, 5, 6, 3, 0)
      )
    end
  end

  def test_summary_rate_window_uses_the_rollup_bucket_span
    configure_delivery_event

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0, 0))
    record_delivery({
      customer_id: 45,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 1, 0))

    process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0, 30),
      to: utc(2026, 5, 6, 1, 1, 30),
      by: { provider: "postmark" }
    )

    assert_equal 2, summary.fetch(:count)
    assert_equal 120, summary.fetch(:rate_window_seconds)
  end

  def test_compound_indexes_support_summary
    configure_delivery_event

    4.times do |index|
      record_delivery({
        customer_id: 44 + index,
        provider: "postmark",
        delivery_mode: "background_worker",
        queue: "mailers"
      }, started_at: utc(2026, 5, 6, 1, index), duration_ms: 100 + index)
    end

    process_delivery_pending
    by = { queue: "mailers", provider: "postmark", delivery_mode: "background_worker" }
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 5),
      by: by
    )

    assert_equal 4, summary[:count]
  end

  def test_interval_metrics_use_started_at_and_skip_out_of_order_events
    configure_delivery_event

    record_delivery({
      customer_id: 44,
      provider: "postmark",
      delivery_mode: "background_worker",
      queue: "mailers"
    }, started_at: utc(2026, 5, 6, 1, 0, 0))
    record_delivery({
      customer_id: 44,
      provider: "postmark",
      delivery_mode: "background_worker",
      queue: "mailers"
    }, started_at: utc(2026, 5, 6, 1, 5, 0))
    record_delivery({
      customer_id: 44,
      provider: "postmark",
      delivery_mode: "background_worker",
      queue: "mailers"
    }, started_at: utc(2026, 5, 6, 1, 4, 0))

    process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 10),
      by: { provider: "postmark" }
    )

    assert_equal 3, summary[:count]
    assert_equal 1, summary[:interval_ms_count]
    assert_equal 300_000, summary[:interval_ms_sum]
    assert_equal 300_000.0, summary[:interval_ms_avg]
    assert_equal 300_000, summary[:interval_ms_min]
    assert_equal 300_000, summary[:interval_ms_max]

    grouped_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 10),
      by: {
        provider: "postmark",
        delivery_mode: "background_worker",
        queue: "mailers"
      }
    )

    assert_equal 3, grouped_summary[:count]
    assert_equal 1, grouped_summary[:interval_ms_count]
    assert_equal 300_000, grouped_summary[:interval_ms_sum]
  end

  def test_interval_metrics_ignore_corrupt_stored_interval_state
    configure_delivery_event
    interval = delivery_report_definition.intervals.first
    state_key = EventMeter::Keys.interval_state(
      namespace: EventMeter.configuration.namespace,
      name: "invoice_delivery",
      version: DELIVERY_VERSION,
      definition: interval,
      value: 44
    )

    EventMeter.rollup_storage.strings[state_key] = "not-a-timestamp"
    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)

    process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1),
      by: { provider: "postmark" }
    )

    assert_equal 1, summary.fetch(:count)
    assert_equal 0, summary.fetch(:interval_ms_count)
  end

  def test_interval_metrics_accept_persisted_epoch_state
    configure_delivery_event
    interval = delivery_report_definition.intervals.first
    state_key = EventMeter::Keys.interval_state(
      namespace: EventMeter.configuration.namespace,
      name: "invoice_delivery",
      version: DELIVERY_VERSION,
      definition: interval,
      value: 44
    )

    EventMeter.rollup_storage.strings[state_key] = "0"
    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: Time.at(5).utc, duration_ms: 100)

    process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: Time.at(0).utc,
      to: Time.at(10).utc,
      by: { provider: "postmark" }
    )

    assert_equal 1, summary.fetch(:interval_ms_count)
    assert_equal 5_000, summary.fetch(:interval_ms_sum)
  end

  def test_nil_interval_group_values_do_not_merge_with_empty_string_values
    append_event("feed_refresh",
      {
        feed_id: 77,
        provider: nil
      },
      started_at: utc(2026, 5, 6, 1, 0),
      duration_ms: 100
    )
    append_event("feed_refresh",
      {
        feed_id: 77,
        provider: ""
      },
      started_at: utc(2026, 5, 6, 1, 5),
      duration_ms: 200
    )

    EventMeter.process_pending("feed_refresh", version: DELIVERY_VERSION) do |report|
      report.index_by(:provider)
      report.measure_interval_by(:feed_id, group_by: :provider)
    end
    blank_provider = EventMeter.summary("feed_refresh", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 10),
      by: { provider: "" }
    )

    assert_equal 1, blank_provider.fetch(:count)
    assert_equal 0, blank_provider.fetch(:interval_ms_count)
  end

  def test_series_and_compare_reuse_rollups_without_reprocessing
    configure_delivery_event

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0, 0), duration_ms: 100)
    record_delivery({
      customer_id: 45,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 1, 0), duration_ms: 300)

    process_delivery_pending
    series = EventMeter.series("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 3),
      by: { provider: "postmark" }
    )
    comparison = EventMeter.compare("invoice_delivery", version: DELIVERY_VERSION,
      before: utc(2026, 5, 6, 1, 0)...utc(2026, 5, 6, 1, 1),
      after: utc(2026, 5, 6, 1, 1)...utc(2026, 5, 6, 1, 2),
      by: { provider: "postmark" }
    )

    assert_equal [1, 1, 0], series.map { |bucket| bucket[:count] }
    assert_equal 1, comparison.fetch(:before).fetch(:count)
    assert_equal 100.0, comparison.fetch(:before).fetch(:duration_ms_avg)
    assert_equal 1, comparison.fetch(:after).fetch(:count)
    assert_equal 300.0, comparison.fetch(:after).fetch(:duration_ms_avg)
  end

  def test_series_defaults_to_last_hour_of_minute_buckets
    configure_delivery_event
    now = utc(2026, 5, 6, 2, 0, 30)

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0, 0))
    record_delivery({
      customer_id: 45,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 58, 0))

    process_delivery_pending
    Time.stub(:now, now) do
      series = EventMeter.series("invoice_delivery", version: DELIVERY_VERSION, by: { provider: "postmark" })

      assert_equal 60, series.length
      assert_equal "2026-05-06T01:01:00Z", series.first.fetch(:bucket)
      assert_equal "2026-05-06T02:00:00Z", series.last.fetch(:bucket)
      assert_equal 0, series.first.fetch(:count)
      assert_equal 1, series.find { |bucket| bucket.fetch(:bucket) == "2026-05-06T01:58:00Z" }.fetch(:count)
    end
  end

  def test_cleanup_history_removes_old_rollups_and_interval_state
    configure_delivery_event

    old_time = utc(2026, 5, 6, 1, 0, 0)
    new_time = utc(2026, 5, 6, 1, 10, 0)

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: old_time, duration_ms: 100)
    record_delivery({
      customer_id: 45,
      provider: "postmark"
    }, started_at: new_time, duration_ms: 300)

    process_delivery_pending
    result = EventMeter.cleanup_history(before: "2026-05-06T01:05:00Z")

    refute result.key?(:processing)
    assert_equal 3, result.fetch(:rollup_keys_deleted)
    assert_equal 1, result.fetch(:interval_state_keys_deleted)
    assert result.key?(:processed_entries_deleted)

    old_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: old_time,
      to: old_time + 60,
      by: { provider: "postmark" }
    )
    new_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: new_time,
      to: new_time + 60,
      by: { provider: "postmark" }
    )

    assert_equal 0, old_summary.fetch(:count)
    assert_equal 1, new_summary.fetch(:count)
  end

  def test_auto_cleanup_history_runs_from_process_pending_when_enabled
    configure_delivery_event
    old_time = utc(2026, 5, 6, 1, 0, 0)
    cleanup_time = utc(2026, 5, 6, 3, 0, 0)

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: old_time, duration_ms: 100)
    process_delivery_pending

    assert_equal 1, EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: old_time,
      to: old_time + 60,
      by: { provider: "postmark" }
    ).fetch(:count)

    EventMeter.configure do |config|
      config.auto_cleanup_history = true
      config.cleanup_history_retention = 60 * 60
      config.cleanup_history_interval = 60 * 60
    end

    Time.stub(:now, cleanup_time) do
      process_delivery_pending
    end

    assert_equal 0, EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: old_time,
      to: old_time + 60,
      by: { provider: "postmark" }
    ).fetch(:count)
    assert EventMeter.rollup_storage.cleanup_watermark("event_meter:test:auto_cleanup:history:last_run")
  end

  def test_auto_cleanup_history_respects_the_cleanup_interval
    configure_delivery_event
    old_time = utc(2026, 5, 6, 1, 0, 0)
    first_cleanup_time = utc(2026, 5, 6, 3, 0, 0)

    EventMeter.configure do |config|
      config.auto_cleanup_history = true
      config.cleanup_history_retention = 60 * 60
      config.cleanup_history_interval = 60 * 60
    end

    Time.stub(:now, first_cleanup_time) do
      process_delivery_pending
    end

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: old_time, duration_ms: 100)

    Time.stub(:now, first_cleanup_time + 10 * 60) do
      process_delivery_pending
    end

    assert_equal 1, EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: old_time,
      to: old_time + 60,
      by: { provider: "postmark" }
    ).fetch(:count)

    Time.stub(:now, first_cleanup_time + 61 * 60) do
      process_delivery_pending
    end

    assert_equal 0, EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: old_time,
      to: old_time + 60,
      by: { provider: "postmark" }
    ).fetch(:count)
  end

  def test_auto_cleanup_history_does_not_break_processing_when_cleanup_fails
    configure_delivery_event
    rollup_storage = memory_rollup_storage(namespace: EventMeter.configuration.namespace)
    cleanup_errors = []

    EventMeter.configure do |config|
      config.rollup_storage = rollup_storage
      config.auto_cleanup_history = true
      config.auto_cleanup_error_handler = ->(error) { cleanup_errors << error }
    end

    rollup_storage.define_singleton_method(:cleanup_history) do |before:, events:, interval_state:|
      raise "cleanup failed"
    end

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)

    assert_equal expected_delivery_result(processed: 1), process_delivery_pending.to_h
    assert_equal 1, EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1),
      by: { provider: "postmark" }
    ).fetch(:count)
    assert_equal ["cleanup failed"], cleanup_errors.map(&:message)
  end

  def test_cleanup_history_ignores_malformed_rollup_keys
    configure_delivery_event

    EventMeter.rollup_storage.hashes["event_meter:test:rollup:invoice_delivery:minute"] = {
      "count" => "99"
    }
    EventMeter.rollup_storage.hashes["event_meter:test:rollup:invoice_delivery:minute:bad:all"] = {
      "count" => "100"
    }
    EventMeter.rollup_storage.hashes["event_meter:test:rollup:invoice_delivery:hour:202605:all"] = {
      "count" => "100"
    }
    EventMeter.rollup_storage.hashes["event_meter:test:rollup:invoice_delivery:day:20260506:all"] = {
      "count" => "100"
    }
    result = EventMeter.cleanup_history(before: utc(2026, 5, 6, 2, 0))

    assert_equal 0, result.fetch(:rollup_keys_deleted)
    assert EventMeter.rollup_storage.hashes.key?("event_meter:test:rollup:invoice_delivery:minute")
    assert EventMeter.rollup_storage.hashes.key?("event_meter:test:rollup:invoice_delivery:minute:bad:all")
    assert EventMeter.rollup_storage.hashes.key?("event_meter:test:rollup:invoice_delivery:hour:202605:all")
    assert EventMeter.rollup_storage.hashes.key?("event_meter:test:rollup:invoice_delivery:day:20260506:all")
  end

  def test_storage_cleanup_accepts_a_single_event_name_filter
    configure_delivery_event

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)

    process_delivery_pending
    result = EventMeter.rollup_storage.cleanup_history(
      before: utc(2026, 5, 6, 2, 0),
      events: "invoice_delivery",
      interval_state: true
    )

    assert_operator result.fetch(:rollup_keys_deleted), :>, 0
  end
end

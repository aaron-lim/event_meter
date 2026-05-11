require "test_helper"
require "securerandom"
require "timeout"

class EventMeterStorageTest < EventMeterTest
  def test_stream_storage_can_be_separate_from_rollup_storage
    configure_delivery_event
    stream_storage = memory_stream_storage
    rollup_storage = memory_rollup_storage(namespace: "event_meter:test:rollup")

    EventMeter.configure do |config|
      config.stream_storage = stream_storage
      config.rollup_storage = rollup_storage
    end

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
    record_delivery({
      customer_id: 45,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 1), duration_ms: 200)

    assert_equal 2, stream_storage.stream.length
    assert_empty rollup_storage.hashes

    process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 3),
      by: { provider: "postmark" }
    )

    assert_equal 2, summary.fetch(:count)
    assert_equal 300, summary.fetch(:duration_ms_sum)
    assert_empty stream_storage.stream
    assert_equal ["1-0", "2-0"], stream_storage.deleted_ids
    assert_empty rollup_storage.processed
  end

  def test_builtin_stores_reject_blank_storage_coordinates
    ["", " ", nil].each do |path|
      assert_raises(ArgumentError) { EventMeter::Stores::Stream::File.new(path: path) }
      assert_raises(ArgumentError) { EventMeter::Stores::Rollup::File.new(path: path) }
    end

    assert_raises(ArgumentError) do
      EventMeter::Stores::Stream::Redis.new(redis: Object.new, namespace: nil)
    end
    assert_raises(ArgumentError) do
      EventMeter::Stores::Rollup::Redis.new(redis: Object.new, namespace: " ")
    end
    assert_raises(ArgumentError) do
      EventMeter::Stores::Rollup::Postgres.new(
        connection: fake_postgres_connection([]),
        namespace: nil
      )
    end
  end

  def test_file_stream_storage_uses_the_configured_path_directly
    configure_delivery_event
    namespace = "event_meter:test:stream"

    Dir.mktmpdir("event-meter-file-stream-path") do |root|
      path = File.join(root, "event-meter-test-stream")
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: path,
        sync: :flush
      )

      EventMeter.configure do |config|
        config.stream_storage = stream_storage
        config.rollup_storage = memory_rollup_storage(namespace: "event_meter:test:rollup")
      end

      record_delivery({ customer_id: 44, provider: "postmark" })

      readable_logs = file_stream_logs(path)
      escaped_namespace_logs = file_stream_logs(root, namespace)

      assert_equal File.expand_path(path), stream_storage.path
      assert_equal 1, readable_logs.length
      assert_empty escaped_namespace_logs
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_storage_rejects_blank_paths
    ["", " ", nil].each do |path|
      assert_raises(ArgumentError) do
        EventMeter::Stores::Stream::File.new(path: path)
      end
    end
  end

  def test_processed_entry_ids_prevent_duplicate_rollups_when_stream_delete_is_late
    configure_delivery_event
    stream_storage = flaky_delete_stream_storage
    rollup_storage = memory_rollup_storage(namespace: "event_meter:test:rollup")

    EventMeter.configure do |config|
      config.stream_storage = stream_storage
      config.rollup_storage = rollup_storage
    end

    2.times do |index|
      record_delivery({
        customer_id: 44 + index,
        provider: "postmark"
      }, started_at: utc(2026, 5, 6, 1, index), duration_ms: 100)
    end

    first_result = process_delivery_pending
    first_processed_ids = rollup_storage.processed.keys
    first_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 3),
      by: { provider: "postmark" }
    )

    second_result = process_delivery_pending
    second_processed_ids = rollup_storage.processed.keys
    second_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 3),
      by: { provider: "postmark" }
    )

    assert_equal expected_delivery_result(processed: 2, complete: false), first_result.to_h
    assert_equal expected_delivery_result(processed: 0, skipped: 2), second_result.to_h
    assert_equal ["1-0", "2-0"], first_processed_ids
    assert_empty second_processed_ids
    assert_equal 2, first_summary.fetch(:count)
    assert_equal 2, second_summary.fetch(:count)
    assert_empty stream_storage.stream
    assert_equal 2, stream_storage.delete_calls
  end

  def test_duplicate_stream_ids_in_one_batch_are_not_counted_twice
    configure_delivery_event
    stream_storage = memory_stream_storage
    rollup_storage = memory_rollup_storage(namespace: "event_meter:test:rollup")

    EventMeter.configure do |config|
      config.stream_storage = stream_storage
      config.rollup_storage = rollup_storage
    end

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
    stream_storage.stream << stream_storage.stream.first

    result = process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1),
      by: { provider: "postmark" }
    )

    assert_equal expected_delivery_result(processed: 1, skipped: 1), result.to_h
    assert_equal 1, summary.fetch(:count)
    assert_equal ["1-0", "1-0"], stream_storage.deleted_ids
    assert_empty rollup_storage.processed
  end

  def test_malformed_stream_payloads_are_marked_processed_and_deleted
    configure_delivery_event
    stream_storage = memory_stream_storage
    rollup_storage = memory_rollup_storage(namespace: "event_meter:test:rollup")

    EventMeter.configure do |config|
      config.stream_storage = stream_storage
      config.rollup_storage = rollup_storage
    end

    stream_storage.stream << ["bad-1", { "name" => "invoice_delivery", "started_at" => "not-a-time" }]
    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)

    result = process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1),
      by: { provider: "postmark" }
    )

    assert_equal expected_delivery_result(processed: 2, malformed: 1), result.to_h
    assert_empty stream_storage.stream
    assert_equal ["bad-1", "1-0"], stream_storage.deleted_ids
    assert_empty rollup_storage.processed
    assert_equal 1, summary.fetch(:count)
  end

  def test_stream_payloads_missing_started_at_are_malformed
    configure_delivery_event
    stream_storage = memory_stream_storage
    rollup_storage = memory_rollup_storage(namespace: "event_meter:test:rollup")

    EventMeter.configure do |config|
      config.stream_storage = stream_storage
      config.rollup_storage = rollup_storage
    end

    stream_storage.stream << ["bad-1", { "name" => "invoice_delivery", "params" => { "provider" => "postmark" } }]

    result = process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1),
      by: { provider: "postmark" }
    )

    assert_equal expected_delivery_result(processed: 1, malformed: 1), result.to_h
    assert_empty stream_storage.stream
    assert_empty rollup_storage.processed
    assert_equal 0, summary.fetch(:count)
  end

  def test_file_stream_storage_can_feed_a_separate_rollup_storage
    configure_delivery_event
    rollup_storage = memory_rollup_storage(namespace: "event_meter:test:rollup")

    Dir.mktmpdir("event-meter-file-stream") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )

      EventMeter.configure do |config|
        config.stream_storage = stream_storage
        config.rollup_storage = rollup_storage
      end

      write_to_previous_stream_bucket do
        record_delivery({
          customer_id: 44,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
        record_delivery({
          customer_id: 45,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 1), duration_ms: 200)
      end

      process_delivery_pending
      summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 3),
        by: { provider: "postmark" }
      )

      assert_equal 2, summary.fetch(:count)
      assert_equal 300, summary.fetch(:duration_ms_sum)
      assert_empty read_delivery_stream(stream_storage)
      assert_empty file_stream_logs(root, "event_meter:test:stream")
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_deletes_processed_past_minute_logs_but_keeps_current_minute_logs
    configure_delivery_event
    namespace = "event_meter:test:stream"
    rollup_storage = memory_rollup_storage(namespace: "event_meter:test:rollup")

    Dir.mktmpdir("event-meter-file-stream-compact") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )

      EventMeter.configure do |config|
        config.stream_storage = stream_storage
        config.rollup_storage = rollup_storage
      end

      Time.stub(:now, Time.now.utc - 7200) do
        record_delivery({
          customer_id: 44,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
      end

      process_delivery_pending

      assert_empty file_stream_logs(root, namespace)

      stream_storage.close
      Time.stub(:now, Time.now.utc) do
        record_delivery({
          customer_id: 45,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 1), duration_ms: 200)
      end

      process_delivery_pending

      logs = file_stream_logs(root, namespace)
      assert_equal 1, logs.length
      assert_empty read_delivery_stream(stream_storage)
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_reopens_when_active_log_was_removed
    namespace = "event_meter:test:stream"

    Dir.mktmpdir("event-meter-file-stream-removed-log") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )

      write_to_previous_stream_bucket do
        append_delivery(stream_storage,
          customer_id: 44,
          provider: "postmark",
          started_at: utc(2026, 5, 6, 1, 0),
          duration_ms: 100)
      end

      FileUtils.rm_f(file_stream_logs(root, namespace).first)

      write_to_previous_stream_bucket do
        append_delivery(stream_storage,
          customer_id: 45,
          provider: "postmark",
          started_at: utc(2026, 5, 6, 1, 1),
          duration_ms: 200)
      end

      entries = read_delivery_stream(stream_storage)

      assert_equal 1, entries.length
      assert_equal 45, entries.first.last.dig("params", "customer_id")
      assert_empty file_stream_logs(root, namespace)
      assert_equal 1, file_stream_processing_logs(root, namespace).length
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_recreates_logs_path_when_it_was_removed
    Dir.mktmpdir("event-meter-file-stream-removed-dir") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )
      stream_path = file_stream_path(root)

      FileUtils.rm_rf(File.join(stream_path, "logs"))

      write_to_previous_stream_bucket do
        append_delivery(stream_storage,
          customer_id: 44,
          provider: "postmark",
          started_at: utc(2026, 5, 6, 1, 0),
          duration_ms: 100)
      end

      assert_equal 1, read_delivery_stream(stream_storage).length
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_serializes_concurrent_appends
    Dir.mktmpdir("event-meter-file-stream-concurrent-append") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )

      write_to_previous_stream_bucket do
        run_concurrently((0...10).to_a) do |thread_index|
          20.times do |offset|
            append_delivery(stream_storage,
              customer_id: (thread_index * 20) + offset,
              provider: "postmark",
              started_at: utc(2026, 5, 6, 1, 0),
              duration_ms: 100)
          end
        end
      end

      entries = read_delivery_stream(stream_storage)
      customer_ids = entries.map { |_id, payload| payload.dig("params", "customer_id") }

      assert_equal 200, entries.length
      assert_equal (0...200).to_a, customer_ids.sort
      assert_equal entries.length, entries.map(&:first).uniq.length
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_reads_whole_files_without_splitting_by_limit
    Dir.mktmpdir("event-meter-file-stream-whole-file") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )

      write_to_previous_stream_bucket do
        append_delivery(stream_storage,
          customer_id: 44,
          provider: "postmark",
          started_at: utc(2026, 5, 6, 1, 0),
          duration_ms: 100)
        append_delivery(stream_storage,
          customer_id: 45,
          provider: "postmark",
          started_at: utc(2026, 5, 6, 1, 1),
          duration_ms: 200)
      end

      entries = read_delivery_stream(stream_storage)

      assert_equal 2, entries.length
      assert_equal [44, 45], entries.map { |_id, payload| payload.dig("params", "customer_id") }
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_rereads_undeleted_files_without_double_counting
    configure_delivery_event
    rollup_storage = memory_rollup_storage(namespace: "event_meter:test:rollup")

    Dir.mktmpdir("event-meter-file-stream-undeleted") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )
      first_delete = true

      stream_storage.define_singleton_method(:delete) do
        if first_delete
          first_delete = false
        else
          super()
        end
      end

      EventMeter.configure do |config|
        config.stream_storage = stream_storage
        config.rollup_storage = rollup_storage
      end

      write_to_previous_stream_bucket do
        record_delivery({
          customer_id: 44,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
      end

      first_result = process_delivery_pending
      second_result = process_delivery_pending
      summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 1),
        by: { provider: "postmark" }
      )

      assert_equal expected_delivery_result(processed: 1, complete: false), first_result.to_h
      assert_equal expected_delivery_result(processed: 0, skipped: 1), second_result.to_h
      assert_equal 1, summary.fetch(:count)
      assert_empty read_delivery_stream(stream_storage)
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_rotates_writer_logs_by_minute
    namespace = "event_meter:test:stream"

    Dir.mktmpdir("event-meter-file-stream-rotate") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )

      Time.stub(:now, utc(2026, 5, 6, 1, 0)) do
        append_delivery(stream_storage,
          customer_id: 44,
          provider: "postmark",
          started_at: utc(2026, 5, 6, 1, 0),
          duration_ms: 100)
      end

      Time.stub(:now, utc(2026, 5, 6, 1, 1)) do
        append_delivery(stream_storage,
          customer_id: 45,
          provider: "postmark",
          started_at: utc(2026, 5, 6, 1, 1),
          duration_ms: 200)
      end

      log_names = file_stream_logs(root, namespace).map { |path| File.basename(path) }

      assert_equal 2, log_names.length
      assert log_names.any? { |name| name.start_with?("202605060100-") }
      assert log_names.any? { |name| name.start_with?("202605060101-") }
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_waits_until_the_claim_grace_passes_before_reading
    now = utc(2026, 5, 6, 1, 0)

    Dir.mktmpdir("event-meter-file-stream-read-delay") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )

      Time.stub(:now, now) do
        append_delivery(stream_storage,
          customer_id: 44,
          provider: "postmark",
          started_at: utc(2026, 5, 6, 1, 0),
          duration_ms: 100)

        assert_empty read_delivery_stream(stream_storage)
      end

      Time.stub(:now, now + 70) do
        entries = read_delivery_stream(stream_storage)

        assert_equal 1, entries.length
        assert_equal 44, entries.first.last.dig("params", "customer_id")
      end
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_ignores_obsolete_writer_lock_files
    Dir.mktmpdir("event-meter-file-stream-obsolete-writer-lock") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )
      logs_path = File.join(file_stream_path(root), "logs")
      locks_path = File.join(file_stream_path(root), "writer_locks")
      log_name = previous_stream_log_name("locked")
      log_path = File.join(logs_path, log_name)
      lock_path = File.join(locks_path, "#{log_name.delete_suffix(".jsonl")}.lock")

      FileUtils.mkdir_p(logs_path)
      File.write(log_path, JSON.generate("id" => 1, "payload" => {
        "name" => "invoice_delivery",
        "status" => "success",
        "started_at" => utc(2026, 5, 6, 1, 0).iso8601(6),
        "duration_ms" => 100,
        "params" => { "customer_id" => 44, "provider" => "postmark" }
      }) + "\n")

      FileUtils.mkdir_p(locks_path)
      File.open(lock_path, File::RDWR | File::CREAT, 0o600) do |lock|
        lock.flock(File::LOCK_EX)

        assert_equal 1, read_delivery_stream(stream_storage).length
      end
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_does_not_delete_current_minute_logs
    namespace = "event_meter:test:stream"
    now = utc(2026, 5, 6, 1, 0)

    Dir.mktmpdir("event-meter-file-stream-current-delete") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )

      Time.stub(:now, now) do
        append_delivery(stream_storage,
          customer_id: 44,
          provider: "postmark",
          started_at: utc(2026, 5, 6, 1, 0),
          duration_ms: 100)

        log_path = file_stream_logs(root, namespace).first
        stream_storage.delete

        assert_path_exists log_path
        assert_empty read_delivery_stream(stream_storage)
      end

      Time.stub(:now, now + 70) do
        entries = read_delivery_stream(stream_storage)

        assert_equal 1, entries.length
        assert_equal 44, entries.first.last.dig("params", "customer_id")
      end
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_delete_uses_the_read_cutoff_if_the_clock_moves_backwards
    namespace = "event_meter:test:stream"
    now = utc(2026, 5, 6, 1, 0)

    Dir.mktmpdir("event-meter-file-stream-clock-backwards") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )

      Time.stub(:now, now) do
        append_delivery(stream_storage,
          customer_id: 44,
          provider: "postmark",
          started_at: utc(2026, 5, 6, 1, 0),
          duration_ms: 100)
      end

      Time.stub(:now, now + 70) do
        read_delivery_stream(stream_storage)
      end

      Time.stub(:now, now - 60) do
        stream_storage.delete
      end

      Time.stub(:now, now + 70) do
        assert_empty read_delivery_stream(stream_storage)
        assert_empty file_stream_logs(root, namespace)
      end
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_storage_rejects_unsupported_sync_modes
    Dir.mktmpdir("event-meter-file-stream-sync") do |root|
      assert_raises(ArgumentError) do
        EventMeter::Stores::Stream::File.new(path: root, sync: :later)
      end

      assert_raises(ArgumentError) do
        EventMeter::Stores::Stream::File.new(path: root, sync: nil)
      end
    end
  end

  def test_file_stream_processing_does_not_take_a_coarse_lock_for_plain_rollups
    configure_delivery_indexes
    rollup_storage = memory_rollup_storage(namespace: "event_meter:test:rollup")
    rollup_lock_calls = 0
    rollup_lock = rollup_storage.method(:with_lock)

    rollup_storage.define_singleton_method(:with_lock) do |ttl:, &block|
      rollup_lock_calls += 1
      rollup_lock.call(ttl: ttl, &block)
    end

    Dir.mktmpdir("event-meter-file-stream-lock") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )

      EventMeter.configure do |config|
        config.stream_storage = stream_storage
        config.rollup_storage = rollup_storage
      end

      write_to_previous_stream_bucket do
        record_delivery({
          customer_id: 44,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
      end

      result = process_delivery_indexes_pending
      summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 1),
        by: { provider: "postmark" }
      )

      assert_equal expected_delivery_result(processed: 1), result.to_h
      assert_equal 1, summary.fetch(:count)
      assert_equal 0, rollup_lock_calls
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_interval_processing_keeps_entries_when_rollup_lock_is_busy
    configure_delivery_event
    rollup_storage = memory_rollup_storage(namespace: "event_meter:test:rollup")

    def rollup_storage.with_lock(ttl:)
      false
    end

    Dir.mktmpdir("event-meter-file-stream-interval-lock") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )

      EventMeter.configure do |config|
        config.stream_storage = stream_storage
        config.rollup_storage = rollup_storage
      end

      write_to_previous_stream_bucket do
        record_delivery({
          customer_id: 44,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
      end

      result = process_delivery_pending

      assert_equal expected_delivery_result(processed: 0, complete: false, locked: true), result.to_h
      assert_equal 1, read_delivery_stream(stream_storage).length
      assert_empty rollup_storage.hashes
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_quarantines_corrupt_complete_lines
    namespace = "event_meter:test:stream"

    Dir.mktmpdir("event-meter-file-stream-corrupt") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )
      logs_path = File.join(file_stream_path(root), "logs")
      corrupt_log_path = File.join(logs_path, previous_stream_log_name("corrupt"))

      FileUtils.mkdir_p(logs_path)
      File.write(corrupt_log_path, "not-json\n")

      entries = read_delivery_stream(stream_storage)

      assert_empty entries
      assert_empty read_delivery_stream(stream_storage)
      assert_empty file_stream_logs(root, namespace)
      assert_equal 1, file_stream_quarantine_logs(root, namespace).length
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_quarantines_json_that_is_not_an_object
    namespace = "event_meter:test:stream"

    Dir.mktmpdir("event-meter-file-stream-corrupt-shape") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )
      logs_path = File.join(file_stream_path(root), "logs")
      corrupt_log_path = File.join(logs_path, previous_stream_log_name("corrupt"))

      FileUtils.mkdir_p(logs_path)
      File.write(corrupt_log_path, "[]\n")

      entries = read_delivery_stream(stream_storage)

      assert_empty entries
      assert_empty read_delivery_stream(stream_storage)

      File.write(File.join(logs_path, previous_stream_log_name("null")), "null\n")

      entries = read_delivery_stream(stream_storage)

      assert_empty entries
      assert_empty read_delivery_stream(stream_storage)
      assert_equal 2, file_stream_quarantine_logs(root, namespace).length
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_quarantines_empty_files
    namespace = "event_meter:test:stream"

    Dir.mktmpdir("event-meter-file-stream-empty") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )
      logs_path = File.join(file_stream_path(root), "logs")
      empty_log_path = File.join(logs_path, previous_stream_log_name("empty"))

      FileUtils.mkdir_p(logs_path)
      FileUtils.touch(empty_log_path)

      entries = read_delivery_stream(stream_storage)

      assert_empty entries
      assert_empty file_stream_logs(root, namespace)
      assert_equal 1, file_stream_quarantine_logs(root, namespace).length
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_does_not_delete_logs_with_partial_trailing_lines
    Dir.mktmpdir("event-meter-file-stream-partial-compact") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )
      logs_path = File.join(file_stream_path(root), "logs")
      log_name = "199901010100-host-123-deadbeef.jsonl"
      log_path = File.join(logs_path, log_name)

      FileUtils.mkdir_p(logs_path)
      File.write(log_path, JSON.generate("id" => "entry-1", "payload" => { "name" => "invoice_delivery" }))
      stream_storage.delete

      assert_path_exists log_path
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_delete_without_a_read_is_a_no_op
    Dir.mktmpdir("event-meter-file-stream-invalid-delete") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )
      outside_path = File.join(root, "outside.jsonl")

      File.write(outside_path, "keep me")
      stream_storage.delete

      assert_equal "keep me", File.read(outside_path)
      assert_empty read_delivery_stream(stream_storage)
    ensure
      stream_storage&.close
    end
  end

  def test_file_stream_delete_and_read_tolerate_missing_batches
    Dir.mktmpdir("event-meter-file-stream-bad-input") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        sync: :flush
      )

      stream_storage.delete
      stream_storage.delete

      assert_equal [], read_delivery_stream(stream_storage)
      assert_equal [], read_delivery_stream(stream_storage)
      assert_empty read_delivery_stream(stream_storage)
    ensure
      stream_storage&.close
    end
  end

  def test_concurrent_file_stream_processors_do_not_lose_redis_rollup_updates
    redis = redis_client
    namespace = "event_meter:test:race:#{Process.pid}:#{SecureRandom.hex(4)}"
    config = delivery_config(namespace: namespace)
    barrier = TwoThreadBarrier.new(2)
    rollup_storage = race_exposing_redis_rollup(redis: redis, namespace: namespace, barrier: barrier)
      .for_report(name: DELIVERY_EVENT, version: DELIVERY_VERSION)

    Dir.mktmpdir("event-meter-stream-a") do |root_a|
      Dir.mktmpdir("event-meter-stream-b") do |root_b|
        streams = [
          EventMeter::Stores::Stream::File.new(path: root_a, sync: :flush),
          EventMeter::Stores::Stream::File.new(path: root_b, sync: :flush)
        ]

        write_to_previous_stream_bucket do
          append_delivery(streams[0],
            customer_id: 44,
            provider: "postmark",
            started_at: utc(2026, 5, 6, 1, 0, 1),
            duration_ms: 100)
          append_delivery(streams[1],
            customer_id: 45,
            provider: "postmark",
            started_at: utc(2026, 5, 6, 1, 0, 2),
            duration_ms: 250)
        end

        processors = streams.map do |stream|
          EventMeter::Processor.new(
            configuration: config,
            report_definition: delivery_indexes_definition,
            stream_storage: stream,
            rollup_storage: rollup_storage
          )
        end

        results = with_armed_race_barrier(rollup_storage) do
          run_concurrently(processors) { |processor| processor.process }
        end
        summary = EventMeter::Reports.new(
          configuration: config,
          rollup_storage: rollup_storage
        ).summary("invoice_delivery", version: DELIVERY_VERSION,
          from: utc(2026, 5, 6, 1, 0),
          to: utc(2026, 5, 6, 1, 1),
          by: { provider: "postmark" }
        )

        assert_equal [
          expected_delivery_result(processed: 1),
          expected_delivery_result(processed: 1)
        ], results.map(&:to_h)
        assert_equal 2, summary.fetch(:count)
        assert_equal 2, summary.fetch(:success_count)
        assert_equal 2, summary.fetch(:duration_ms_count)
        assert_equal 350, summary.fetch(:duration_ms_sum)
        assert_equal 100, summary.fetch(:duration_ms_min)
        assert_equal 250, summary.fetch(:duration_ms_max)
        assert_equal "2026-05-06T01:00:01.000000Z", summary.fetch(:started_at_min)
        assert_equal "2026-05-06T01:00:02.000000Z", summary.fetch(:started_at_max)
      ensure
        streams&.each(&:close)
      end
    end
  ensure
    cleanup_redis(redis, namespace) if redis && namespace
  end

  def test_file_rollup_storage_can_store_reports_from_a_separate_stream
    configure_delivery_event
    stream_storage = memory_stream_storage

    Dir.mktmpdir("event-meter-file-rollup") do |root|
      path = File.join(root, "event-meter-test-rollup")
      rollup_storage = EventMeter::Stores::Rollup::File.new(path: path)

      EventMeter.configure do |config|
        config.stream_storage = stream_storage
        config.rollup_storage = rollup_storage
      end

      write_to_previous_stream_bucket do
        record_delivery({
          customer_id: 44,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
      end
      record_delivery({
        customer_id: 45,
        provider: "postmark"
      }, started_at: utc(2026, 5, 6, 1, 1), duration_ms: 200)

      process_delivery_pending
      summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 3),
        by: { provider: "postmark" }
      )

      assert_equal 2, summary.fetch(:count)
      assert_equal 300, summary.fetch(:duration_ms_sum)
      assert_empty stream_storage.stream
      assert_equal File.expand_path(path), rollup_storage.path
      report_path = file_rollup_report_path(path, namespace: EventMeter.configuration.namespace)
      assert File.exist?(File.join(report_path, "definition.json"))
      assert File.exist?(File.join(report_path, "hashes", "minute", "202605060100.json"))
    end
  end

  def test_file_rollup_storage_binds_to_configuration_namespace
    stream_storage = memory_stream_storage
    namespace = "event_meter:test:file-bind"

    Dir.mktmpdir("event-meter-file-rollup-namespace-bind") do |root|
      rollup_storage = EventMeter::Stores::Rollup::File.new(path: root)

      EventMeter.configure do |config|
        config.namespace = namespace
        config.stream_storage = stream_storage
        config.rollup_storage = rollup_storage
      end

      record_delivery({
        customer_id: 44,
        provider: "postmark"
      }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)

      process_delivery_pending

      assert_equal namespace, rollup_storage.namespace
      assert_equal 1, EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 1),
        by: { provider: "postmark" }
      ).fetch(:count)
    end
  end

  def test_file_rollup_storage_isolates_shared_path_by_namespace
    first_namespace = "event_meter:test:file-namespace-a"
    second_namespace = "event_meter:test:file-namespace-b"

    Dir.mktmpdir("event-meter-file-rollup-namespace-isolation") do |root|
      EventMeter.configure do |config|
        config.namespace = first_namespace
        config.stream_storage = memory_stream_storage
        config.rollup_storage = EventMeter::Stores::Rollup::File.new(path: root)
      end
      record_delivery({
        customer_id: 44,
        provider: "postmark"
      }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
      process_delivery_pending

      EventMeter.reset
      EventMeter.configure do |config|
        config.namespace = second_namespace
        config.stream_storage = memory_stream_storage
        config.rollup_storage = EventMeter::Stores::Rollup::File.new(path: root)
      end
      record_delivery({
        customer_id: 45,
        provider: "mailgun"
      }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 200)
      process_delivery_pending

      EventMeter.reset
      EventMeter.configure do |config|
        config.namespace = first_namespace
        config.stream_storage = memory_stream_storage
        config.rollup_storage = EventMeter::Stores::Rollup::File.new(path: root)
      end

      first_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 1),
        by: { provider: "postmark" }
      )
      leaked_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 1),
        by: { provider: "mailgun" }
      )

      assert_equal 1, first_summary.fetch(:count)
      assert_equal 0, leaked_summary.fetch(:count)
      assert File.exist?(file_rollup_report_path(root, namespace: first_namespace))
      assert File.exist?(file_rollup_report_path(root, namespace: second_namespace))
    end
  end

  def test_file_rollup_removes_processed_sidecars_and_applied_markers_after_stream_delete
    configure_delivery_event
    stream_storage = memory_stream_storage

    Dir.mktmpdir("event-meter-file-rollup-clean-processed") do |root|
      rollup_storage = EventMeter::Stores::Rollup::File.new(path: root)

      EventMeter.configure do |config|
        config.stream_storage = stream_storage
        config.rollup_storage = rollup_storage
      end

      record_delivery({
        customer_id: 44,
        provider: "postmark"
      }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)

      process_delivery_pending

      report_path = file_rollup_report_path(root, namespace: EventMeter.configuration.namespace)
      bucket_file = File.join(report_path, "hashes", "minute", "202605060100.json")

      assert_empty Dir[File.join(report_path, "processed", "*.processed.json")]
      refute JSON.parse(File.read(bucket_file)).key?("_applied")
    end
  end

  def test_file_rollup_retry_after_partial_apply_does_not_double_count
    configure_delivery_event

    Dir.mktmpdir("event-meter-file-rollup-partial-retry") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: File.join(root, "stream"),
        sync: :flush
      )
      rollup_storage = crash_after_rollup_file_storage(File.join(root, "rollup"))

      EventMeter.configure do |config|
        config.stream_storage = stream_storage
        config.rollup_storage = rollup_storage
      end

      write_to_previous_stream_bucket do
        record_delivery({
          customer_id: 44,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
      end

      rollup_storage.crash_after_rollups_once!

      assert_raises(RuntimeError) { process_delivery_pending }
      result = process_delivery_pending

      summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 1),
        by: { provider: "postmark" }
      )

      assert_equal expected_delivery_result(processed: 1), result.to_h
      assert_equal 1, summary.fetch(:count)
      assert_equal 100, summary.fetch(:duration_ms_sum)
      assert_empty read_delivery_stream(stream_storage)
    ensure
      stream_storage&.close
    end
  end

  def test_file_rollup_forgetting_processed_ids_does_not_create_locks_for_missing_files
    Dir.mktmpdir("event-meter-file-rollup-missing-applied-path") do |root|
      rollup_storage = EventMeter::Stores::Rollup::File.new(
        path: root,
        namespace: EventMeter.configuration.namespace
      ).for_report(name: "invoice_delivery", version: DELIVERY_VERSION)
      sidecar_path = rollup_storage.send(:processed_sidecar_path, "stream.jsonl")
      missing_rollup_path = File.join(
        file_rollup_report_path(root, namespace: EventMeter.configuration.namespace),
        "hashes",
        "minute",
        "missing.json"
      )

      FileUtils.mkdir_p(File.dirname(sidecar_path))
      File.write(sidecar_path, JSON.generate(
        "stream_file" => "stream.jsonl",
        "processed_ids" => { "stream.jsonl:1" => "2026-05-06T01:00:00Z" },
        "batches" => {
          "batch-1" => {
            "processed_at" => "2026-05-06T01:00:00Z",
            "applied_paths" => ["hashes/minute/missing.json"]
          }
        }
      ))

      rollup_storage.forget_processed_ids(["stream.jsonl:1"])

      refute File.exist?(sidecar_path)
      refute File.exist?(missing_rollup_path)
      refute File.exist?("#{missing_rollup_path}.lock")
    end
  end

  def test_file_rollup_storage_rejects_blank_paths
    ["", " ", nil].each do |path|
      assert_raises(ArgumentError) do
        EventMeter::Stores::Rollup::File.new(path: path)
      end
    end
  end

  def test_file_rollup_storage_filters_keys_to_the_exact_namespace
    Dir.mktmpdir("event-meter-file-rollup-namespace") do |root|
      namespace = EventMeter.configuration.namespace
      storage = EventMeter::Stores::Rollup::File.new(
        path: root,
        namespace: namespace
      ).for_report(name: "invoice_delivery", version: DELIVERY_VERSION)

      storage.send(:update_json_file, storage.send(:rollup_bucket_path, :hour, "2026050601"), {}) do |bucket|
        bucket["all"] = { "count" => "1" }
      end

      keys = storage.keys_matching("#{namespace}:rollup:*")

      assert_equal ["#{namespace}:rollup:invoice_delivery:v1:hour:2026050601:all"], keys
    end
  end

  def test_file_rollup_cleanup_removes_old_processed_entry_markers
    configure_delivery_event
    stream_storage = flaky_delete_stream_storage

    Dir.mktmpdir("event-meter-file-rollup-processed") do |root|
      rollup_storage = EventMeter::Stores::Rollup::File.new(
        path: root,
        namespace: EventMeter.configuration.namespace
      ).for_report(name: "invoice_delivery", version: DELIVERY_VERSION)

      EventMeter.configure do |config|
        config.stream_storage = stream_storage
        config.rollup_storage = rollup_storage
      end

      record_delivery({
        customer_id: 44,
        provider: "postmark"
      }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)

      Time.stub(:now, utc(2026, 5, 6, 1, 0)) do
        process_delivery_pending
      end

      result = EventMeter.cleanup_history(before: utc(2026, 5, 6, 2, 0))

      assert_equal 1, result.fetch(:processed_entries_deleted)
    end
  end

  def test_file_rollup_top_level_cleanup_scans_report_folders
    configure_delivery_event
    stream_storage = flaky_delete_stream_storage

    Dir.mktmpdir("event-meter-file-rollup-top-level-cleanup") do |root|
      EventMeter.configure do |config|
        config.stream_storage = stream_storage
        config.rollup_storage = EventMeter::Stores::Rollup::File.new(path: root)
      end

      record_delivery({
        customer_id: 44,
        provider: "postmark"
      }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)

      Time.stub(:now, utc(2026, 5, 6, 1, 0)) do
        process_delivery_pending
      end

      result = EventMeter.cleanup_history(before: utc(2026, 5, 6, 2, 0))
      scoped_rollup = EventMeter.rollup_storage.for_report(name: "invoice_delivery", version: DELIVERY_VERSION)

      assert_equal 1, result.fetch(:processed_entries_deleted)
      assert_empty scoped_rollup.processed_ids(["1-0"])
    end
  end

  def test_file_stores_auto_cleanup_from_process_pending_removes_old_rollups_and_drained_stream_files
    configure_delivery_event
    current_time = utc(2026, 5, 6, 1, 0)

    Dir.mktmpdir("event-meter-file-auto-cleanup") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: root,
        clock: -> { current_time },
        claim_grace: 0
      )

      EventMeter.configure do |config|
        config.stream_storage = stream_storage
        config.rollup_storage = EventMeter::Stores::Rollup::File.new(path: root)
        config.auto_cleanup_history = true
        config.cleanup_history_retention = 60 * 60
        config.cleanup_history_interval = 60 * 60
      end

      record_delivery({
        customer_id: 44,
        provider: "postmark"
      }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)

      current_time = utc(2026, 5, 6, 3, 0)

      Time.stub(:now, current_time) do
        assert_equal expected_delivery_result(processed: 1), process_delivery_pending.to_h
      end

      summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 1),
        by: { provider: "postmark" }
      )

      assert_equal 0, summary.fetch(:count)
      assert_empty file_stream_logs(root)
      assert_empty file_stream_processing_logs(root)
    ensure
      stream_storage&.close
    end
  end

  def test_file_rollup_cleanup_removes_malformed_processed_entry_markers
    Dir.mktmpdir("event-meter-file-rollup-bad-processed") do |root|
      rollup_storage = EventMeter::Stores::Rollup::File.new(
        path: root,
        namespace: EventMeter.configuration.namespace
      ).for_report(name: "invoice_delivery", version: DELIVERY_VERSION)
      sidecar_path = rollup_storage.send(:processed_sidecar_path, "bad-stream.jsonl")

      FileUtils.mkdir_p(File.dirname(sidecar_path))
      File.write(sidecar_path, JSON.generate(
        "stream_file" => "bad-stream.jsonl",
        "processed_ids" => { "bad-entry" => "not-a-time" }
      ))

      result = rollup_storage.cleanup_history(
        before: utc(2026, 5, 6, 2, 0),
        events: nil,
        interval_state: true
      )

      assert_equal 1, result.fetch(:processed_entries_deleted)
      assert_empty rollup_storage.processed_ids(["bad-entry"])
    end
  end

  def test_file_rollup_storage_recovers_from_corrupt_bucket_files
    namespace = "event_meter:test"
    rollup_key = "#{namespace}:rollup:invoice_delivery:v1:minute:202605060100:provider=postmark"

    Dir.mktmpdir("event-meter-file-rollup-corrupt-bucket") do |root|
      rollup_storage = EventMeter::Stores::Rollup::File.new(
        path: root,
        namespace: namespace
      ).for_report(name: "invoice_delivery", version: DELIVERY_VERSION)
      bucket_path = rollup_storage.send(:rollup_bucket_path, :minute, "202605060100")

      FileUtils.mkdir_p(File.dirname(bucket_path))
      File.write(bucket_path, "not-json")
      assert_equal [{}], rollup_storage.hgetall_many([rollup_key])

      File.write(bucket_path, "[]")
      assert_equal [{}], rollup_storage.hgetall_many([rollup_key])
    end
  end

  def test_file_rollup_storage_recovers_from_corrupt_shard_files
    state_key = "event_meter:test:state:invoice_delivery:v1:interval:customer_id:44"

    Dir.mktmpdir("event-meter-file-rollup-corrupt-shards") do |root|
      rollup_storage = EventMeter::Stores::Rollup::File.new(
        path: root,
        namespace: EventMeter.configuration.namespace
      ).for_report(name: "invoice_delivery", version: DELIVERY_VERSION)
      string_path = rollup_storage.send(:shard_path, "strings", state_key)

      FileUtils.mkdir_p(File.dirname(string_path))
      File.write(string_path, "[]")
      assert_nil rollup_storage.get(state_key)
    end
  end

  def test_file_rollup_interval_state_treats_partially_numeric_values_as_corrupt
    configure_delivery_event
    stream_storage = memory_stream_storage
    namespace = EventMeter.configuration.namespace
    started_at = utc(2026, 5, 6, 1, 0)
    interval = delivery_report_definition.intervals.first
    state_key = EventMeter::Keys.interval_state(
      namespace: namespace,
      name: "invoice_delivery",
      version: DELIVERY_VERSION,
      definition: interval,
      value: 44
    )

    Dir.mktmpdir("event-meter-file-rollup-corrupt-interval-state") do |root|
      rollup_storage = EventMeter::Stores::Rollup::File.new(
        path: root,
        namespace: namespace
      ).for_report(name: "invoice_delivery", version: DELIVERY_VERSION)
      rollup_storage.send(:update_json_file, rollup_storage.send(:shard_path, "strings", state_key), {}) do |shard|
        shard[state_key] = "#{"9" * 40}bad"
      end

      EventMeter.configure do |config|
        config.stream_storage = stream_storage
        config.rollup_storage = rollup_storage
      end

      record_delivery({
        customer_id: 44,
        provider: "postmark"
      }, started_at: started_at, duration_ms: 100)

      process_delivery_pending

      assert_equal((started_at.to_f * 1000).to_i.to_s, rollup_storage.get(state_key))
    end
  end

  def test_redis_stream_read_returns_malformed_rows_to_the_processor
    redis = Struct.new(:rows) do
      attr_reader :xrange_options

      def xrange(_key, _start_id, _end_id, **options)
        @xrange_options = options
        count = options[:count] || rows.length
        rows.first(count)
      end
    end.new([
      ["bad-1", { "payload" => "not-json" }],
      ["bad-2", {}]
    ])
    stream = EventMeter::Stores::Stream::Redis.new(redis: redis, namespace: "event_meter:test")

    assert_equal [["bad-1", nil], ["bad-2", nil]], read_delivery_stream(stream)
    assert_equal({}, redis.xrange_options)
  end

  def test_redis_stream_can_use_configured_read_limit
    redis = Struct.new(:rows) do
      attr_reader :xrange_options

      def xrange(_key, _start_id, _end_id, **options)
        @xrange_options = options
        rows.first(options.fetch(:count))
      end
    end.new([
      ["1-0", { "payload" => JSON.generate("name" => "invoice_delivery") }],
      ["2-0", { "payload" => JSON.generate("name" => "invoice_delivery") }]
    ])
    stream = EventMeter::Stores::Stream::Redis.new(
      redis: redis,
      namespace: "event_meter:test",
      redis_read_limit: 1
    )

    assert_equal [["1-0", { "name" => "invoice_delivery" }]], read_delivery_stream(stream)
    assert_equal({ count: 1 }, redis.xrange_options)
  end

  def test_redis_stream_delete_finishes_the_latest_read_batch
    redis = Struct.new(:rows) do
      attr_reader :deleted_ids

      def xrange(_key, _start_id, _end_id, **_options)
        rows
      end

      def xdel(_key, *ids)
        @deleted_ids = ids
        rows.reject! { |id, _fields| ids.include?(id) }
      end
    end.new([
      ["1-0", { "payload" => JSON.generate("name" => "invoice_delivery") }],
      ["2-0", { "payload" => JSON.generate("name" => "invoice_delivery") }]
    ])
    stream = EventMeter::Stores::Stream::Redis.new(redis: redis, namespace: "event_meter:test")

    assert_equal ["1-0", "2-0"], read_delivery_stream(stream).map(&:first)

    stream.delete

    assert_equal ["1-0", "2-0"], redis.deleted_ids
    assert_empty read_delivery_stream(stream)
  end

  def test_redis_stream_storage_process_lock_is_exclusive
    redis = redis_client
    namespace = "event_meter:test:redis:stream-lock:#{Process.pid}:#{SecureRandom.hex(4)}"
    stream = EventMeter::Stores::Stream::Redis.new(redis: redis, namespace: namespace)
    nested_result = nil

    outer_result = stream.with_lock(ttl: 30) do
      nested_result = stream.with_lock(ttl: 30) { true }
      true
    end

    assert_equal true, outer_result
    assert_equal false, nested_result
    assert_equal true, stream.with_lock(ttl: 30) { true }
  ensure
    cleanup_redis(redis, namespace) if redis && namespace
  end

  def test_redis_stream_lock_can_use_a_dedicated_lock_client
    lock_redis = redis_lock_probe
    stream = EventMeter::Stores::Stream::Redis.new(
      redis: Object.new,
      lock_redis: lock_redis,
      namespace: "event_meter:test"
    )

    result = stream.with_lock(ttl: 30) { true }

    assert_equal true, result
    assert_equal %i[set eval], lock_redis.commands.map(&:first)
  end

  def test_redis_stream_lock_prevents_concurrent_processors_from_reading_same_rows
    redis = redis_client
    namespace = "event_meter:test:redis:stream-race:#{Process.pid}:#{SecureRandom.hex(4)}"
    read_started = Queue.new
    release_read = Queue.new
    slow_stream_class = Class.new(EventMeter::Stores::Stream::Redis) do
      define_method(:initialize) do |read_started:, release_read:, **options|
        @read_started = read_started
        @release_read = release_read
        super(**options)
      end

      def read(name:)
        entries = super
        @read_started << true
        @release_read.pop
        entries
      end
    end
    writer_stream = EventMeter::Stores::Stream::Redis.new(redis: redis, namespace: namespace)
    first_stream = slow_stream_class.new(
      redis: redis,
      namespace: namespace,
      read_started: read_started,
      release_read: release_read
    )
    second_stream = EventMeter::Stores::Stream::Redis.new(redis: redis, namespace: namespace)
    rollup_storage = EventMeter::Stores::Rollup::Redis.new(redis: redis, namespace: namespace)

    2.times do |index|
      append_delivery(writer_stream,
        customer_id: 44 + index,
        provider: "postmark",
        started_at: utc(2026, 5, 6, 1, index),
        duration_ms: 100)
    end

    EventMeter.configure do |config|
      config.namespace = namespace
      config.stream_storage = first_stream
      config.rollup_storage = rollup_storage
    end

    first_thread = Thread.new { process_delivery_indexes_pending }
    read_started.pop

    EventMeter.configure do |config|
      config.stream_storage = second_stream
      config.rollup_storage = rollup_storage
    end
    second_result = process_delivery_indexes_pending

    release_read << true
    first_result = first_thread.value
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 2),
      by: { provider: "postmark" }
    )

    assert_equal expected_delivery_result(processed: 2), first_result.to_h
    assert_equal expected_delivery_result(processed: 0, complete: false, locked: true), second_result.to_h
    assert_equal 2, summary.fetch(:count)
    assert_equal 200, summary.fetch(:duration_ms_sum)
    assert_empty redis_processed_keys(redis, namespace)
    assert_empty second_stream.read(name: DELIVERY_EVENT)
  ensure
    release_read << true if release_read
    cleanup_redis(redis, namespace) if redis && namespace
  end

  def test_redis_rollup_storage_escapes_namespace_globs
    redis = Struct.new(:keys) do
      attr_reader :last_match

      def scan_each(match:)
        @last_match = match
        keys.each { |key| yield key }
      end
    end.new([])
    storage = EventMeter::Stores::Rollup::Redis.new(
      redis: redis,
      namespace: "event*meter[prod]"
    )

    storage.keys_matching("event*meter[prod]:rollup:*")

    assert_equal "event\\*meter\\[prod\\]:rollup:*", redis.last_match
  end

  def test_redis_rollup_storage_filters_scan_results_to_the_exact_namespace
    redis = Struct.new(:keys) do
      def scan_each(match:)
        keys.each { |key| yield key }
      end
    end.new([
      "event*meter[prod]:rollup:invoice_delivery:hour:2026050601:all",
      "eventXmeterp:rollup:invoice_delivery:hour:2026050601:all"
    ])
    storage = EventMeter::Stores::Rollup::Redis.new(
      redis: redis,
      namespace: "event*meter[prod]"
    )

    keys = storage.keys_matching("event*meter[prod]:rollup:*")

    assert_equal ["event*meter[prod]:rollup:invoice_delivery:hour:2026050601:all"], keys
  end

  def test_redis_rollup_storage_rejects_bad_lock_ttls_with_a_clear_error
    storage = EventMeter::Stores::Rollup::Redis.new(
      redis: Object.new,
      namespace: "event_meter:test"
    )

    error = assert_raises(ArgumentError) do
      storage.send(:redis_lock_ttl, Float::NAN)
    end

    assert_equal "lock ttl must be positive", error.message
  end

  def test_redis_rollup_lock_can_use_a_dedicated_lock_client_after_scoping
    lock_redis = redis_lock_probe
    storage = EventMeter::Stores::Rollup::Redis.new(
      redis: Object.new,
      lock_redis: lock_redis,
      namespace: "event_meter:test"
    ).for_report(name: DELIVERY_EVENT, version: DELIVERY_VERSION)

    result = storage.with_lock(ttl: 30) { true }

    assert_equal true, result
    assert_same lock_redis, storage.lock_redis
    assert_equal %i[set eval], lock_redis.commands.map(&:first)
  end

  def test_redis_lock_refresher_interrupts_owner_when_refresh_fails
    redis = Class.new do
      def set(_key, _value, nx: false, ex: nil)
        true
      end

      def eval(script, keys:, argv:)
        script.include?("expire") ? 0 : 1
      end
    end.new
    storage = EventMeter::Stores::Rollup::Redis.new(
      redis: redis,
      namespace: "event_meter:test"
    ).for_report(name: DELIVERY_EVENT, version: DELIVERY_VERSION)

    error = assert_raises(EventMeter::LockLostError) do
      storage.with_lock(ttl: 1) { sleep 2 }
    end

    assert_includes error.message, "redis lock refresh failed"
    refute Thread.list.any? { |thread| thread.name == "event_meter redis lock refresher" }
  end

  def test_redis_lock_refresher_stops_cooperatively
    redis = Class.new do
      attr_reader :scripts

      def initialize
        @values = {}
        @scripts = []
      end

      def set(key, value, nx: false, ex: nil)
        return false if nx && @values.key?(key)

        @values[key] = value
        true
      end

      def eval(script, keys:, argv:)
        @scripts << script

        if script.include?("expire")
          @values[keys.first] == argv.first ? 1 : 0
        else
          @values.delete(keys.first) if @values[keys.first] == argv.first
          1
        end
      end
    end.new
    storage = EventMeter::Stores::Rollup::Redis.new(
      redis: redis,
      namespace: "event_meter:test"
    )

    result = storage.with_lock(ttl: 1) do
      sleep 1.1
      true
    end

    assert_equal true, result
    assert_operator redis.scripts.count { |script| script.include?("expire") }, :>=, 1
    refute Thread.list.any? { |thread| thread.name == "event_meter redis lock refresher" }
  end

  def test_redis_scripts_replace_corrupt_existing_numbers_and_reject_corrupt_incoming_values
    assert_includes EventMeter::Stores::Rollup::Redis::MIN_FIELD_SCRIPT, "current_number == nil"
    assert_includes EventMeter::Stores::Rollup::Redis::MAX_FIELD_SCRIPT, "current_number == nil"
    assert_includes EventMeter::Stores::Rollup::Redis::MIN_FIELD_SCRIPT, "if value == nil"
    assert_includes EventMeter::Stores::Rollup::Redis::MAX_FIELD_SCRIPT, "if value == nil"
    assert_includes EventMeter::Stores::Rollup::Redis::MIN_FIELD_SCRIPT, "redis.error_reply"
    assert_includes EventMeter::Stores::Rollup::Redis::MAX_FIELD_SCRIPT, "redis.error_reply"
    assert_includes EventMeter::Stores::Rollup::Redis::SET_MAX_SCRIPT, "current_number == nil"
    assert_includes EventMeter::Stores::Rollup::Redis::SET_MAX_SCRIPT, "if value == nil"
    assert_includes EventMeter::Stores::Rollup::Redis::SET_MAX_SCRIPT, "redis.error_reply"
  end

  def test_redis_stream_and_rollup_stores_process_against_real_redis
    redis = redis_client
    namespace = "event_meter:test:redis:#{Process.pid}:#{SecureRandom.hex(4)}"

    EventMeter.configure do |config|
      config.namespace = namespace
      config.stream_storage = EventMeter::Stores::Stream::Redis.new(redis: redis, namespace: namespace)
      config.rollup_storage = EventMeter::Stores::Rollup::Redis.new(redis: redis, namespace: namespace)
    end

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
    record_delivery({
      customer_id: 45,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 1), duration_ms: 200)
    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 5), duration_ms: 150)

    result = process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 6),
      by: { provider: "postmark" }
    )
    cleanup = EventMeter.cleanup_history(before: utc(2026, 5, 6, 2, 0), events: ["invoice_delivery"])

    assert_equal expected_delivery_result(processed: 3), result.to_h
    assert_equal 3, summary.fetch(:count)
    assert_equal 450, summary.fetch(:duration_ms_sum)
    assert_equal 1, summary.fetch(:interval_ms_count)
    assert_equal 5 * 60 * 1000, summary.fetch(:interval_ms_sum)
    assert_operator cleanup.fetch(:rollup_keys_deleted), :>, 0
    assert_operator cleanup.fetch(:interval_state_keys_deleted), :>, 0
    assert_empty EventMeter.stream_storage.read(name: DELIVERY_EVENT)
  ensure
    cleanup_redis(redis, namespace) if redis && namespace
  end

  def test_redis_rollup_storage_skips_duplicate_rows_after_late_stream_delete
    redis = redis_client
    namespace = "event_meter:test:redis:retry:#{Process.pid}:#{SecureRandom.hex(4)}"
    stream_storage = flaky_delete_stream_storage

    EventMeter.configure do |config|
      config.namespace = namespace
      config.stream_storage = stream_storage
      config.rollup_storage = EventMeter::Stores::Rollup::Redis.new(redis: redis, namespace: namespace)
    end

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)

    first_result = process_delivery_pending
    first_processed_keys = redis_processed_keys(redis, namespace)
    second_result = process_delivery_pending
    second_processed_keys = redis_processed_keys(redis, namespace)
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1),
      by: { provider: "postmark" }
    )

    assert_equal expected_delivery_result(processed: 1, complete: false), first_result.to_h
    assert_equal expected_delivery_result(processed: 0, skipped: 1), second_result.to_h
    assert_equal ["#{namespace}:processed:invoice_delivery:v1:1-0"], first_processed_keys
    assert_empty second_processed_keys
    assert_equal 1, summary.fetch(:count)
    assert_equal 100, summary.fetch(:duration_ms_sum)
  ensure
    cleanup_redis(redis, namespace) if redis && namespace
  end

  def test_redis_processed_ids_are_scoped_by_event_name_and_version
    redis = redis_client
    namespace = "event_meter:test:redis:scoped:#{Process.pid}:#{SecureRandom.hex(4)}"
    storage = EventMeter::Stores::Rollup::Redis.new(redis: redis, namespace: namespace)
    first_report = storage.for_report(name: "invoice_delivery", version: 1)
    second_report = storage.for_report(name: "receipt_delivery", version: 1)
    next_version = storage.for_report(name: "invoice_delivery", version: 2)

    first_report.apply(fake_processed_batch(["same-stream-id"]))

    assert_equal ["same-stream-id"], first_report.processed_ids(["same-stream-id"])
    assert_empty second_report.processed_ids(["same-stream-id"])
    assert_empty next_version.processed_ids(["same-stream-id"])

    first_report.forget_processed_ids(["same-stream-id"])

    assert_empty redis_processed_keys(redis, namespace)
  ensure
    cleanup_redis(redis, namespace) if redis && namespace
  end

  def test_redis_cleanup_history_removes_old_processed_ids_for_selected_events
    redis = redis_client
    namespace = "event_meter:test:redis:processed-cleanup:#{Process.pid}:#{SecureRandom.hex(4)}"
    storage = EventMeter::Stores::Rollup::Redis.new(redis: redis, namespace: namespace)
    deliveries = storage.for_report(name: "invoice_delivery", version: 1)
    receipts = storage.for_report(name: "receipt_delivery", version: 1)

    deliveries.apply(fake_processed_batch(["old-delivery", "new-delivery", "corrupt-delivery"]))
    receipts.apply(fake_processed_batch(["old-receipt"]))

    redis.set(redis_processed_key(namespace, "invoice_delivery", 1, "old-delivery"), utc(2026, 5, 6, 1, 0).iso8601(6))
    redis.set(redis_processed_key(namespace, "invoice_delivery", 1, "new-delivery"), utc(2026, 5, 6, 3, 0).iso8601(6))
    redis.set(redis_processed_key(namespace, "invoice_delivery", 1, "corrupt-delivery"), "not-a-time")
    redis.set(redis_processed_key(namespace, "receipt_delivery", 1, "old-receipt"), utc(2026, 5, 6, 1, 0).iso8601(6))

    result = storage.cleanup_history(
      before: utc(2026, 5, 6, 2, 0),
      events: ["invoice_delivery"],
      interval_state: false
    )

    assert_equal 2, result.fetch(:processed_entries_deleted)
    assert_equal ["new-delivery"], deliveries.processed_ids(["old-delivery", "new-delivery", "corrupt-delivery"])
    assert_equal ["old-receipt"], receipts.processed_ids(["old-receipt"])
  ensure
    cleanup_redis(redis, namespace) if redis && namespace
  end

  def test_redis_interval_cleanup_pipelines_state_value_reads
    namespace = "event_meter:test:redis:interval-cleanup"
    before = utc(2026, 5, 6, 2, 0)
    old_key = "#{namespace}:state:invoice_delivery:v1:interval:customer_id:old"
    new_key = "#{namespace}:state:invoice_delivery:v1:interval:customer_id:new"
    corrupt_key = "#{namespace}:state:invoice_delivery:v1:interval:customer_id:corrupt"
    other_event_key = "#{namespace}:state:receipt_delivery:v1:interval:customer_id:old"
    redis = redis_cleanup_probe(
      old_key => ((before - 60).to_f * 1000).to_i.to_s,
      new_key => ((before + 60).to_f * 1000).to_i.to_s,
      corrupt_key => "not-a-timestamp",
      other_event_key => ((before - 60).to_f * 1000).to_i.to_s
    )
    storage = EventMeter::Stores::Rollup::Redis.new(redis: redis, namespace: namespace)

    deleted = storage.send(:cleanup_interval_state, before, ["invoice_delivery"])

    assert_equal 2, deleted
    assert_empty redis.get_calls
    assert_equal [old_key, new_key, corrupt_key], redis.pipelined_gets
    assert_equal [[old_key, corrupt_key]], redis.deleted_batches
  end

  def test_postgres_rollup_storage_exposes_schema_sql
    sql = EventMeter::Stores::Rollup::Postgres.schema_sql(table_prefix: "event_meter")

    assert_includes sql, "event_meter_rollups"
    assert_includes sql, "event_meter_processed_entries"
    assert_includes sql, "event_meter_processed_created_at_idx"
    assert_includes sql, "event_meter_rollups_key_prefix_idx"
    assert_includes sql, "event_meter_strings_key_prefix_idx"

    assert_raises(ArgumentError) do
      EventMeter::Stores::Rollup::Postgres.schema_sql(table_prefix: "event-meter")
    end
  end

  def test_postgres_install_executes_schema_sql
    connection = fake_postgres_connection([])

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: "event_meter_test"
    )

    assert_equal 1, connection.calls.length
    assert_includes connection.calls.first.fetch(:sql), "event_meter_test_rollups"
    assert_equal [], connection.calls.first.fetch(:params)
  end

  def test_postgres_rollup_storage_escapes_like_prefixes
    connection = fake_postgres_connection([
      [
        { "key" => "event*meter:%_v1:rollup:invoice_delivery:hour:2026050601:all" },
        { "key" => "eventXmeter:%_v1:rollup:invoice_delivery:hour:2026050601:all" }
      ]
    ])
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: "event*meter:%_v1"
    )

    keys = storage.keys_matching("event*meter:%_v1:rollup:*")

    assert_equal ["event*meter:%_v1:rollup:invoice_delivery:hour:2026050601:all"], keys
    assert_includes connection.calls.first.fetch(:sql), "ESCAPE '\\'"
    assert_equal ["event*meter:\\%\\_v1:rollup:%"], connection.calls.first.fetch(:params)
  end

  def test_postgres_rollup_cleanup_deletes_by_bucket_prefix_in_sql
    connection = fake_postgres_connection([[{ "count" => "12" }]])
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: "event_meter:test"
    )

    deleted = storage.send(:cleanup_rollups, utc(2026, 5, 6, 2, 0), ["invoice_delivery"])

    assert_equal 12, deleted

    call = connection.calls.fetch(0)
    assert_includes call.fetch(:sql), "DELETE FROM event_meter_rollups"
    assert_includes call.fetch(:sql), "split_part"
    assert_includes call.fetch(:sql), "minute"
    assert_includes call.fetch(:sql), "hour"
    refute_includes call.fetch(:sql), "SELECT key FROM event_meter_rollups"
    assert_equal [
      "event_meter:test:rollup:",
      "event\\_meter:test:rollup:%",
      "202605060200",
      "2026050602",
      "invoice_delivery"
    ], call.fetch(:params)
  end

  def test_postgres_interval_cleanup_deletes_old_state_in_sql
    connection = fake_postgres_connection([[{ "count" => "7" }]])
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: "event_meter:test"
    )

    deleted = storage.send(:cleanup_interval_state, utc(2026, 5, 6, 2, 0), nil)

    assert_equal 7, deleted

    call = connection.calls.fetch(0)
    assert_includes call.fetch(:sql), "DELETE FROM event_meter_strings"
    assert_includes call.fetch(:sql), "regexp_replace(ltrim(value, '-')"
    assert_includes call.fetch(:sql), "9223372036854775807"
    refute_includes call.fetch(:sql), "value::bigint < $2"
    refute_includes call.fetch(:sql), "SELECT key, value FROM event_meter_strings"
    assert_equal [
      "event\\_meter:test:state:%",
      utc(2026, 5, 6, 2, 0).to_f.to_i * 1000
    ], call.fetch(:params)
  end

  def test_postgres_cleanup_history_returns_the_cleanup_counts
    connection = fake_postgres_connection([
      [],
      [{ "count" => "0" }],
      []
    ])
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: "event_meter:test"
    )

    result = storage.cleanup_history(
      before: utc(2026, 5, 6, 2, 0),
      events: [],
      interval_state: false
    )

    assert_equal({
      rollup_keys_deleted: 0,
      interval_state_keys_deleted: 0,
      processed_entries_deleted: 0
    }, result)
    assert_equal "BEGIN", connection.calls.first.fetch(:sql)
    assert_equal "COMMIT", connection.calls.last.fetch(:sql)
  end

  def test_postgres_processed_ids_chunks_large_queries
    ids = Array.new(1_001) { |index| "entry-#{index}" }
    connection = fake_postgres_connection([
      [{ "entry_id" => "entry-0" }],
      [{ "entry_id" => "entry-1000" }]
    ])
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: "event_meter:test"
    ).for_report(name: DELIVERY_EVENT, version: DELIVERY_VERSION)

    result = storage.processed_ids(ids)

    assert_equal ["entry-0", "entry-1000"], result
    assert_equal [1_000, 7], connection.calls.map { |call| call.fetch(:params).length }
  end

  def test_postgres_processed_ids_are_scoped_by_event_name_and_version
    connection = postgres_connection
    namespace = "event_meter:test:postgres:scoped:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: namespace,
      table_prefix: table_prefix
    )
    first_report = storage.for_report(name: "invoice_delivery", version: 1)
    second_report = storage.for_report(name: "receipt_delivery", version: 1)
    next_version = storage.for_report(name: "invoice_delivery", version: 2)

    first_report.apply(fake_processed_batch(["same-stream-id"]))

    assert_equal ["same-stream-id"], first_report.processed_ids(["same-stream-id"])
    assert_empty second_report.processed_ids(["same-stream-id"])
    assert_empty next_version.processed_ids(["same-stream-id"])

    first_report.forget_processed_ids(["same-stream-id"])

    assert_equal 0, postgres_processed_count(connection, table_prefix)
  ensure
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_cleanup_history_removes_old_processed_ids_for_selected_events
    connection = postgres_connection
    namespace = "event_meter:test:postgres:cleanup:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    stream_storage = nil
    fresh_invoice_id = nil
    receipt_id = nil

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    Dir.mktmpdir("event-meter-postgres-cleanup-selected") do |root|
      stream_storage = always_incomplete_file_stream_storage(File.join(root, "stream"))

      EventMeter.configure do |config|
        config.namespace = namespace
        config.stream_storage = stream_storage
        config.rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
          connection: connection,
          namespace: namespace,
          table_prefix: table_prefix
        )
      end

      write_to_previous_stream_bucket do
        record_delivery({
          customer_id: 44,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
      end
      assert_equal expected_delivery_result(processed: 1, complete: false), process_delivery_pending.to_h
      old_invoice_id = postgres_processed_entry_ids(connection, table_prefix,
        namespace: namespace,
        event_name: "invoice_delivery").fetch(0)
      update_postgres_processed_created_at(connection, table_prefix,
        namespace: namespace,
        event_name: "invoice_delivery",
        entry_id: old_invoice_id,
        created_at: utc(2026, 5, 6, 1, 0))

      write_to_previous_stream_bucket do
        record_delivery({
          customer_id: 45,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 1), duration_ms: 200)
      end
      assert_equal expected_delivery_result(processed: 1, skipped: 1, complete: false), process_delivery_pending.to_h
      fresh_invoice_id = (postgres_processed_entry_ids(connection, table_prefix,
        namespace: namespace,
        event_name: "invoice_delivery") - [old_invoice_id]).fetch(0)
      update_postgres_processed_created_at(connection, table_prefix,
        namespace: namespace,
        event_name: "invoice_delivery",
        entry_id: fresh_invoice_id,
        created_at: utc(2026, 5, 6, 3, 0))

      write_to_previous_stream_bucket do
        append_event("receipt_delivery", {
          customer_id: 46,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 2), duration_ms: 300)
      end
      receipt_result = EventMeter.process_pending("receipt_delivery", version: DELIVERY_VERSION) do |report|
        report.index_by(:provider)
      end
      assert_equal({
        event_name: "receipt_delivery",
        version: DELIVERY_VERSION,
        processed: 1,
        skipped_already_processed: 0,
        malformed: 0,
        complete: false,
        locked: false
      }, receipt_result.to_h)
      receipt_id = postgres_processed_entry_ids(connection, table_prefix,
        namespace: namespace,
        event_name: "receipt_delivery").fetch(0)
      update_postgres_processed_created_at(connection, table_prefix,
        namespace: namespace,
        event_name: "receipt_delivery",
        entry_id: receipt_id,
        created_at: utc(2026, 5, 6, 1, 0))

      invoice_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 3),
        by: { provider: "postmark" }
      )
      receipt_summary = EventMeter.summary("receipt_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 3),
        by: { provider: "postmark" }
      )

      assert_equal 2, invoice_summary.fetch(:count)
      assert_equal 1, receipt_summary.fetch(:count)
    end

    result = EventMeter.cleanup_history(
      before: utc(2026, 5, 6, 2, 0),
      events: ["invoice_delivery"],
      interval_state: false
    )

    rows = connection.exec(<<~SQL).to_a
      SELECT event_name, entry_id
      FROM #{table_prefix}_processed_entries
      ORDER BY event_name, entry_id
    SQL

    assert_equal 1, result.fetch(:processed_entries_deleted)
    assert_equal [
      {
        "event_name" => EventMeter::Keys.event_name("invoice_delivery"),
        "entry_id" => fresh_invoice_id
      },
      {
        "event_name" => EventMeter::Keys.event_name("receipt_delivery"),
        "entry_id" => receipt_id
      }
    ], rows
  ensure
    stream_storage&.close
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_cleanup_history_does_not_delete_processed_ids_from_other_namespaces
    connection = postgres_connection
    namespace = "event_meter:test:postgres:cleanup:#{Process.pid}:#{SecureRandom.hex(4)}"
    other_namespace = "#{namespace}:other"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    stream_storages = []
    other_entry_id = nil

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    Dir.mktmpdir("event-meter-postgres-cleanup-namespace-a") do |root_a|
      Dir.mktmpdir("event-meter-postgres-cleanup-namespace-b") do |root_b|
        [
          [namespace, root_a, 44],
          [other_namespace, root_b, 45]
        ].each do |current_namespace, root, customer_id|
          stream_storage = always_incomplete_file_stream_storage(File.join(root, "stream"))
          stream_storages << stream_storage

          EventMeter.configure do |config|
            config.namespace = current_namespace
            config.stream_storage = stream_storage
            config.rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
              connection: connection,
              namespace: current_namespace,
              table_prefix: table_prefix
            )
          end

          write_to_previous_stream_bucket do
            record_delivery({
              customer_id: customer_id,
              provider: "postmark"
            }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
          end

          assert_equal expected_delivery_result(processed: 1, complete: false), process_delivery_pending.to_h
          summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
            from: utc(2026, 5, 6, 1, 0),
            to: utc(2026, 5, 6, 1, 1),
            by: { provider: "postmark" }
          )

          assert_equal 1, summary.fetch(:count)
          update_postgres_processed_created_at(connection, table_prefix,
            namespace: current_namespace,
            event_name: "invoice_delivery",
            created_at: utc(2026, 5, 6, 1, 0))
          other_entry_id = postgres_processed_entry_ids(connection, table_prefix,
            namespace: current_namespace,
            event_name: "invoice_delivery").fetch(0) if current_namespace == other_namespace
        end
      end
    end

    EventMeter.configure do |config|
      config.namespace = namespace
      config.rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
        connection: connection,
        namespace: namespace,
        table_prefix: table_prefix
      )
    end
    result = EventMeter.cleanup_history(
      before: utc(2026, 5, 6, 2, 0),
      events: nil,
      interval_state: false
    )

    rows = connection.exec(<<~SQL).to_a
      SELECT namespace, entry_id
      FROM #{table_prefix}_processed_entries
      ORDER BY entry_id
    SQL

    assert_equal 1, result.fetch(:processed_entries_deleted)
    assert_equal [
      {
        "namespace" => other_namespace,
        "entry_id" => other_entry_id
      }
    ], rows
  ensure
    stream_storages&.each(&:close)
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_hgetall_many_chunks_large_queries_and_preserves_key_order
    keys = Array.new(1_001) { |index| "rollup-#{index}" }
    connection = fake_postgres_connection([
      [{ "key" => "rollup-999", "fields" => JSON.generate("count" => "3") }],
      [{ "key" => "rollup-1000", "fields" => { "count" => "4" } }]
    ])
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: "event_meter:test"
    )

    result = storage.hgetall_many(keys)

    assert_equal({}, result.fetch(0))
    assert_equal({ "count" => "3" }, result.fetch(999))
    assert_equal({ "count" => "4" }, result.fetch(1000))
    assert_equal [1_000, 1], connection.calls.map { |call| call.fetch(:params).length }
  end

  def test_postgres_hgetall_many_treats_corrupt_hash_values_as_empty
    connection = fake_postgres_connection([
      [{ "key" => "rollup-1", "fields" => JSON.generate(["not", "a hash"]) }]
    ])
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: "event_meter:test"
    )

    assert_equal [{}], storage.hgetall_many(["rollup-1"])
  end

  def test_postgres_hgetall_many_treats_invalid_json_as_empty
    connection = fake_postgres_connection([
      [{ "key" => "rollup-1", "fields" => "not-json" }]
    ])
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: "event_meter:test"
    )

    assert_equal [{}], storage.hgetall_many(["rollup-1"])
  end

  def test_postgres_delete_by_key_chunks_large_queries
    keys = Array.new(1_001) { |index| "rollup-#{index}" }
    connection = fake_postgres_connection([[], []])
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: "event_meter:test"
    )

    storage.send(:delete_by_key, "event_meter_rollups", keys)

    assert_equal [1_000, 1], connection.calls.map { |call| call.fetch(:params).length }
    assert_equal 2, connection.calls.length
  end

  def test_postgres_interval_state_upsert_tolerates_corrupt_existing_values
    connection = fake_postgres_connection([[]])
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: "event_meter:test"
    )

    storage.send(:upsert_max_strings, "state-key" => "100")

    sql = connection.calls.first.fetch(:sql)
    assert_includes sql, "WHEN event_meter_strings.value ~ '^-?[0-9]+$'"
    assert_includes sql, "GREATEST"
  end

  def test_postgres_process_lock_id_is_stable_and_uses_signed_bigint_range
    first_storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: fake_postgres_connection([]),
      namespace: "event_meter:test"
    )
    second_storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: fake_postgres_connection([]),
      namespace: "event_meter:other"
    )

    first_lock_id = first_storage.send(:lock_id)

    assert_equal first_lock_id, first_storage.send(:lock_id)
    assert_operator first_lock_id, :>=, 0
    assert_operator first_lock_id, :<=, 9_223_372_036_854_775_807
    refute_equal first_lock_id, second_storage.send(:lock_id)
  end

  def test_postgres_process_locks_are_scoped_by_report_name_and_version
    first_connection = postgres_connection
    second_connection = postgres_connection
    namespace = "event_meter:test:postgres:lock:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: first_connection,
      table_prefix: table_prefix
    )
    first_storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: first_connection,
      namespace: namespace,
      table_prefix: table_prefix
    )
    second_storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: second_connection,
      namespace: namespace,
      table_prefix: table_prefix
    )
    first_report = first_storage.for_report(name: "invoice_delivery", version: 1)
    same_report = second_storage.for_report(name: "invoice_delivery", version: 1)
    next_version = second_storage.for_report(name: "invoice_delivery", version: 2)
    other_report = second_storage.for_report(name: "receipt_delivery", version: 1)
    same_report_result = nil
    next_version_result = nil
    other_report_result = nil

    first_report.with_lock(ttl: 30) do
      same_report_result = same_report.with_lock(ttl: 30) { true }
      next_version_result = next_version.with_lock(ttl: 30) { true }
      other_report_result = other_report.with_lock(ttl: 30) { true }
    end

    assert_equal false, same_report_result
    assert_equal true, next_version_result
    assert_equal true, other_report_result
  ensure
    drop_postgres_tables(first_connection, table_prefix) if first_connection && table_prefix
    first_connection&.close
    second_connection&.close
  end

  def test_postgres_process_lock_can_replace_expired_lease
    connection = postgres_connection
    namespace = "event_meter:test:postgres:expired-lock:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: namespace,
      table_prefix: table_prefix
    ).for_report(name: "invoice_delivery", version: 1)
    connection.exec_params(
      "INSERT INTO #{table_prefix}_strings (key, value) VALUES ($1, $2)",
      [storage.send(:lock_key), "1:stale"]
    )

    assert_equal true, storage.with_lock(ttl: 30) { true }
  ensure
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_rollup_storage_opens_independent_lock_connection_for_pg
    connection = postgres_connection
    namespace = "event_meter:test:postgres:independent-lock:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    storage = nil
    lock_connection = nil

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: namespace,
      table_prefix: table_prefix
    )
    lock_connection = storage.send(:lock_connection)

    refute_same connection, lock_connection
    assert_equal true, storage.for_report(name: "invoice_delivery", version: 1).with_lock(ttl: 30) { true }
  ensure
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    lock_connection&.close if lock_connection && lock_connection != connection
    connection&.close
  end

  def test_postgres_process_lock_treats_out_of_range_lease_expiry_as_expired
    connection = postgres_connection
    namespace = "event_meter:test:postgres:huge-expired-lock:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: namespace,
      table_prefix: table_prefix
    ).for_report(name: "invoice_delivery", version: 1)
    connection.exec_params(
      "INSERT INTO #{table_prefix}_strings (key, value) VALUES ($1, $2)",
      [storage.send(:lock_key), "#{"9" * 40}:stale"]
    )

    assert_equal true, storage.with_lock(ttl: 30) { true }
  ensure
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_lock_refresher_interrupts_owner_when_refresh_fails
    storage = Class.new(EventMeter::Stores::Rollup::Postgres) do
      attr_reader :released

      private

      def acquire_lock_lease(_token, _ttl)
        true
      end

      def refresh_lock_lease(_token, _ttl)
        false
      end

      def release_lock_lease(_token)
        @released = true
      end
    end.new(
      connection: fake_postgres_connection([]),
      lock_connection: fake_postgres_connection([]),
      namespace: "event_meter:test"
    )

    error = assert_raises(EventMeter::LockLostError) do
      storage.with_lock(ttl: 1) { sleep 2 }
    end

    assert_includes error.message, "postgres lock refresh failed"
    assert_equal true, storage.released
    refute Thread.list.any? { |thread| thread.name == "event_meter postgres lock refresher" }
  end

  def test_postgres_lock_refresher_uses_lock_connection_during_long_transaction
    main_connection = fake_postgres_connection([])
    lock_connection = refreshing_fake_postgres_lock_connection
    storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: main_connection,
      lock_connection: lock_connection,
      namespace: "event_meter:test"
    ).for_report(name: "invoice_delivery", version: 1)

    result = Timeout.timeout(3) do
      storage.with_lock(ttl: 1) do
        storage.send(:transaction) do
          lock_connection.wait_for_refresh
        end
      end
    end

    assert_equal true, result
    assert_operator lock_connection.refresh_count, :>=, 1
    assert_equal(
      %w[BEGIN COMMIT],
      main_connection.calls.filter_map { |call| call.fetch(:sql) if call.fetch(:params).empty? }
    )
    refute Thread.list.any? { |thread| thread.name == "event_meter postgres lock refresher" }
  end

  def test_postgres_rollup_store_merges_concurrent_streams_against_real_postgres
    connection = postgres_connection
    namespace = "event_meter:test:postgres:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    config = delivery_config(namespace: namespace)
    streams = nil

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )
    rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: namespace,
      table_prefix: table_prefix
    )

    Dir.mktmpdir("event-meter-postgres-stream-a") do |root_a|
      Dir.mktmpdir("event-meter-postgres-stream-b") do |root_b|
        streams = [
          EventMeter::Stores::Stream::File.new(path: root_a, sync: :flush),
          EventMeter::Stores::Stream::File.new(path: root_b, sync: :flush)
        ]

        write_to_previous_stream_bucket do
          append_delivery(streams[0],
            customer_id: 44,
            provider: "postmark",
            started_at: utc(2026, 5, 6, 1, 0, 1),
            duration_ms: 100)
          append_delivery(streams[1],
            customer_id: 45,
            provider: "postmark",
            started_at: utc(2026, 5, 6, 1, 0, 2),
            duration_ms: 250)
        end

        processors = streams.map do |stream|
          EventMeter::Processor.new(
            configuration: config,
            report_definition: delivery_indexes_definition,
            stream_storage: stream,
            rollup_storage: rollup_storage
          )
        end
        results = run_concurrently(processors) { |processor| processor.process }
        summary = EventMeter::Reports.new(
          configuration: config,
          rollup_storage: rollup_storage
        ).summary("invoice_delivery", version: DELIVERY_VERSION,
          from: utc(2026, 5, 6, 1, 0),
          to: utc(2026, 5, 6, 1, 1),
          by: { provider: "postmark" }
        )

        assert_equal [
          expected_delivery_result(processed: 1),
          expected_delivery_result(processed: 1)
        ], results.map(&:to_h)
        assert_equal 2, summary.fetch(:count)
        assert_equal 350, summary.fetch(:duration_ms_sum)
      ensure
        streams&.each(&:close)
      end
    end
  ensure
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_rollup_store_processes_reports_intervals_and_cleanup
    connection = postgres_connection
    namespace = "event_meter:test:postgres:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    EventMeter.configure do |config|
      config.namespace = namespace
      config.stream_storage = memory_stream_storage
      config.rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
        connection: connection,
        namespace: namespace,
        table_prefix: table_prefix
      )
    end

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 4), duration_ms: 150)
    record_delivery({
      customer_id: 45,
      provider: "mailgun"
    }, started_at: utc(2026, 5, 6, 1, 5), duration_ms: 200)

    result = process_delivery_pending
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 6),
      by: { provider: "postmark" }
    )
    cleanup = EventMeter.cleanup_history(before: Time.now.utc + 60)

    assert_equal expected_delivery_result(processed: 3), result.to_h
    assert_equal 2, summary.fetch(:count)
    assert_equal 250, summary.fetch(:duration_ms_sum)
    assert_equal 1, summary.fetch(:interval_ms_count)
    assert_equal 4 * 60 * 1000, summary.fetch(:interval_ms_sum)
    assert_operator cleanup.fetch(:rollup_keys_deleted), :>, 0
    assert_operator cleanup.fetch(:interval_state_keys_deleted), :>, 0
    assert_equal 0, cleanup.fetch(:processed_entries_deleted)
  ensure
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_auto_cleanup_runs_from_process_pending
    connection = postgres_connection
    namespace = "event_meter:test:postgres:auto_cleanup:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
      connection: connection,
      namespace: namespace,
      table_prefix: table_prefix
    )

    EventMeter.configure do |config|
      config.namespace = namespace
      config.stream_storage = memory_stream_storage
      config.rollup_storage = rollup_storage
      config.auto_cleanup_history = true
      config.cleanup_history_retention = 60 * 60
      config.cleanup_history_interval = 60 * 60
    end

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)

    Time.stub(:now, utc(2026, 5, 6, 3, 0)) do
      assert_equal expected_delivery_result(processed: 1), process_delivery_pending.to_h
    end

    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1),
      by: { provider: "postmark" }
    )

    assert_equal 0, summary.fetch(:count)
    assert rollup_storage.cleanup_watermark("#{namespace}:auto_cleanup:history:last_run")
  ensure
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_rollup_storage_skips_duplicate_rows_after_late_stream_delete
    connection = postgres_connection
    namespace = "event_meter:test:postgres:retry:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    stream_storage = flaky_delete_stream_storage

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    EventMeter.configure do |config|
      config.namespace = namespace
      config.stream_storage = stream_storage
      config.rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
        connection: connection,
        namespace: namespace,
        table_prefix: table_prefix
      )
    end

    record_delivery({
      customer_id: 44,
      provider: "postmark"
    }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)

    first_result = process_delivery_pending
    first_processed_count = postgres_processed_count(connection, table_prefix)
    second_result = process_delivery_pending
    second_processed_count = postgres_processed_count(connection, table_prefix)
    summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
      from: utc(2026, 5, 6, 1, 0),
      to: utc(2026, 5, 6, 1, 1),
      by: { provider: "postmark" }
    )

    assert_equal expected_delivery_result(processed: 1, complete: false), first_result.to_h
    assert_equal expected_delivery_result(processed: 0, skipped: 1), second_result.to_h
    assert_equal 1, first_processed_count
    assert_equal 0, second_processed_count
    assert_equal 1, summary.fetch(:count)
    assert_equal 100, summary.fetch(:duration_ms_sum)
  ensure
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_process_pending_rolls_back_partial_writes_and_retries_cleanly
    connection = postgres_connection
    namespace = "event_meter:test:postgres:rollback:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    stream_storage = nil
    rollup_storage = nil

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    Dir.mktmpdir("event-meter-postgres-rollback") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: File.join(root, "stream"),
        sync: :flush
      )
      rollup_storage = crash_after_postgres_rollup_storage(
        connection: connection,
        namespace: namespace,
        table_prefix: table_prefix
      )

      EventMeter.configure do |config|
        config.namespace = namespace
        config.stream_storage = stream_storage
        config.rollup_storage = rollup_storage
      end

      write_to_previous_stream_bucket do
        record_delivery({
          customer_id: 44,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 0, 1), duration_ms: 100)
        record_delivery({
          customer_id: 45,
          provider: "postmark"
        }, started_at: utc(2026, 5, 6, 1, 0, 2), duration_ms: 250)
      end

      rollup_storage.crash_after_first_rollup_once!

      assert_raises(RuntimeError) { process_delivery_indexes_pending }
      failed_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 1),
        by: { provider: "postmark" }
      )
      retry_result = process_delivery_indexes_pending
      retried_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 1),
        by: { provider: "postmark" }
      )

      assert_equal 0, failed_summary.fetch(:count)
      assert_equal 0, failed_summary.fetch(:duration_ms_sum)
      assert_equal expected_delivery_result(processed: 2), retry_result.to_h
      assert_equal 2, retried_summary.fetch(:count)
      assert_equal 350, retried_summary.fetch(:duration_ms_sum)
      assert_equal 0, postgres_processed_count(connection, table_prefix)
      assert_empty read_delivery_stream(stream_storage)
    end
  ensure
    stream_storage&.close
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_process_pending_handles_large_batches_indexes_and_intervals
    connection = postgres_connection
    namespace = "event_meter:test:postgres:large:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    stream_storage = nil
    expected = Hash.new do |hash, key|
      hash[key] = {
        count: 0,
        duration_ms_sum: 0,
        interval_ms_count: 0,
        interval_ms_sum: 0
      }
    end
    last_started_ms_by_provider_and_customer = {}

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    Dir.mktmpdir("event-meter-postgres-large") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: File.join(root, "stream"),
        sync: :flush
      )

      EventMeter.configure do |config|
        config.namespace = namespace
        config.stream_storage = stream_storage
        config.rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
          connection: connection,
          namespace: namespace,
          table_prefix: table_prefix
        )
      end

      base = utc(2026, 5, 6, 1, 0)

      write_to_previous_stream_bucket do
        2_500.times do |index|
          customer_slot = index % 100
          provider = customer_slot.even? ? "postmark" : "mailgun"
          delivery_mode = customer_slot.even? ? "email" : "sms"
          queue = customer_slot.even? ? "fast" : "bulk"
          customer_id = 10_000 + customer_slot
          started_at = base + index
          started_ms = (started_at.to_f * 1000).to_i
          duration_ms = 10 + (index % 50)
          provider_key = [provider]
          compound_key = [provider, delivery_mode, queue]

          [provider_key, compound_key].each do |key|
            expected[key][:count] += 1
            expected[key][:duration_ms_sum] += duration_ms
          end

          interval_key = [provider, customer_id]
          if last_started_ms_by_provider_and_customer.key?(interval_key)
            interval_ms = started_ms - last_started_ms_by_provider_and_customer.fetch(interval_key)
            [provider_key, compound_key].each do |key|
              expected[key][:interval_ms_count] += 1
              expected[key][:interval_ms_sum] += interval_ms
            end
          end
          last_started_ms_by_provider_and_customer[interval_key] = started_ms

          record_delivery({
            customer_id: customer_id,
            provider: provider,
            delivery_mode: delivery_mode,
            queue: queue
          }, started_at: started_at, duration_ms: duration_ms)
        end
      end

      result = process_delivery_pending
      postmark_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: base,
        to: base + 2_560,
        by: { provider: "postmark" }
      )
      compound_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: base,
        to: base + 2_560,
        by: {
          provider: "postmark",
          delivery_mode: "email",
          queue: "fast"
        }
      )

      assert_equal expected_delivery_result(processed: 2_500), result.to_h
      assert_delivery_metrics expected[["postmark"]], postmark_summary
      assert_delivery_metrics expected[["postmark", "email", "fast"]], compound_summary
      assert_empty read_delivery_stream(stream_storage)
    end
  ensure
    stream_storage&.close
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_processed_ids_handle_large_incomplete_batches_without_double_counting
    connection = postgres_connection
    namespace = "event_meter:test:postgres:processed:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    stream_storage = nil

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    Dir.mktmpdir("event-meter-postgres-processed") do |root|
      stream_storage = always_incomplete_file_stream_storage(File.join(root, "stream"))

      EventMeter.configure do |config|
        config.namespace = namespace
        config.stream_storage = stream_storage
        config.rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
          connection: connection,
          namespace: namespace,
          table_prefix: table_prefix
        )
      end

      base = utc(2026, 5, 6, 1, 0)

      write_to_previous_stream_bucket do
        1_250.times do |index|
          record_delivery({
            customer_id: 20_000 + index,
            provider: "postmark"
          }, started_at: base + index, duration_ms: 5 + (index % 10))
        end
      end

      first_result = process_delivery_indexes_pending
      first_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: base,
        to: base + 1_300,
        by: { provider: "postmark" }
      )
      first_processed_count = postgres_processed_count(connection, table_prefix)

      second_result = process_delivery_indexes_pending
      second_summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: base,
        to: base + 1_300,
        by: { provider: "postmark" }
      )
      second_processed_count = postgres_processed_count(connection, table_prefix)

      assert_equal expected_delivery_result(processed: 1_250, complete: false), first_result.to_h
      assert_equal expected_delivery_result(processed: 0, skipped: 1_250, complete: false), second_result.to_h
      assert_equal 1_250, first_processed_count
      assert_equal 1_250, second_processed_count
      assert_equal 1_250, first_summary.fetch(:count)
      assert_equal 1_250, second_summary.fetch(:count)
      assert_equal first_summary.fetch(:duration_ms_sum), second_summary.fetch(:duration_ms_sum)
    end
  ensure
    stream_storage&.close
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_process_pending_merges_min_max_fields_across_multiple_passes
    connection = postgres_connection
    namespace = "event_meter:test:postgres:minmax:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    stream_storage = nil

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    Dir.mktmpdir("event-meter-postgres-minmax") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: File.join(root, "stream"),
        sync: :flush
      )

      EventMeter.configure do |config|
        config.namespace = namespace
        config.stream_storage = stream_storage
        config.rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
          connection: connection,
          namespace: namespace,
          table_prefix: table_prefix
        )
      end

      base = utc(2026, 5, 6, 1, 0)

      write_to_previous_stream_bucket do
        record_delivery({ customer_id: 44, provider: "postmark" }, started_at: base + 10, duration_ms: 300)
        record_delivery({ customer_id: 45, provider: "postmark" }, started_at: base + 20, duration_ms: 50)
      end
      assert_equal expected_delivery_result(processed: 2), process_delivery_indexes_pending.to_h

      write_to_previous_stream_bucket do
        record_delivery({ customer_id: 46, provider: "postmark" }, started_at: base + 5, duration_ms: 900)
        record_delivery({ customer_id: 47, provider: "postmark" }, started_at: base + 40, duration_ms: 10)
      end
      assert_equal expected_delivery_result(processed: 2), process_delivery_indexes_pending.to_h

      summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: base,
        to: base + 60,
        by: { provider: "postmark" }
      )

      assert_equal 4, summary.fetch(:count)
      assert_equal 1_260, summary.fetch(:duration_ms_sum)
      assert_equal 10, summary.fetch(:duration_ms_min)
      assert_equal 900, summary.fetch(:duration_ms_max)
      assert_equal((base + 5).iso8601(6), summary.fetch(:started_at_min))
      assert_equal((base + 40).iso8601(6), summary.fetch(:started_at_max))
      assert_empty read_delivery_stream(stream_storage)
    end
  ensure
    stream_storage&.close
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_process_pending_replaces_corrupt_existing_rollup_fields
    connection = postgres_connection
    namespace = "event_meter:test:postgres:corrupt:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    stream_storage = nil

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    Dir.mktmpdir("event-meter-postgres-corrupt") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: File.join(root, "stream"),
        sync: :flush
      )

      EventMeter.configure do |config|
        config.namespace = namespace
        config.stream_storage = stream_storage
        config.rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
          connection: connection,
          namespace: namespace,
          table_prefix: table_prefix
        )
      end

      base = utc(2026, 5, 6, 1, 0)
      index = delivery_indexes_definition.index_for!(provider: "postmark")
      corrupt_rollup_key = EventMeter::Keys.rollup(
        namespace: namespace,
        name: "invoice_delivery",
        version: DELIVERY_VERSION,
        every: :minute,
        bucket: base,
        index: index
      )
      connection.exec_params(<<~SQL, [corrupt_rollup_key, JSON.generate(["not", "a", "hash"])])
        INSERT INTO #{table_prefix}_rollups (key, fields)
        VALUES ($1, $2::jsonb)
      SQL

      write_to_previous_stream_bucket do
        record_delivery({
          customer_id: 44,
          provider: "postmark"
        }, started_at: base + 10, duration_ms: 123)
      end

      result = process_delivery_indexes_pending
      summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: base,
        to: base + 60,
        by: { provider: "postmark" }
      )
      row = connection.exec_params(<<~SQL, [corrupt_rollup_key]).first
        SELECT jsonb_typeof(fields) AS fields_type
        FROM #{table_prefix}_rollups
        WHERE key = $1
      SQL

      assert_equal expected_delivery_result(processed: 1), result.to_h
      assert_equal 1, summary.fetch(:count)
      assert_equal 123, summary.fetch(:duration_ms_sum)
      assert_equal "object", row.fetch("fields_type")
      assert_empty read_delivery_stream(stream_storage)
    end
  ensure
    stream_storage&.close
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_process_pending_ignores_out_of_range_existing_rollup_fields
    connection = postgres_connection
    namespace = "event_meter:test:postgres:huge-rollup:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    stream_storage = nil

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    Dir.mktmpdir("event-meter-postgres-huge-rollup") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: File.join(root, "stream"),
        sync: :flush
      )

      EventMeter.configure do |config|
        config.namespace = namespace
        config.stream_storage = stream_storage
        config.rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
          connection: connection,
          namespace: namespace,
          table_prefix: table_prefix
        )
      end

      base = utc(2026, 5, 6, 1, 0)
      index = delivery_indexes_definition.index_for!(provider: "postmark")
      rollup_key = EventMeter::Keys.rollup(
        namespace: namespace,
        name: "invoice_delivery",
        version: DELIVERY_VERSION,
        every: :minute,
        bucket: base,
        index: index
      )
      huge = "9" * 40
      huge_fields = JSON.generate({
        "count" => huge,
        "duration_ms_sum" => huge,
        "duration_ms_min" => huge,
        "duration_ms_max" => huge
      })
      connection.exec_params(<<~SQL, [rollup_key, huge_fields])
        INSERT INTO #{table_prefix}_rollups (key, fields)
        VALUES ($1, $2::jsonb)
      SQL

      write_to_previous_stream_bucket do
        record_delivery({
          customer_id: 44,
          provider: "postmark"
        }, started_at: base + 10, duration_ms: 123)
      end

      result = process_delivery_indexes_pending
      summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: base,
        to: base + 60,
        by: { provider: "postmark" }
      )

      assert_equal expected_delivery_result(processed: 1), result.to_h
      assert_equal 1, summary.fetch(:count)
      assert_equal 123, summary.fetch(:duration_ms_sum)
      assert_equal 123, summary.fetch(:duration_ms_min)
      assert_equal 123, summary.fetch(:duration_ms_max)
      assert_empty read_delivery_stream(stream_storage)
    end
  ensure
    stream_storage&.close
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_definition_mismatch_leaves_stream_available_for_the_correct_definition
    connection = postgres_connection
    namespace = "event_meter:test:postgres:definition:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    stream_storage = nil

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )

    Dir.mktmpdir("event-meter-postgres-definition") do |root|
      stream_storage = EventMeter::Stores::Stream::File.new(
        path: File.join(root, "stream"),
        sync: :flush
      )

      EventMeter.configure do |config|
        config.namespace = namespace
        config.stream_storage = stream_storage
        config.rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
          connection: connection,
          namespace: namespace,
          table_prefix: table_prefix
        )
      end

      write_to_previous_stream_bucket do
        record_delivery({ customer_id: 44, provider: "postmark" }, started_at: utc(2026, 5, 6, 1, 0), duration_ms: 100)
      end
      assert_equal expected_delivery_result(processed: 1), process_delivery_indexes_pending.to_h

      write_to_previous_stream_bucket do
        record_delivery({ customer_id: 45, provider: "postmark" }, started_at: utc(2026, 5, 6, 1, 1), duration_ms: 200)
      end
      assert_raises(EventMeter::DefinitionChangedError) do
        EventMeter.process_pending("invoice_delivery", version: DELIVERY_VERSION) do |report|
          report.index_by(:customer_id)
        end
      end

      retry_result = process_delivery_indexes_pending
      summary = EventMeter.summary("invoice_delivery", version: DELIVERY_VERSION,
        from: utc(2026, 5, 6, 1, 0),
        to: utc(2026, 5, 6, 1, 2),
        by: { provider: "postmark" }
      )

      assert_equal expected_delivery_result(processed: 1), retry_result.to_h
      assert_equal 2, summary.fetch(:count)
      assert_equal 300, summary.fetch(:duration_ms_sum)
      assert_empty read_delivery_stream(stream_storage)
    end
  ensure
    stream_storage&.close
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  def test_postgres_definition_insert_revalidates_the_stored_fingerprint
    connection = postgres_connection
    namespace = "event_meter:test:postgres:definition-race:#{Process.pid}:#{SecureRandom.hex(4)}"
    table_prefix = "event_meter_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    competing_definition = EventMeter::ReportDefinition.build(DELIVERY_EVENT, version: DELIVERY_VERSION) do |report|
      report.index_by(:queue)
    end

    EventMeter::Stores::Rollup::Postgres.install!(
      connection: connection,
      table_prefix: table_prefix
    )
    storage = definition_racing_postgres_storage(
      connection: connection,
      namespace: namespace,
      table_prefix: table_prefix,
      competing_definition: competing_definition
    )

    assert_raises(EventMeter::DefinitionChangedError) do
      storage.ensure_definition(delivery_indexes_definition)
    end
  ensure
    drop_postgres_tables(connection, table_prefix) if connection && table_prefix
    connection&.close
  end

  private

  def append_delivery(stream, customer_id:, provider:, started_at:, duration_ms:)
    stream.append(EventMeter::EventPayload.build(
      "invoice_delivery",
      params: {
        customer_id: customer_id,
        provider: provider
      },
      status: "success",
      started_at: started_at,
      duration_ms: duration_ms
    ))
  end

  def assert_delivery_metrics(expected, summary)
    assert_equal expected.fetch(:count), summary.fetch(:count)
    assert_equal expected.fetch(:duration_ms_sum), summary.fetch(:duration_ms_sum)
    assert_equal expected.fetch(:interval_ms_count), summary.fetch(:interval_ms_count)
    assert_equal expected.fetch(:interval_ms_sum), summary.fetch(:interval_ms_sum)
  end

  def read_delivery_stream(stream)
    stream.read(name: DELIVERY_EVENT)
  end

  def write_to_previous_stream_bucket
    Time.stub(:now, Time.now.utc - 120) do
      yield
    end
  end

  def previous_stream_log_name(suffix)
    "#{(Time.now.utc - 120).strftime("%Y%m%d%H%M")}-#{suffix}-123-deadbeef.jsonl"
  end

  def file_stream_path(root, event_name = DELIVERY_EVENT)
    File.join(root, "streams", EventMeter::PathName.event(event_name))
  end

  def file_rollup_report_path(root, namespace:, event_name: DELIVERY_EVENT, version: DELIVERY_VERSION)
    File.join(
      root,
      "rollups",
      EventMeter::PathName.event(namespace),
      EventMeter::PathName.event(event_name),
      EventMeter::PathName.version(version)
    )
  end

  def file_stream_logs(root, _namespace = nil)
    Dir[File.join(file_stream_path(root), "logs", "*.jsonl")]
  end

  def file_stream_processing_logs(root, _namespace = nil)
    Dir[File.join(file_stream_path(root), "processing", "*.jsonl")]
  end

  def file_stream_quarantine_logs(root, _namespace = nil)
    Dir[File.join(file_stream_path(root), "quarantine", "*.jsonl")]
  end

  def delivery_config(namespace:)
    EventMeter::Configuration.new.tap do |config|
      config.namespace = namespace
    end
  end

  def redis_client
    require "redis"

    options = ENV["EVENT_METER_REDIS_URL"].to_s.strip
    client = options.empty? ? Redis.new : Redis.new(url: options)
    client.tap(&:ping)
  rescue LoadError, Redis::BaseConnectionError, Errno::ECONNREFUSED => error
    skip "Redis is unavailable: #{error.class}: #{error.message}"
  end

  def postgres_connection
    EventMeterTestSupport::PostgresDatabase.connect(test: self)
  end

  def cleanup_redis(redis, namespace)
    keys = []
    redis.scan_each(match: "#{namespace}:*") { |key| keys << key }
    keys.each_slice(500) { |slice| redis.del(*slice) unless slice.empty? }
  end

  def redis_processed_keys(redis, namespace)
    keys = []
    redis.scan_each(match: "#{namespace}:processed:*") { |key| keys << key }
    keys.sort
  end

  def redis_processed_key(namespace, name, version, id)
    EventMeter::Keys.processed(namespace: namespace, name: name, version: version, id: id)
  end

  def redis_lock_probe
    Class.new do
      attr_reader :commands

      def initialize
        @values = {}
        @commands = []
      end

      def set(key, value, nx: false, ex: nil)
        commands << [:set, key, nx, ex]
        return false if nx && @values.key?(key)

        @values[key] = value
        true
      end

      def eval(script, keys:, argv:)
        commands << [:eval, keys, argv]
        key = keys.first
        token = argv.first

        if script.include?("expire")
          @values[key] == token ? 1 : 0
        else
          @values.delete(key) if @values[key] == token
          1
        end
      end
    end.new
  end

  def redis_cleanup_probe(values)
    pipeline_class = Class.new do
      attr_reader :results

      def initialize(values, pipelined_gets)
        @values = values
        @pipelined_gets = pipelined_gets
        @results = []
      end

      def get(key)
        @pipelined_gets << key
        @results << @values[key]
      end
    end

    Class.new do
      attr_reader :deleted_batches, :get_calls, :pipelined_gets

      define_method(:pipeline_class) { pipeline_class }

      def initialize(values)
        @values = values.dup
        @deleted_batches = []
        @get_calls = []
        @pipelined_gets = []
      end

      def scan_each(match:)
        @values.keys.each do |key|
          yield key if File.fnmatch?(match, key)
        end
      end

      def get(key)
        @get_calls << key
        @values[key]
      end

      def pipelined
        pipeline = pipeline_class.new(@values, @pipelined_gets)
        yield pipeline
        pipeline.results
      end

      def del(*keys)
        @deleted_batches << keys
        keys.each { |key| @values.delete(key) }
        keys.length
      end
    end.new(values)
  end

  def postgres_processed_count(connection, table_prefix)
    connection.exec("SELECT count(*) AS count FROM #{table_prefix}_processed_entries").first.fetch("count").to_i
  end

  def postgres_processed_entry_ids(connection, table_prefix, namespace:, event_name:)
    rows = connection.exec_params(<<~SQL, [namespace, EventMeter::Keys.event_name(event_name)])
      SELECT entry_id
      FROM #{table_prefix}_processed_entries
      WHERE namespace = $1
        AND event_name = $2
      ORDER BY entry_id
    SQL

    rows.map { |row| row.fetch("entry_id") }
  end

  def update_postgres_processed_created_at(connection, table_prefix, namespace:, event_name:, created_at:, entry_id: nil)
    conditions = [
      "namespace = $1",
      "event_name = $2"
    ]
    params = [namespace, EventMeter::Keys.event_name(event_name), created_at.iso8601]

    if entry_id
      conditions << "entry_id = $4"
      params << entry_id
    end

    connection.exec_params(<<~SQL, params)
      UPDATE #{table_prefix}_processed_entries
      SET created_at = $3::timestamptz
      WHERE #{conditions.join(" AND ")}
    SQL
  end

  def drop_postgres_tables(connection, table_prefix)
    connection.exec(<<~SQL)
      DROP TABLE IF EXISTS
        #{table_prefix}_processed_entries,
        #{table_prefix}_strings,
        #{table_prefix}_rollups
    SQL
  end

  def fake_processed_batch(ids)
    EventMeter::Processor::Batch.new.tap do |batch|
      batch.entry_ids.concat(ids)
    end
  end

  def race_exposing_redis_rollup(redis:, namespace:, barrier:)
    Class.new(EventMeter::Stores::Rollup::Redis) do
      define_method(:initialize) do |**options|
        @barrier = barrier
        @barrier_uses_remaining = 0
        @barrier_mutex = Mutex.new
        super(**{
          redis: redis,
          namespace: namespace
        }.merge(options))
      end

      def arm_race_barrier!
        @barrier_mutex.synchronize { @barrier_uses_remaining = 2 }
      end

      def disarm_race_barrier!
        @barrier_mutex.synchronize { @barrier_uses_remaining = 0 }
      end

      def hgetall_many(keys)
        result = super
        wait_at_old_rollup_read_path if rollup_keys?(keys)
        result
      end

      private

      def rollup_keys?(keys)
        keys.any? { |key| key.start_with?("#{namespace}:rollup:") }
      end

      def wait_at_old_rollup_read_path
        should_wait = @barrier_mutex.synchronize do
          if @barrier_uses_remaining.positive?
            @barrier_uses_remaining -= 1
            true
          else
            false
          end
        end

        @barrier.wait if should_wait
      end
    end.new
  end

  def with_armed_race_barrier(rollup_storage)
    rollup_storage.arm_race_barrier!
    yield
  ensure
    rollup_storage.disarm_race_barrier!
  end

  def run_concurrently(items)
    results = Array.new(items.length)
    errors = Queue.new

    threads = items.each_with_index.map do |item, index|
      Thread.new do
        results[index] = yield item
      rescue StandardError => error
        errors << error
      end
    end
    threads.each(&:join)

    raise errors.pop unless errors.empty?

    results
  end

  def flaky_delete_stream_storage
    Class.new(EventMeterTestSupport::MemoryStreamStorage) do
      attr_reader :delete_calls

      def initialize
        super
        @delete_calls = 0
      end

      def delete
        @delete_calls += 1
        super() unless delete_calls == 1
      end
    end.new
  end

  def always_incomplete_file_stream_storage(path)
    Class.new(EventMeter::Stores::Stream::File) do
      def delete
        false
      end
    end.new(path: path, sync: :flush)
  end

  def crash_after_rollup_file_storage(root)
    Class.new(EventMeter::Stores::Rollup::File) do
      def crash_after_rollups_once!
        @crash_after_rollups = true
      end

      def for_report(name:, version:)
        super.tap do |storage|
          if @crash_after_rollups
            storage.crash_after_rollups_once!
            @crash_after_rollups = false
          end
        end
      end

      private

      def apply_rollups(batch_id, batch)
        super.tap do |applied_paths|
          if @crash_after_rollups
            @crash_after_rollups = false
            raise "simulated crash after rollup writes"
          end
        end
      end
    end.new(path: root)
  end

  def crash_after_postgres_rollup_storage(connection:, namespace:, table_prefix:)
    Class.new(EventMeter::Stores::Rollup::Postgres) do
      def crash_after_first_rollup_once!
        @crash_after_first_rollup = true
      end

      def for_report(name:, version:)
        super.tap do |storage|
          if @crash_after_first_rollup
            storage.crash_after_first_rollup_once!
            @crash_after_first_rollup = false
          end
        end
      end

      private

      def merge_rollup_rows(rows)
        super.tap do
          if @crash_after_first_rollup
            @crash_after_first_rollup = false
            raise "simulated postgres rollup failure"
          end
        end
      end
    end.new(
      connection: connection,
      namespace: namespace,
      table_prefix: table_prefix
    )
  end

  def definition_racing_postgres_storage(connection:, namespace:, table_prefix:, competing_definition:)
    Class.new(EventMeter::Stores::Rollup::Postgres) do
      define_method(:initialize) do |competing_definition:, **options|
        @competing_payload = JSON.generate(competing_definition.to_h)
        super(**options)
      end

      private

      def insert_string_once(key, _value)
        upsert_string(key, @competing_payload)
      end
    end.new(
      connection: connection,
      namespace: namespace,
      table_prefix: table_prefix,
      competing_definition: competing_definition
    )
  end

  def fake_postgres_connection(results)
    Class.new do
      attr_reader :calls

      define_method(:initialize) do |queued_results|
        @queued_results = queued_results
        @calls = []
      end

      def exec_params(sql, params)
        calls << {
          sql: sql,
          params: params
        }
        @queued_results.shift || []
      end

      def exec(sql)
        calls << {
          sql: sql,
          params: []
        }
        []
      end
    end.new(results)
  end

  def refreshing_fake_postgres_lock_connection
    Class.new do
      attr_reader :refresh_count

      def initialize
        @refresh_count = 0
        @refreshes = Queue.new
      end

      def exec_params(sql, params)
        case sql
        when /INSERT INTO/
          [{ "value" => params.fetch(1) }]
        when /UPDATE/
          @refresh_count += 1
          @refreshes << true
          [{ "value" => "refreshed" }]
        else
          []
        end
      end

      def exec(_sql)
        []
      end

      def wait_for_refresh
        @refreshes.pop
      end
    end.new
  end

  class TwoThreadBarrier
    def initialize(size)
      @size = size
      @waiting = 0
      @mutex = Mutex.new
      @condition = ConditionVariable.new
    end

    def wait(timeout: 5)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

      @mutex.synchronize do
        @waiting += 1

        if @waiting == @size
          @condition.broadcast
          return
        end

        while @waiting < @size
          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          raise "timed out waiting for concurrent rollup writers" unless remaining.positive?

          @condition.wait(@mutex, remaining)
        end
      end
    end
  end
end
